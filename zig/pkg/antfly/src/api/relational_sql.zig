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

const db_mod = @import("../storage/db/mod.zig");
const json_helpers = @import("json_helpers.zig");
const platform_time = @import("../platform/time.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");

pub const default_array_agg_max_items: u32 = db_mod.types.default_relational_rows_array_agg_max_items;

pub const SqlValue = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    json: []const u8,

    fn jsonAlloc(self: SqlValue, alloc: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .null => try alloc.dupe(u8, "null"),
            .bool => |value| try alloc.dupe(u8, if (value) "true" else "false"),
            .integer => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .float => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .string => |value| try std.json.Stringify.valueAlloc(alloc, value, .{}),
            .json => |value| try alloc.dupe(u8, value),
        };
    }

    fn asU32(self: SqlValue) !u32 {
        return switch (self) {
            .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @intCast(value) else error.UnsupportedSqlShape,
            else => error.UnsupportedSqlShape,
        };
    }
};

pub const LoweredSelect = struct {
    table_name: []const u8,
    query: db_mod.types.RelationalRowsQueryRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.query.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredQueryPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsQueryPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.plan.ctes) |cte| {
            alloc.free(cte.name);
            var query = cte.query;
            query.deinit(alloc);
        }
        if (self.plan.ctes.len > 0) alloc.free(self.plan.ctes);
        self.plan.query.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredWindowPlan = struct {
    table_name: []const u8,
    plan: db_mod.types.RelationalRowsWindowPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.plan.ctes) |cte| {
            alloc.free(cte.name);
            var query = cte.query;
            query.deinit(alloc);
        }
        if (self.plan.ctes.len > 0) alloc.free(self.plan.ctes);
        self.plan.window.source.deinit(alloc);
        freeWindowSpecs(alloc, self.plan.window.windows);
        if (self.plan.window.windows.len > 0) alloc.free(self.plan.window.windows);
        freeStringSlice(alloc, self.plan.window.select);
        freeOrderBy(alloc, self.plan.window.order_by);
        if (self.plan.window.order_by.len > 0) alloc.free(self.plan.window.order_by);
        self.* = undefined;
    }
};

pub const LoweredInsert = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    returning_expression_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredMutation = struct {
    table_name: []const u8,
    batch: relational_rows.OwnedRowsBatchRequest,
    returning_expression_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.batch.deinit(alloc);
        self.* = undefined;
    }
};

const ReturningProjection = struct {
    fields: []const []const u8 = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionProjection = &.{},

    fn hasProjection(self: ReturningProjection) bool {
        return self.fields.len > 0 or self.expressions.len > 0;
    }

    fn deinit(self: ReturningProjection, alloc: std.mem.Allocator) void {
        freeStringSlice(alloc, self.fields);
        freeExpressionProjections(alloc, self.expressions);
    }
};

pub const LoweredMutationSource = struct {
    table_name: []const u8,
    mutation: relational_rows.OwnedRowsMutationSourceRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.mutation.deinit(alloc);
        self.* = undefined;
    }
};

pub const LoweredAggregate = struct {
    table_name: []const u8,
    aggregate: db_mod.types.RelationalRowsAggregateRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.aggregate.source.deinit(alloc);
        freeStringSlice(alloc, self.aggregate.group_by);
        freeAggregateSpecs(alloc, self.aggregate.aggregations);
        if (self.aggregate.aggregations.len > 0) alloc.free(self.aggregate.aggregations);
        freeRelationalChecks(alloc, self.aggregate.having_predicates);
        if (self.aggregate.having_predicates.len > 0) alloc.free(self.aggregate.having_predicates);
        freeOrderBy(alloc, self.aggregate.order_by);
        if (self.aggregate.order_by.len > 0) alloc.free(self.aggregate.order_by);
        self.* = undefined;
    }
};

pub const LoweredJoin = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    join: db_mod.types.RelationalRowsJoinRequest,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.left_table_name);
        alloc.free(self.right_table_name);
        self.join.left.deinit(alloc);
        self.join.right.deinit(alloc);
        freeJoinOn(alloc, self.join.on);
        if (self.join.on.len > 0) alloc.free(self.join.on);
        freeJoinProjections(alloc, self.join.select);
        if (self.join.select.len > 0) alloc.free(self.join.select);
        freeOrderBy(alloc, self.join.order_by);
        if (self.join.order_by.len > 0) alloc.free(self.join.order_by);
        self.* = undefined;
    }
};

pub const LoweredLateralPlan = struct {
    left_table_name: []const u8,
    right_table_name: []const u8,
    plan: db_mod.types.RelationalRowsLateralPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.left_table_name);
        alloc.free(self.right_table_name);
        for (self.plan.ctes) |cte| {
            alloc.free(cte.name);
            var query = cte.query;
            query.deinit(alloc);
        }
        if (self.plan.ctes.len > 0) alloc.free(self.plan.ctes);
        self.plan.lateral.left.deinit(alloc);
        self.plan.lateral.right.deinit(alloc);
        freeLateralCorrelations(alloc, self.plan.lateral.correlations);
        if (self.plan.lateral.correlations.len > 0) alloc.free(self.plan.lateral.correlations);
        freeJoinProjections(alloc, self.plan.lateral.select);
        if (self.plan.lateral.select.len > 0) alloc.free(self.plan.lateral.select);
        freeOrderBy(alloc, self.plan.lateral.order_by);
        if (self.plan.lateral.order_by.len > 0) alloc.free(self.plan.lateral.order_by);
        self.* = undefined;
    }
};

pub const LoweredDdlPlan = union(enum) {
    create_table: CreateTablePlan,
    create_index: CreateIndexPlan,
    alter_table: AlterTablePlan,
    create_update_policy: CreateUpdatePolicyPlan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .create_table => |*plan| plan.deinit(alloc),
            .create_index => |*plan| plan.deinit(alloc),
            .alter_table => |*plan| plan.deinit(alloc),
            .create_update_policy => |*plan| plan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const CreateTablePlan = struct {
    table_name: []const u8,
    columns: []const runtime_schema.RelationalColumn = &.{},
    primary_key: ?runtime_schema.PrimaryKey = null,
    unique_constraints: []const runtime_schema.UniqueConstraint = &.{},
    foreign_keys: []const runtime_schema.ForeignKey = &.{},
    checks: []const runtime_schema.RelationalCheck = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        freeDdlRelationalColumns(alloc, self.columns);
        if (self.primary_key) |primary_key| freeDdlPrimaryKey(alloc, primary_key);
        freeDdlUniqueConstraints(alloc, self.unique_constraints);
        freeDdlForeignKeys(alloc, self.foreign_keys);
        freeDdlRelationalChecks(alloc, self.checks);
        self.* = undefined;
    }
};

pub const CreateUpdatePolicyPlan = struct {
    trigger_name: []const u8,
    table_name: []const u8,
    column_name: []const u8,
    on_update_value: runtime_schema.RelationalDefaultValue,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.trigger_name);
        alloc.free(self.table_name);
        alloc.free(self.column_name);
        alloc.free(self.on_update_value.value_json);
        self.* = undefined;
    }
};

pub const AlterTablePlan = struct {
    table_name: []const u8,
    operations: []const AlterTableOperation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.operations) |operation| freeAlterTableOperation(alloc, operation);
        if (self.operations.len > 0) alloc.free(self.operations);
        self.* = undefined;
    }
};

pub const AlterTableOperation = union(enum) {
    add_column: runtime_schema.RelationalColumn,
    add_unique_constraint: runtime_schema.UniqueConstraint,
    add_foreign_key: runtime_schema.ForeignKey,
    add_check: runtime_schema.RelationalCheck,
    validate_constraint: []const u8,
};

pub const CreateIndexPlan = struct {
    index_name: []const u8,
    table_name: []const u8,
    unique: bool = false,
    columns: []const []const u8 = &.{},
    expressions: []const runtime_schema.UniqueExpression = &.{},
    where: []const runtime_schema.UniquePredicate = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.table_name);
        freeStringSlice(alloc, self.columns);
        freeDdlUniqueExpressions(alloc, self.expressions);
        freeDdlUniquePredicates(alloc, self.where);
        self.* = undefined;
    }
};

pub const AppliedDdlSchemaJson = struct {
    schema_json: []u8,
    requires_rebuild: bool = false,
    validation_required: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.schema_json);
        self.* = undefined;
    }
};

const TokenKind = enum {
    identifier,
    string,
    number,
    placeholder,
    comma,
    star,
    eq,
    neq,
    gt,
    gte,
    lt,
    lte,
    plus,
    minus,
    slash,
    lparen,
    rparen,
    lbracket,
    rbracket,
    at_contains,
    pipe_concat,
    question,
    arrow_json,
    arrow_text,
    semicolon,
};

const Token = struct {
    kind: TokenKind,
    text: []const u8,
};

pub fn lowerSelectAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSelect {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseSelect();
}

pub fn lowerQueryPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredQueryPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseQueryPlan();
}

pub fn lowerWindowPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredWindowPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseWindowPlan();
}

pub fn lowerInsertAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseInsert();
}

pub fn lowerInsertWithResolverAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredInsert {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try parser.parseInsert();
}

pub fn lowerUpdateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try parser.parseUpdate();
}

pub fn lowerDeleteAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    unique_resolver: relational_rows.UniqueSelectorResolver,
) !LoweredMutation {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .unique_resolver = unique_resolver,
    };
    return try parser.parseDelete();
}

pub fn lowerUpdateMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .mutation_claim = row_claim,
    };
    return try parser.parseUpdateMutationSource();
}

pub fn lowerDeleteMutationSourceAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    row_claim: db_mod.types.RowClaimRequest,
) !LoweredMutationSource {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (row_claim.txn_id == null) return error.UnsupportedRowsQuery;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
        .mutation_claim = row_claim,
    };
    return try parser.parseDeleteMutationSource();
}

pub fn lowerAggregateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregate {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseAggregate();
}

pub fn lowerJoinAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseJoin();
}

pub fn lowerLateralPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
        .schema = schema,
        .params = params,
    };
    return try parser.parseLateral();
}

pub fn lowerDdlPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
) !LoweredDdlPlan {
    var tokens = try tokenizeAlloc(alloc, sql);
    defer freeTokens(alloc, &tokens);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens.items,
    };
    return try parser.parseDdlPlan();
}

pub fn runtimeSchemaFromCreateTablePlanAlloc(
    alloc: std.mem.Allocator,
    plan: CreateTablePlan,
) !runtime_schema.TableSchema {
    if (plan.primary_key == null) return error.InvalidSqlCatalog;
    try validateRelationalColumnCatalog(plan.columns);
    try validatePrimaryKeyColumns(plan.columns, plan.primary_key.?);
    try validateUniqueConstraintCatalog(plan.columns, plan.unique_constraints);
    try validateForeignKeyCatalog(plan.columns, plan.foreign_keys);
    try validateRelationalCheckCatalog(plan.columns, plan.checks);

    const default_type = try alloc.dupe(u8, "_default");
    const ttl_field = alloc.dupe(u8, "_timestamp") catch |err| {
        alloc.free(default_type);
        return err;
    };
    var schema: runtime_schema.TableSchema = .{
        .default_type = default_type,
        .ttl_field = ttl_field,
        .enforce_types = true,
        .storage_mode = .relational,
    };
    errdefer runtime_schema.freeSchema(alloc, schema);
    schema.relational_columns = try cloneDdlRelationalColumns(alloc, plan.columns);
    schema.primary_key = try cloneDdlPrimaryKeyMaybe(alloc, plan.primary_key);
    schema.unique_constraints = try cloneDdlUniqueConstraints(alloc, plan.unique_constraints);
    schema.foreign_keys = try cloneDdlForeignKeys(alloc, plan.foreign_keys);
    schema.checks = try cloneDdlRelationalChecks(alloc, plan.checks);
    return schema;
}

pub fn applyDdlPlanToRuntimeSchemaAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: LoweredDdlPlan,
) !runtime_schema.TableSchema {
    return switch (plan) {
        .create_table => |create_table| runtimeSchemaFromCreateTablePlanAlloc(alloc, create_table),
        .create_index => |create_index| applyCreateIndexPlanAlloc(alloc, current, create_index),
        .alter_table => |alter_table| applyAlterTablePlanAlloc(alloc, current, alter_table),
        .create_update_policy => |update_policy| applyCreateUpdatePolicyPlanAlloc(alloc, current, update_policy),
    };
}

pub fn schemaJsonFromCreateTablePlanAlloc(
    alloc: std.mem.Allocator,
    plan: CreateTablePlan,
) ![]u8 {
    const runtime = try runtimeSchemaFromCreateTablePlanAlloc(alloc, plan);
    defer runtime_schema.freeSchema(alloc, runtime);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const root = try schemaJsonValueFromCreateTablePlanAlloc(arena, plan);
    const schema_json = try std.json.Stringify.valueAlloc(alloc, root, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(schema_json);
    try validateDdlAppliedSchemaJsonAlloc(alloc, schema_json);
    return schema_json;
}

pub fn applyDdlPlanToSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    plan: LoweredDdlPlan,
) !AppliedDdlSchemaJson {
    switch (plan) {
        .create_table => |create_table| {
            if (current_schema_json.len != 0) return error.InvalidSqlCatalog;
            return .{ .schema_json = try schemaJsonFromCreateTablePlanAlloc(alloc, create_table) };
        },
        .create_index, .alter_table, .create_update_policy => {},
    }

    if (current_schema_json.len == 0) return error.InvalidSqlCatalog;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var parsed = try std.json.parseFromSlice(std.json.Value, arena, current_schema_json, .{});
    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSqlCatalog,
    };

    var result: AppliedDdlSchemaJson = .{ .schema_json = &.{} };
    switch (plan) {
        .create_table => unreachable,
        .create_index => |create_index| {
            result.requires_rebuild = true;
            result.validation_required = create_index.unique;
            try applyCreateIndexPlanToSchemaJsonValue(arena, root, create_index);
        },
        .alter_table => |alter_table| {
            result.requires_rebuild = true;
            result.validation_required = true;
            try applyAlterTablePlanToSchemaJsonValue(arena, root, alter_table);
        },
        .create_update_policy => |update_policy| {
            try applyCreateUpdatePolicyPlanToSchemaJsonValue(arena, root, update_policy);
        },
    }
    result.schema_json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(result.schema_json);
    try validateDdlAppliedSchemaJsonAlloc(alloc, result.schema_json);
    return result;
}

fn applyCreateIndexPlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: CreateIndexPlan,
) !runtime_schema.TableSchema {
    var schema = try cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);

    if (plan.unique) {
        const constraint: runtime_schema.UniqueConstraint = .{
            .name = plan.index_name,
            .columns = plan.columns,
            .expressions = plan.expressions,
            .where = plan.where,
            .validation_state = .unvalidated,
        };
        try validateUniqueConstraintForColumns(schema.relational_columns, constraint);
        try appendUniqueConstraintAlloc(alloc, &schema, constraint);
        return schema;
    }

    const index_generation = stableSecondaryIndexGeneration(plan);
    if (plan.expressions.len == 1 and plan.expressions[0].op == .lower and plan.columns.len == 0) {
        if (relationalColumnIndex(schema.relational_columns, plan.index_name) != null) return error.InvalidSqlCatalog;
        if (relationalColumnIndex(schema.relational_columns, plan.expressions[0].field) == null) return error.InvalidSqlCatalog;
        const generated: runtime_schema.RelationalGeneratedValue = .{
            .op = .lower,
            .field = plan.expressions[0].field,
        };
        const column: runtime_schema.RelationalColumn = .{
            .name = plan.index_name,
            .path = plan.index_name,
            .field_type = .keyword,
            .nullable = true,
            .indexed = true,
            .index_lifecycle = .building,
            .index_generation = index_generation,
            .generated = generated,
            .index_where = plan.where,
        };
        try validateUniquePredicatesForColumns(schema.relational_columns, plan.where);
        try appendRelationalColumnAlloc(alloc, &schema, column);
        return schema;
    }

    if (plan.columns.len != 1 or plan.expressions.len != 0) return error.UnsupportedSqlShape;
    try validateUniquePredicatesForColumns(schema.relational_columns, plan.where);
    try markColumnIndexedAlloc(alloc, &schema, plan.columns[0], plan.where, index_generation);
    return schema;
}

fn applyAlterTablePlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: AlterTablePlan,
) !runtime_schema.TableSchema {
    var schema = try cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);

    for (plan.operations) |operation| {
        switch (operation) {
            .add_column => |column| {
                try validateGeneratedColumnForColumns(schema.relational_columns, column);
                try validateUniquePredicatesForColumns(schema.relational_columns, column.index_where);
                try appendRelationalColumnAlloc(alloc, &schema, column);
            },
            .add_unique_constraint => |constraint| {
                try validateUniqueConstraintForColumns(schema.relational_columns, constraint);
                try appendUniqueConstraintAlloc(alloc, &schema, constraint);
            },
            .add_foreign_key => |foreign_key| {
                try validateForeignKeyForColumns(schema.relational_columns, foreign_key);
                try appendForeignKeyAlloc(alloc, &schema, foreign_key);
            },
            .add_check => |check| {
                try validateCheckForColumns(schema.relational_columns, check);
                try appendRelationalCheckAlloc(alloc, &schema, check);
            },
            .validate_constraint => |constraint_name| try validateConstraintByName(&schema, constraint_name),
        }
    }
    return schema;
}

fn applyCreateUpdatePolicyPlanAlloc(
    alloc: std.mem.Allocator,
    current: runtime_schema.TableSchema,
    plan: CreateUpdatePolicyPlan,
) !runtime_schema.TableSchema {
    var schema = try cloneRelationalRuntimeSchemaAlloc(alloc, current);
    errdefer runtime_schema.freeSchema(alloc, schema);
    try setColumnOnUpdatePolicyAlloc(alloc, &schema, plan.column_name, plan.on_update_value);
    return schema;
}

const Parser = struct {
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize = 0,
    schema: runtime_schema.TableSchema = .{},
    params: []const SqlValue = &.{},
    unique_resolver: ?relational_rows.UniqueSelectorResolver = null,
    mutation_claim: ?db_mod.types.RowClaimRequest = null,

    fn parseDdlPlan(self: *@This()) !LoweredDdlPlan {
        if (self.matchKeyword("create")) {
            if (self.matchKeyword("or")) {
                try self.expectKeyword("replace");
            }
            const unique = self.matchKeyword("unique");
            if (self.peekKeyword("index")) {
                return .{ .create_index = try self.parseCreateIndexDdl(unique) };
            }
            if (unique) return error.UnsupportedSqlShape;
            if (self.peekKeyword("trigger")) {
                return .{ .create_update_policy = try self.parseCreateTriggerPolicyDdl() };
            }
            return .{ .create_table = try self.parseCreateTableDdl() };
        }
        if (self.matchKeyword("alter")) {
            return .{ .alter_table = try self.parseAlterTableDdl() };
        }
        return error.UnsupportedSqlShape;
    }

    fn parseCreateTriggerPolicyDdl(self: *@This()) !CreateUpdatePolicyPlan {
        try self.expectKeyword("trigger");
        const trigger_name = try self.parseIdentifierOwned();
        var trigger_name_transferred = false;
        errdefer if (!trigger_name_transferred) self.alloc.free(trigger_name);
        try self.expectKeyword("before");
        try self.expectKeyword("update");
        if (self.matchKeyword("of")) {
            while (!self.peekKeyword("on")) {
                const column = try self.parseIdentifierOwned();
                self.alloc.free(column);
                if (self.match(.comma) == null) break;
            }
        }
        try self.expectKeyword("on");
        const table_name = try self.parseIdentifierOwned();
        var table_name_transferred = false;
        errdefer if (!table_name_transferred) self.alloc.free(table_name);
        if (self.matchKeyword("for")) {
            try self.expectKeyword("each");
            try self.expectKeyword("row");
        }
        try self.expectKeyword("execute");
        if (!(self.matchKeyword("function") or self.matchKeyword("procedure"))) return error.UnsupportedSqlShape;
        const function_name = try self.parseIdentifierOwned();
        defer self.alloc.free(function_name);
        if (!isSupportedUpdatedAtTriggerFunction(function_name)) return error.UnsupportedSqlShape;
        try self.expect(.lparen);
        const column_name = if (self.match(.rparen) != null)
            try self.alloc.dupe(u8, "updated_at")
        else blk: {
            const parsed_column = if (self.match(.string)) |token|
                try self.alloc.dupe(u8, token.text)
            else
                try self.parseIdentifierOwned();
            var parsed_column_transferred = false;
            errdefer if (!parsed_column_transferred) self.alloc.free(parsed_column);
            if (self.match(.comma) != null) return error.UnsupportedSqlShape;
            try self.expect(.rparen);
            parsed_column_transferred = true;
            break :blk parsed_column;
        };
        var column_name_transferred = false;
        errdefer if (!column_name_transferred) self.alloc.free(column_name);
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const value_json = try self.alloc.dupe(u8, "");
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        trigger_name_transferred = true;
        table_name_transferred = true;
        column_name_transferred = true;
        value_transferred = true;
        return .{
            .trigger_name = trigger_name,
            .table_name = table_name,
            .column_name = column_name,
            .on_update_value = .{ .kind = .now_ns, .value_json = value_json },
        };
    }

    fn parseAlterTableDdl(self: *@This()) !AlterTablePlan {
        try self.expectKeyword("table");
        if (self.matchKeyword("if")) {
            try self.expectKeyword("exists");
        }
        _ = self.matchKeyword("only");
        const table_name = try self.parseIdentifierOwned();
        var table_name_transferred = false;
        errdefer if (!table_name_transferred) self.alloc.free(table_name);

        var operations = std.ArrayListUnmanaged(AlterTableOperation).empty;
        errdefer {
            for (operations.items) |operation| freeAlterTableOperation(self.alloc, operation);
            operations.deinit(self.alloc);
        }
        while (true) {
            try self.parseAlterTableOperation(&operations);
            if (self.match(.comma) == null) break;
        }
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const owned_operations = try operations.toOwnedSlice(self.alloc);
        var operations_transferred = false;
        errdefer if (!operations_transferred) {
            for (owned_operations) |operation| freeAlterTableOperation(self.alloc, operation);
            self.alloc.free(owned_operations);
        };
        table_name_transferred = true;
        operations_transferred = true;
        return .{
            .table_name = table_name,
            .operations = owned_operations,
        };
    }

    fn parseAlterTableOperation(
        self: *@This(),
        operations: *std.ArrayListUnmanaged(AlterTableOperation),
    ) !void {
        if (self.matchKeyword("validate")) {
            try self.expectKeyword("constraint");
            const constraint_name = try self.parseIdentifierOwned();
            try self.appendAlterTableOperation(operations, .{ .validate_constraint = constraint_name });
            return;
        }

        try self.expectKeyword("add");
        if (self.matchKeyword("column")) {
            if (self.matchKeyword("if")) {
                try self.expectKeyword("not");
                try self.expectKeyword("exists");
            }
            try self.parseAlterTableAddColumnOperation(operations);
            return;
        }

        const constraint_name = if (self.matchKeyword("constraint")) try self.parseIdentifierOwned() else null;
        var constraint_name_transferred = false;
        errdefer if (!constraint_name_transferred) if (constraint_name) |name| self.alloc.free(name);
        if (self.matchKeyword("unique")) {
            const columns = try self.parseDdlColumnListAlloc();
            defer freeStringSlice(self.alloc, columns);
            var constraint = try self.makeDdlUniqueConstraint(constraint_name, columns);
            if (constraint_name) |name| self.alloc.free(name);
            constraint_name_transferred = true;
            constraint.validation_state = .unvalidated;
            try self.appendAlterTableOperation(operations, .{ .add_unique_constraint = constraint });
            return;
        }
        if (self.matchKeyword("foreign")) {
            var foreign_key = try self.parseDdlForeignKeyConstraint(constraint_name);
            if (constraint_name) |name| self.alloc.free(name);
            constraint_name_transferred = true;
            foreign_key.validation_state = if (self.consumeOptionalDdlNotValid()) .unvalidated else .unvalidated;
            try self.appendAlterTableOperation(operations, .{ .add_foreign_key = foreign_key });
            return;
        }
        if (self.matchKeyword("check")) {
            var check = try self.parseDdlCheckConstraint(constraint_name);
            if (constraint_name) |name| self.alloc.free(name);
            constraint_name_transferred = true;
            if (self.consumeOptionalDdlNotValid()) check.validation_state = .unvalidated;
            try self.appendAlterTableOperation(operations, .{ .add_check = check });
            return;
        }

        return error.UnsupportedSqlShape;
    }

    fn parseAlterTableAddColumnOperation(
        self: *@This(),
        operations: *std.ArrayListUnmanaged(AlterTableOperation),
    ) !void {
        const column = try self.parseDdlColumnDefinitionStandalone();
        var column_transferred = false;
        errdefer if (!column_transferred) freeDdlRelationalColumn(self.alloc, column);

        try self.appendAlterTableOperation(operations, .{ .add_column = column });
        column_transferred = true;

        while (!self.atEnd() and !self.peekKind(.comma) and !self.peekKind(.semicolon)) {
            if (self.matchKeyword("unique")) {
                var constraint = try self.makeDdlUniqueConstraint(null, &.{column.name});
                constraint.validation_state = .unvalidated;
                try self.appendAlterTableOperation(operations, .{ .add_unique_constraint = constraint });
            } else if (self.matchKeyword("check")) {
                var check = try self.parseDdlCheckConstraint(null);
                if (self.consumeOptionalDdlNotValid()) check.validation_state = .unvalidated;
                try self.appendAlterTableOperation(operations, .{ .add_check = check });
            } else if (self.matchKeyword("references")) {
                var foreign_key = try self.parseDdlInlineForeignKeyConstraint(column.name, null);
                _ = self.consumeOptionalDdlNotValid();
                foreign_key.validation_state = .unvalidated;
                try self.appendAlterTableOperation(operations, .{ .add_foreign_key = foreign_key });
            } else if (self.matchKeyword("constraint")) {
                const constraint_name = try self.parseIdentifierOwned();
                var constraint_name_transferred = false;
                errdefer if (!constraint_name_transferred) self.alloc.free(constraint_name);
                if (self.matchKeyword("unique")) {
                    var constraint = try self.makeDdlUniqueConstraint(constraint_name, &.{column.name});
                    self.alloc.free(constraint_name);
                    constraint_name_transferred = true;
                    constraint.validation_state = .unvalidated;
                    try self.appendAlterTableOperation(operations, .{ .add_unique_constraint = constraint });
                } else if (self.matchKeyword("check")) {
                    var check = try self.parseDdlCheckConstraint(constraint_name);
                    self.alloc.free(constraint_name);
                    constraint_name_transferred = true;
                    if (self.consumeOptionalDdlNotValid()) check.validation_state = .unvalidated;
                    try self.appendAlterTableOperation(operations, .{ .add_check = check });
                } else if (self.matchKeyword("references")) {
                    var foreign_key = try self.parseDdlInlineForeignKeyConstraint(column.name, constraint_name);
                    self.alloc.free(constraint_name);
                    constraint_name_transferred = true;
                    _ = self.consumeOptionalDdlNotValid();
                    foreign_key.validation_state = .unvalidated;
                    try self.appendAlterTableOperation(operations, .{ .add_foreign_key = foreign_key });
                } else {
                    return error.UnsupportedSqlShape;
                }
            } else {
                return error.UnsupportedSqlShape;
            }
        }
    }

    fn appendAlterTableOperation(
        self: *@This(),
        operations: *std.ArrayListUnmanaged(AlterTableOperation),
        operation: AlterTableOperation,
    ) !void {
        var operation_transferred = false;
        errdefer if (!operation_transferred) freeAlterTableOperation(self.alloc, operation);
        try operations.append(self.alloc, operation);
        operation_transferred = true;
    }

    fn parseCreateIndexDdl(self: *@This(), unique: bool) !CreateIndexPlan {
        try self.expectKeyword("index");
        if (self.matchKeyword("concurrently")) return error.UnsupportedSqlShape;
        if (self.matchKeyword("if")) {
            try self.expectKeyword("not");
            try self.expectKeyword("exists");
        }
        const index_name = try self.parseIdentifierOwned();
        var index_name_transferred = false;
        errdefer if (!index_name_transferred) self.alloc.free(index_name);
        try self.expectKeyword("on");
        const table_name = try self.parseIdentifierOwned();
        var table_name_transferred = false;
        errdefer if (!table_name_transferred) self.alloc.free(table_name);
        if (self.matchKeyword("using")) {
            const method = self.match(.identifier) orelse return error.UnsupportedSqlShape;
            if (!std.ascii.eqlIgnoreCase(method.text, "btree")) return error.UnsupportedSqlShape;
        }

        var columns = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
        }
        var expressions = std.ArrayListUnmanaged(runtime_schema.UniqueExpression).empty;
        errdefer {
            for (expressions.items) |expression| self.alloc.free(expression.field);
            expressions.deinit(self.alloc);
        }
        try self.expect(.lparen);
        while (true) {
            if (self.matchKeyword("lower")) {
                try self.expect(.lparen);
                const field = try self.parseIdentifierOwned();
                var field_transferred = false;
                errdefer if (!field_transferred) self.alloc.free(field);
                try self.expect(.rparen);
                try self.parseDdlIndexElementOrderOptions();
                try expressions.append(self.alloc, .{ .op = .lower, .field = field });
                field_transferred = true;
            } else {
                const column = try self.parseIdentifierOwned();
                var column_transferred = false;
                errdefer if (!column_transferred) self.alloc.free(column);
                if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
                try self.parseDdlIndexElementOrderOptions();
                try columns.append(self.alloc, column);
                column_transferred = true;
            }
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);
        if (columns.items.len == 0 and expressions.items.len == 0) return error.UnsupportedSqlShape;

        var predicates: []const runtime_schema.UniquePredicate = &.{};
        errdefer freeDdlUniquePredicates(self.alloc, predicates);
        if (self.matchKeyword("where")) {
            predicates = try self.parseDdlUniquePredicatesAlloc();
        }
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const owned_columns = try columns.toOwnedSlice(self.alloc);
        var columns_transferred = false;
        errdefer if (!columns_transferred) freeStringSlice(self.alloc, owned_columns);
        const owned_expressions = try expressions.toOwnedSlice(self.alloc);
        var expressions_transferred = false;
        errdefer if (!expressions_transferred) freeDdlUniqueExpressions(self.alloc, owned_expressions);

        index_name_transferred = true;
        table_name_transferred = true;
        columns_transferred = true;
        expressions_transferred = true;
        return .{
            .index_name = index_name,
            .table_name = table_name,
            .unique = unique,
            .columns = owned_columns,
            .expressions = owned_expressions,
            .where = predicates,
        };
    }

    fn parseDdlIndexElementOrderOptions(self: *@This()) !void {
        _ = self.matchKeyword("asc") or self.matchKeyword("desc");
        if (self.matchKeyword("nulls")) {
            if (!(self.matchKeyword("first") or self.matchKeyword("last"))) return error.UnsupportedSqlShape;
        }
    }

    fn parseCreateTableDdl(self: *@This()) !CreateTablePlan {
        if (self.matchKeyword("temporary") or self.matchKeyword("temp") or self.matchKeyword("unlogged")) return error.UnsupportedSqlShape;
        try self.expectKeyword("table");
        if (self.matchKeyword("if")) {
            try self.expectKeyword("not");
            try self.expectKeyword("exists");
        }

        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);
        try self.expect(.lparen);

        var columns = std.ArrayListUnmanaged(runtime_schema.RelationalColumn).empty;
        errdefer {
            freeDdlRelationalColumns(self.alloc, columns.items);
        }
        var primary_key: ?runtime_schema.PrimaryKey = null;
        errdefer if (primary_key) |pk| freeDdlPrimaryKey(self.alloc, pk);
        var unique_constraints = std.ArrayListUnmanaged(runtime_schema.UniqueConstraint).empty;
        errdefer {
            freeDdlUniqueConstraints(self.alloc, unique_constraints.items);
        }
        var foreign_keys = std.ArrayListUnmanaged(runtime_schema.ForeignKey).empty;
        errdefer {
            freeDdlForeignKeys(self.alloc, foreign_keys.items);
        }
        var checks = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeDdlRelationalChecks(self.alloc, checks.items);
        }

        while (true) {
            const constraint_name = if (self.matchKeyword("constraint")) try self.parseIdentifierOwned() else null;
            var constraint_name_transferred = false;
            errdefer if (!constraint_name_transferred) if (constraint_name) |name| self.alloc.free(name);

            if (self.peekKeyword("primary") or self.peekKeyword("unique") or self.peekKeyword("foreign") or self.peekKeyword("check")) {
                try self.parseDdlTableConstraint(constraint_name, &primary_key, &unique_constraints, &foreign_keys, &checks);
                if (constraint_name) |name| self.alloc.free(name);
                constraint_name_transferred = true;
            } else {
                if (constraint_name != null) return error.UnsupportedSqlShape;
                try self.parseDdlColumnDefinition(&columns, &primary_key, &unique_constraints, &foreign_keys, &checks);
            }

            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;
        if (primary_key == null) return error.UnsupportedSqlShape;

        return .{
            .table_name = table_name,
            .columns = try columns.toOwnedSlice(self.alloc),
            .primary_key = primary_key,
            .unique_constraints = try unique_constraints.toOwnedSlice(self.alloc),
            .foreign_keys = try foreign_keys.toOwnedSlice(self.alloc),
            .checks = try checks.toOwnedSlice(self.alloc),
        };
    }

    const DdlType = struct {
        field_type: runtime_schema.AntflyType,
        array_item_type: ?runtime_schema.AntflyType = null,
    };

    fn parseDdlColumnDefinition(
        self: *@This(),
        columns: *std.ArrayListUnmanaged(runtime_schema.RelationalColumn),
        primary_key: *?runtime_schema.PrimaryKey,
        unique_constraints: *std.ArrayListUnmanaged(runtime_schema.UniqueConstraint),
        foreign_keys: *std.ArrayListUnmanaged(runtime_schema.ForeignKey),
        checks: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    ) !void {
        var column = try self.parseDdlColumnDefinitionStandalone();
        var column_transferred = false;
        errdefer if (!column_transferred) freeDdlRelationalColumn(self.alloc, column);
        if (findDdlColumn(columns.items, column.name) != null) return error.UnsupportedSqlShape;
        while (!self.atEnd() and !self.peekKind(.comma) and !self.peekKind(.rparen) and !self.peekKind(.semicolon)) {
            if (self.matchKeyword("not")) {
                try self.expectKeyword("null");
                column.nullable = false;
            } else if (self.matchKeyword("null")) {
                column.nullable = true;
            } else if (self.matchKeyword("default")) {
                if (column.default_value != null) return error.UnsupportedSqlShape;
                if (column.generated != null) return error.UnsupportedSqlShape;
                column.default_value = try self.parseDdlDefaultValue(column.field_type);
            } else if (self.matchKeyword("generated")) {
                if (column.default_value != null or column.generated != null) return error.UnsupportedSqlShape;
                column.generated = try self.parseDdlGeneratedValue();
            } else if (self.matchKeyword("primary")) {
                try self.expectKeyword("key");
                column.nullable = false;
                try self.installDdlPrimaryKey(primary_key, &.{column.name});
            } else if (self.matchKeyword("unique")) {
                try self.appendDdlUniqueConstraint(unique_constraints, null, &.{column.name});
            } else if (self.matchKeyword("check")) {
                const check = try self.parseDdlCheckConstraint(null);
                var check_transferred = false;
                errdefer if (!check_transferred) freeDdlRelationalCheck(self.alloc, check);
                try checks.append(self.alloc, check);
                check_transferred = true;
            } else if (self.matchKeyword("references")) {
                const foreign_key = try self.parseDdlInlineForeignKeyConstraint(column.name, null);
                var foreign_key_transferred = false;
                errdefer if (!foreign_key_transferred) freeDdlForeignKey(self.alloc, foreign_key);
                try foreign_keys.append(self.alloc, foreign_key);
                foreign_key_transferred = true;
            } else if (self.matchKeyword("constraint")) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        try columns.append(self.alloc, column);
        column_transferred = true;
    }

    fn parseDdlColumnDefinitionStandalone(self: *@This()) !runtime_schema.RelationalColumn {
        const name = try self.parseIdentifierOwned();
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);

        const ddl_type = try self.parseDdlType();
        const path = try self.alloc.dupe(u8, name);
        var path_transferred = false;
        errdefer if (!path_transferred) self.alloc.free(path);
        var column: runtime_schema.RelationalColumn = .{
            .name = name,
            .path = path,
            .field_type = ddl_type.field_type,
            .array_item_type = ddl_type.array_item_type,
            .nullable = true,
        };
        var column_transferred = false;
        errdefer if (!column_transferred) freeDdlRelationalColumn(self.alloc, column);
        name_transferred = true;
        path_transferred = true;

        while (!self.peekKind(.comma) and !self.peekKind(.rparen) and !self.peekKind(.semicolon) and !self.peekKeyword("primary") and !self.peekKeyword("unique") and !self.peekKeyword("check") and !self.peekKeyword("references") and !self.peekKeyword("constraint")) {
            if (self.matchKeyword("not")) {
                try self.expectKeyword("null");
                column.nullable = false;
            } else if (self.matchKeyword("null")) {
                column.nullable = true;
            } else if (self.matchKeyword("default")) {
                if (column.default_value != null) return error.UnsupportedSqlShape;
                if (column.generated != null) return error.UnsupportedSqlShape;
                column.default_value = try self.parseDdlDefaultValue(column.field_type);
            } else if (self.matchKeyword("generated")) {
                if (column.default_value != null or column.generated != null) return error.UnsupportedSqlShape;
                column.generated = try self.parseDdlGeneratedValue();
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        column_transferred = true;
        return column;
    }

    fn parseDdlGeneratedValue(self: *@This()) !runtime_schema.RelationalGeneratedValue {
        try self.expectKeyword("always");
        try self.expectKeyword("as");
        try self.expect(.lparen);
        const generated = try self.parseDdlGeneratedExpression();
        var generated_transferred = false;
        errdefer if (!generated_transferred) freeDdlGeneratedValue(self.alloc, generated);
        try self.expect(.rparen);
        try self.expectKeyword("stored");
        generated_transferred = true;
        return generated;
    }

    fn parseDdlGeneratedExpression(self: *@This()) !runtime_schema.RelationalGeneratedValue {
        if (self.matchKeyword("lower")) {
            try self.expect(.lparen);
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            try self.expect(.rparen);
            const separator = try self.alloc.dupe(u8, "");
            var separator_transferred = false;
            errdefer if (!separator_transferred) self.alloc.free(separator);
            field_transferred = true;
            separator_transferred = true;
            return .{ .op = .lower, .field = field, .separator = separator };
        }
        if (self.matchKeyword("concat")) {
            try self.expect(.lparen);
            var fields = std.ArrayListUnmanaged([]const u8).empty;
            errdefer {
                for (fields.items) |field| self.alloc.free(field);
                fields.deinit(self.alloc);
            }
            const first = try self.parseIdentifierOwned();
            var first_transferred = false;
            errdefer if (!first_transferred) self.alloc.free(first);
            try fields.append(self.alloc, first);
            first_transferred = true;

            var separator: ?[]const u8 = null;
            errdefer if (separator) |value| self.alloc.free(value);
            while (self.match(.comma) != null) {
                if (self.match(.string)) |token| {
                    if (separator) |existing| {
                        if (!std.mem.eql(u8, existing, token.text)) return error.UnsupportedSqlShape;
                    } else {
                        separator = try self.alloc.dupe(u8, token.text);
                    }
                    try self.expect(.comma);
                } else if (separator == null) {
                    separator = try self.alloc.dupe(u8, "");
                }
                const field = try self.parseIdentifierOwned();
                var field_transferred = false;
                errdefer if (!field_transferred) self.alloc.free(field);
                try fields.append(self.alloc, field);
                field_transferred = true;
            }
            try self.expect(.rparen);
            if (fields.items.len < 2) return error.UnsupportedSqlShape;
            const owned_fields = try fields.toOwnedSlice(self.alloc);
            var fields_transferred = false;
            errdefer if (!fields_transferred) freeStringSlice(self.alloc, owned_fields);
            const owned_separator = separator orelse try self.alloc.dupe(u8, "");
            separator = null;
            fields_transferred = true;
            return .{ .op = .concat, .fields = owned_fields, .separator = owned_separator };
        }
        return error.UnsupportedSqlShape;
    }

    fn parseDdlTableConstraint(
        self: *@This(),
        constraint_name: ?[]const u8,
        primary_key: *?runtime_schema.PrimaryKey,
        unique_constraints: *std.ArrayListUnmanaged(runtime_schema.UniqueConstraint),
        foreign_keys: *std.ArrayListUnmanaged(runtime_schema.ForeignKey),
        checks: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
    ) !void {
        if (self.matchKeyword("primary")) {
            try self.expectKeyword("key");
            const columns = try self.parseDdlColumnListAlloc();
            defer freeStringSlice(self.alloc, columns);
            try self.installDdlPrimaryKey(primary_key, columns);
        } else if (self.matchKeyword("unique")) {
            const columns = try self.parseDdlColumnListAlloc();
            defer freeStringSlice(self.alloc, columns);
            try self.appendDdlUniqueConstraint(unique_constraints, constraint_name, columns);
        } else if (self.matchKeyword("foreign")) {
            const foreign_key = try self.parseDdlForeignKeyConstraint(constraint_name);
            var transferred = false;
            errdefer if (!transferred) freeDdlForeignKey(self.alloc, foreign_key);
            try foreign_keys.append(self.alloc, foreign_key);
            transferred = true;
        } else if (self.matchKeyword("check")) {
            const check = try self.parseDdlCheckConstraint(constraint_name);
            var transferred = false;
            errdefer if (!transferred) freeDdlRelationalCheck(self.alloc, check);
            try checks.append(self.alloc, check);
            transferred = true;
        } else {
            return error.UnsupportedSqlShape;
        }
    }

    fn parseDdlType(self: *@This()) !DdlType {
        const first = self.match(.identifier) orelse return error.UnsupportedSqlShape;
        const base = ddlBaseTypeForName(first.text) orelse blk: {
            if (std.ascii.eqlIgnoreCase(first.text, "character")) {
                try self.expectKeyword("varying");
                break :blk runtime_schema.AntflyType.keyword;
            }
            if (std.ascii.eqlIgnoreCase(first.text, "double")) {
                try self.expectKeyword("precision");
                break :blk runtime_schema.AntflyType.numeric;
            }
            if (std.ascii.eqlIgnoreCase(first.text, "timestamp")) {
                if (self.matchKeyword("with")) {
                    try self.expectKeyword("time");
                    try self.expectKeyword("zone");
                } else if (self.matchKeyword("without")) {
                    try self.expectKeyword("time");
                    try self.expectKeyword("zone");
                }
                break :blk runtime_schema.AntflyType.datetime;
            }
            return error.UnsupportedSqlShape;
        };

        if (self.peekKind(.lparen)) try self.skipParenthesizedTokens();
        const is_array = if (self.match(.lbracket) != null) blk: {
            try self.expect(.rbracket);
            break :blk true;
        } else false;
        if (!is_array) return .{ .field_type = base };
        if (base == .json or base == .array or base == .blob) return error.UnsupportedSqlShape;
        return .{ .field_type = .array, .array_item_type = base };
    }

    fn parseDdlDefaultValue(self: *@This(), field_type: runtime_schema.AntflyType) !runtime_schema.RelationalDefaultValue {
        if (self.matchKeyword("null")) {
            return .{ .kind = .literal, .value_json = try self.alloc.dupe(u8, "null") };
        }
        if (self.matchKeyword("gen_random_uuid") or self.matchKeyword("uuid_generate_v4")) {
            try self.expect(.lparen);
            try self.expect(.rparen);
            if (field_type != .keyword and field_type != .text and field_type != .link) return error.UnsupportedSqlShape;
            return .{ .kind = .uuid_v4, .value_json = try self.alloc.dupe(u8, "") };
        }
        if (self.matchKeyword("now")) {
            try self.expect(.lparen);
            try self.expect(.rparen);
            if (field_type != .numeric and field_type != .datetime) return error.UnsupportedSqlShape;
            return .{ .kind = .now_ns, .value_json = try self.alloc.dupe(u8, "") };
        }
        if (self.matchKeyword("current_timestamp")) {
            if (field_type != .numeric and field_type != .datetime) return error.UnsupportedSqlShape;
            return .{ .kind = .now_ns, .value_json = try self.alloc.dupe(u8, "") };
        }
        const value = try self.parseSqlColumnValueAlloc(.{ .name = "", .path = "", .field_type = field_type });
        return .{ .kind = .literal, .value_json = value };
    }

    fn parseDdlCheckConstraint(self: *@This(), constraint_name: ?[]const u8) !runtime_schema.RelationalCheck {
        try self.expect(.lparen);
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("is")) blk: {
            if (self.matchKeyword("not")) {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            }
            try self.expectKeyword("null");
            break :blk .is_null;
        } else try self.parseComparisonOp();
        const value_json = if (op == .is_null or op == .is_not_null)
            null
        else
            try self.parseSqlUntypedValueJsonAlloc();
        var value_transferred = false;
        errdefer if (!value_transferred) if (value_json) |json| self.alloc.free(json);
        try self.expect(.rparen);
        const name = if (constraint_name) |name|
            try self.alloc.dupe(u8, name)
        else
            try std.fmt.allocPrint(self.alloc, "{s}_{s}_check", .{ field, relationalCheckOpToken(op) });
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);
        field_transferred = true;
        value_transferred = true;
        name_transferred = true;
        return .{ .name = name, .field = field, .op = op, .value_json = value_json };
    }

    fn parseDdlForeignKeyConstraint(self: *@This(), constraint_name: ?[]const u8) !runtime_schema.ForeignKey {
        try self.expectKeyword("key");
        const child_columns = try self.parseDdlColumnListAlloc();
        var child_transferred = false;
        errdefer if (!child_transferred) freeStringSlice(self.alloc, child_columns);

        try self.expectKeyword("references");
        const parent_table = try self.parseIdentifierOwned();
        var parent_table_transferred = false;
        errdefer if (!parent_table_transferred) self.alloc.free(parent_table);
        const parent_columns = try self.parseDdlColumnListAlloc();
        var parent_transferred = false;
        errdefer if (!parent_transferred) freeStringSlice(self.alloc, parent_columns);

        const options = try self.parseDdlForeignKeyOptions();

        const name = if (constraint_name) |existing|
            try self.alloc.dupe(u8, existing)
        else
            try std.fmt.allocPrint(self.alloc, "{s}_{s}_fkey", .{ parent_table, child_columns[0] });
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);

        child_transferred = true;
        parent_table_transferred = true;
        parent_transferred = true;
        name_transferred = true;
        return .{
            .name = name,
            .child_columns = child_columns,
            .parent_table = parent_table,
            .parent_columns = parent_columns,
            .on_delete = options.on_delete,
            .on_update = options.on_update,
            .deferrable = options.deferrable,
            .timing = options.timing,
        };
    }

    fn parseDdlInlineForeignKeyConstraint(
        self: *@This(),
        column_name: []const u8,
        constraint_name: ?[]const u8,
    ) !runtime_schema.ForeignKey {
        const child_columns = try cloneStringSlice(self.alloc, &.{column_name});
        var child_transferred = false;
        errdefer if (!child_transferred) freeStringSlice(self.alloc, child_columns);

        const parent_table = try self.parseIdentifierOwned();
        var parent_table_transferred = false;
        errdefer if (!parent_table_transferred) self.alloc.free(parent_table);

        const parent_columns = if (self.peekKind(.lparen))
            try self.parseDdlColumnListAlloc()
        else
            try cloneStringSlice(self.alloc, &.{"id"});
        var parent_transferred = false;
        errdefer if (!parent_transferred) freeStringSlice(self.alloc, parent_columns);

        const options = try self.parseDdlForeignKeyOptions();

        const name = if (constraint_name) |existing|
            try self.alloc.dupe(u8, existing)
        else
            try std.fmt.allocPrint(self.alloc, "{s}_{s}_fkey", .{ parent_table, child_columns[0] });
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);

        child_transferred = true;
        parent_table_transferred = true;
        parent_transferred = true;
        name_transferred = true;
        return .{
            .name = name,
            .child_columns = child_columns,
            .parent_table = parent_table,
            .parent_columns = parent_columns,
            .on_delete = options.on_delete,
            .on_update = options.on_update,
            .deferrable = options.deferrable,
            .timing = options.timing,
        };
    }

    const DdlForeignKeyOptions = struct {
        on_delete: runtime_schema.ForeignKeyAction = .restrict,
        on_update: runtime_schema.ForeignKeyAction = .restrict,
        deferrable: bool = false,
        timing: runtime_schema.ForeignKeyTiming = .immediate,
    };

    fn parseDdlForeignKeyOptions(self: *@This()) !DdlForeignKeyOptions {
        var options: DdlForeignKeyOptions = .{};
        while (!self.atEnd() and !self.peekKind(.comma) and !self.peekKind(.rparen) and !self.peekKind(.semicolon)) {
            if (self.peekDdlNotValid()) break;
            if (self.matchKeyword("on")) {
                if (self.matchKeyword("delete")) {
                    options.on_delete = try self.parseDdlForeignKeyAction();
                } else if (self.matchKeyword("update")) {
                    options.on_update = try self.parseDdlForeignKeyAction();
                } else {
                    return error.UnsupportedSqlShape;
                }
            } else if (self.matchKeyword("deferrable")) {
                options.deferrable = true;
            } else if (self.matchKeyword("not")) {
                try self.expectKeyword("deferrable");
                options.deferrable = false;
            } else if (self.matchKeyword("initially")) {
                if (self.matchKeyword("deferred")) {
                    options.timing = .deferred;
                    options.deferrable = true;
                } else {
                    try self.expectKeyword("immediate");
                    options.timing = .immediate;
                }
            } else {
                return error.UnsupportedSqlShape;
            }
        }
        return options;
    }

    fn consumeOptionalDdlNotValid(self: *@This()) bool {
        if (!self.peekDdlNotValid()) return false;
        self.pos += 2;
        return true;
    }

    fn peekDdlNotValid(self: *@This()) bool {
        if (self.pos + 1 >= self.tokens.len) return false;
        const not_token = self.tokens[self.pos];
        const valid_token = self.tokens[self.pos + 1];
        return not_token.kind == .identifier and valid_token.kind == .identifier and
            std.ascii.eqlIgnoreCase(not_token.text, "not") and
            std.ascii.eqlIgnoreCase(valid_token.text, "valid");
    }

    fn parseDdlForeignKeyAction(self: *@This()) !runtime_schema.ForeignKeyAction {
        if (self.matchKeyword("cascade")) return .cascade;
        if (self.matchKeyword("restrict")) return .restrict;
        if (self.matchKeyword("no")) {
            try self.expectKeyword("action");
            return .no_action;
        }
        if (self.matchKeyword("set")) {
            try self.expectKeyword("null");
            return .set_null;
        }
        return error.UnsupportedSqlShape;
    }

    fn parseDdlColumnListAlloc(self: *@This()) ![]const []const u8 {
        try self.expect(.lparen);
        var columns = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
        }
        while (true) {
            const column = try self.parseIdentifierOwned();
            var transferred = false;
            errdefer if (!transferred) self.alloc.free(column);
            try columns.append(self.alloc, column);
            transferred = true;
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);
        return try columns.toOwnedSlice(self.alloc);
    }

    fn parseDdlUniquePredicatesAlloc(self: *@This()) ![]const runtime_schema.UniquePredicate {
        var predicates = std.ArrayListUnmanaged(runtime_schema.UniquePredicate).empty;
        errdefer {
            for (predicates.items) |predicate| {
                self.alloc.free(predicate.field);
                if (predicate.value_json) |value| self.alloc.free(value);
            }
            predicates.deinit(self.alloc);
        }
        while (true) {
            const atom_start = self.pos;
            var depth: usize = 0;
            while (self.pos < self.tokens.len) {
                const token = self.tokens[self.pos];
                if (depth == 0 and token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "and")) break;
                if (depth == 0 and token.kind == .semicolon) break;
                switch (token.kind) {
                    .lparen => depth += 1,
                    .rparen => {
                        if (depth == 0) return error.UnsupportedSqlShape;
                        depth -= 1;
                    },
                    else => {},
                }
                self.pos += 1;
            }
            if (depth != 0 or atom_start == self.pos) return error.UnsupportedSqlShape;

            const predicate = try self.parseDdlUniquePredicateAtomAlloc(self.tokens[atom_start..self.pos]);
            var predicate_transferred = false;
            errdefer if (!predicate_transferred) {
                self.alloc.free(predicate.field);
                if (predicate.value_json) |value| self.alloc.free(value);
            };
            try predicates.append(self.alloc, predicate);
            predicate_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
        return try predicates.toOwnedSlice(self.alloc);
    }

    fn parseDdlUniquePredicateAtomAlloc(self: *@This(), raw_tokens: []const Token) !runtime_schema.UniquePredicate {
        const tokens = stripDdlPredicateOuterParens(raw_tokens);
        if (tokens.len == 0) return error.UnsupportedSqlShape;

        var idx: usize = 0;
        const field_token = try parseDdlPredicateIdentifierOperand(tokens, &idx);
        const field = try self.alloc.dupe(u8, field_token.text);
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);

        if (idx >= tokens.len) return error.UnsupportedSqlShape;
        if (tokens[idx].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[idx].text, "is")) {
            idx += 1;
            const op: runtime_schema.UniquePredicateOp = if (idx < tokens.len and tokens[idx].kind == .identifier and std.ascii.eqlIgnoreCase(tokens[idx].text, "not")) blk: {
                idx += 1;
                if (idx >= tokens.len or tokens[idx].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[idx].text, "null")) return error.UnsupportedSqlShape;
                idx += 1;
                break :blk .is_not_null;
            } else blk: {
                if (idx >= tokens.len or tokens[idx].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[idx].text, "null")) return error.UnsupportedSqlShape;
                idx += 1;
                break :blk .is_null;
            };
            if (idx != tokens.len) return error.UnsupportedSqlShape;
            field_transferred = true;
            return .{ .field = field, .op = op };
        }

        const op: runtime_schema.UniquePredicateOp = if (tokens[idx].kind == .eq) .eq else if (tokens[idx].kind == .neq) .ne else return error.UnsupportedSqlShape;
        idx += 1;
        if (idx >= tokens.len) return error.UnsupportedSqlShape;
        const value_json = try self.sqlUntypedValueTokenJsonAlloc(tokens[idx]);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        idx += 1;
        if (idx != tokens.len) return error.UnsupportedSqlShape;

        field_transferred = true;
        value_transferred = true;
        return .{ .field = field, .op = op, .value_json = value_json };
    }

    fn sqlUntypedValueTokenJsonAlloc(self: *@This(), token: Token) ![]const u8 {
        if (token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "true")) return try self.alloc.dupe(u8, "true");
        if (token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "false")) return try self.alloc.dupe(u8, "false");
        if (token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "null")) return try self.alloc.dupe(u8, "null");
        if (token.kind == .string) return try std.json.Stringify.valueAlloc(self.alloc, token.text, .{});
        if (token.kind == .number) return try self.alloc.dupe(u8, token.text);
        return error.UnsupportedSqlShape;
    }

    fn installDdlPrimaryKey(self: *@This(), primary_key: *?runtime_schema.PrimaryKey, columns: []const []const u8) !void {
        if (primary_key.* != null) return error.UnsupportedSqlShape;
        primary_key.* = .{ .columns = try cloneStringSlice(self.alloc, columns) };
    }

    fn appendDdlUniqueConstraint(
        self: *@This(),
        unique_constraints: *std.ArrayListUnmanaged(runtime_schema.UniqueConstraint),
        constraint_name: ?[]const u8,
        columns: []const []const u8,
    ) !void {
        const constraint = try self.makeDdlUniqueConstraint(constraint_name, columns);
        var transferred = false;
        errdefer if (!transferred) freeDdlUniqueConstraint(self.alloc, constraint);
        try unique_constraints.append(self.alloc, constraint);
        transferred = true;
    }

    fn makeDdlUniqueConstraint(
        self: *@This(),
        constraint_name: ?[]const u8,
        columns: []const []const u8,
    ) !runtime_schema.UniqueConstraint {
        const owned_columns = try cloneStringSlice(self.alloc, columns);
        var columns_transferred = false;
        errdefer if (!columns_transferred) freeStringSlice(self.alloc, owned_columns);
        const name = if (constraint_name) |existing|
            try self.alloc.dupe(u8, existing)
        else
            try self.defaultUniqueConstraintNameAlloc(columns);
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);
        columns_transferred = true;
        name_transferred = true;
        return .{ .name = name, .columns = owned_columns };
    }

    fn defaultUniqueConstraintNameAlloc(self: *@This(), columns: []const []const u8) ![]const u8 {
        var buf = std.ArrayListUnmanaged(u8).empty;
        errdefer buf.deinit(self.alloc);
        try buf.appendSlice(self.alloc, columns[0]);
        for (columns[1..]) |column| {
            try buf.append(self.alloc, '_');
            try buf.appendSlice(self.alloc, column);
        }
        try buf.appendSlice(self.alloc, "_key");
        return try buf.toOwnedSlice(self.alloc);
    }

    fn parseSqlUntypedValueJsonAlloc(self: *@This()) ![]const u8 {
        if (self.matchKeyword("true")) return try self.alloc.dupe(u8, "true");
        if (self.matchKeyword("false")) return try self.alloc.dupe(u8, "false");
        if (self.matchKeyword("null")) return try self.alloc.dupe(u8, "null");
        if (self.match(.string)) |token| return try std.json.Stringify.valueAlloc(self.alloc, token.text, .{});
        if (self.match(.number)) |token| return try self.alloc.dupe(u8, token.text);
        return error.UnsupportedSqlShape;
    }

    fn skipParenthesizedTokens(self: *@This()) !void {
        try self.expect(.lparen);
        var depth: usize = 1;
        while (self.pos < self.tokens.len and depth > 0) {
            if (self.match(.lparen) != null) {
                depth += 1;
            } else if (self.match(.rparen) != null) {
                depth -= 1;
            } else {
                self.pos += 1;
            }
        }
        if (depth != 0) return error.UnsupportedSqlShape;
    }

    fn parseQueryPlan(self: *@This()) !LoweredQueryPlan {
        if (!self.peekKeyword("with")) {
            var lowered = try self.parseSelect();
            errdefer lowered.deinit(self.alloc);
            const table_name = lowered.table_name;
            lowered.table_name = "";
            return .{
                .table_name = table_name,
                .plan = .{ .query = lowered.query },
            };
        }

        try self.expectKeyword("with");
        var ctes = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCte).empty;
        errdefer {
            for (ctes.items) |cte| {
                self.alloc.free(cte.name);
                var query = cte.query;
                query.deinit(self.alloc);
            }
            ctes.deinit(self.alloc);
        }
        var base_table_name: ?[]const u8 = null;
        errdefer if (base_table_name) |table| self.alloc.free(table);

        while (true) {
            const cte_name = try self.parseIdentifierOwned();
            var cte_name_transferred = false;
            errdefer if (!cte_name_transferred) self.alloc.free(cte_name);
            if (findCteByName(ctes.items, cte_name) != null) return error.UnsupportedSqlShape;
            try self.expectKeyword("as");
            try self.expect(.lparen);
            const close_index = try self.findMatchingRParen();
            var sub = Parser{
                .alloc = self.alloc,
                .tokens = self.tokens[self.pos..close_index],
                .schema = self.schema,
                .params = self.params,
                .unique_resolver = self.unique_resolver,
            };
            var lowered = try sub.parseSelect();
            errdefer lowered.deinit(self.alloc);
            self.pos = close_index + 1;
            try self.resolveSelectSourceForPlan(&lowered, ctes.items, &base_table_name);
            try ctes.append(self.alloc, .{
                .name = cte_name,
                .query = lowered.query,
            });
            lowered.query = .{};
            self.alloc.free(lowered.table_name);
            lowered.table_name = "";
            cte_name_transferred = true;
            if (self.match(.comma) == null) break;
        }

        var final = try self.parseSelect();
        errdefer final.deinit(self.alloc);
        try self.resolveSelectSourceForPlan(&final, ctes.items, &base_table_name);
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const table_name = base_table_name orelse return error.UnsupportedSqlShape;
        base_table_name = null;
        const owned_ctes = try ctes.toOwnedSlice(self.alloc);
        self.alloc.free(final.table_name);
        final.table_name = "";
        return .{
            .table_name = table_name,
            .plan = .{
                .ctes = owned_ctes,
                .query = final.query,
            },
        };
    }

    fn parseWindowPlan(self: *@This()) !LoweredWindowPlan {
        if (!self.peekKeyword("with")) return try self.parseWindowSelect();

        try self.expectKeyword("with");
        var ctes = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCte).empty;
        errdefer {
            for (ctes.items) |cte| {
                self.alloc.free(cte.name);
                var query = cte.query;
                query.deinit(self.alloc);
            }
            ctes.deinit(self.alloc);
        }
        var base_table_name: ?[]const u8 = null;
        errdefer if (base_table_name) |table| self.alloc.free(table);

        while (true) {
            const cte_name = try self.parseIdentifierOwned();
            var cte_name_transferred = false;
            errdefer if (!cte_name_transferred) self.alloc.free(cte_name);
            if (findCteByName(ctes.items, cte_name) != null) return error.UnsupportedSqlShape;
            try self.expectKeyword("as");
            try self.expect(.lparen);
            const close_index = try self.findMatchingRParen();
            var sub = Parser{
                .alloc = self.alloc,
                .tokens = self.tokens[self.pos..close_index],
                .schema = self.schema,
                .params = self.params,
                .unique_resolver = self.unique_resolver,
            };
            var lowered = try sub.parseSelect();
            errdefer lowered.deinit(self.alloc);
            self.pos = close_index + 1;
            try self.resolveSelectSourceForPlan(&lowered, ctes.items, &base_table_name);
            try ctes.append(self.alloc, .{
                .name = cte_name,
                .query = lowered.query,
            });
            lowered.query = .{};
            self.alloc.free(lowered.table_name);
            lowered.table_name = "";
            cte_name_transferred = true;
            if (self.match(.comma) == null) break;
        }

        var final = try self.parseWindowSelect();
        errdefer final.deinit(self.alloc);
        try self.resolveWindowSourceForPlan(&final, ctes.items, &base_table_name);
        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;

        const table_name = base_table_name orelse return error.UnsupportedSqlShape;
        base_table_name = null;
        const owned_ctes = try ctes.toOwnedSlice(self.alloc);
        self.alloc.free(final.table_name);
        final.table_name = table_name;
        final.plan.ctes = owned_ctes;
        return final;
    }

    fn resolveSelectSourceForPlan(
        self: *@This(),
        lowered: *LoweredSelect,
        ctes: []const db_mod.types.RelationalRowsCte,
        base_table_name: *?[]const u8,
    ) !void {
        if (findCteByName(ctes, lowered.table_name) != null) {
            lowered.query.source_cte = try self.alloc.dupe(u8, lowered.table_name);
            return;
        }
        if (base_table_name.*) |base| {
            if (!std.mem.eql(u8, base, lowered.table_name)) return error.UnsupportedSqlShape;
        } else {
            base_table_name.* = try self.alloc.dupe(u8, lowered.table_name);
        }
    }

    fn resolveWindowSourceForPlan(
        self: *@This(),
        lowered: *LoweredWindowPlan,
        ctes: []const db_mod.types.RelationalRowsCte,
        base_table_name: *?[]const u8,
    ) !void {
        if (findCteByName(ctes, lowered.table_name) != null) {
            lowered.plan.window.source.source_cte = try self.alloc.dupe(u8, lowered.table_name);
            return;
        }
        if (base_table_name.*) |base| {
            if (!std.mem.eql(u8, base, lowered.table_name)) return error.UnsupportedSqlShape;
        } else {
            base_table_name.* = try self.alloc.dupe(u8, lowered.table_name);
        }
    }

    fn findMatchingRParen(self: *@This()) !usize {
        var depth: usize = 1;
        var i = self.pos;
        while (i < self.tokens.len) : (i += 1) {
            switch (self.tokens[i].kind) {
                .lparen => depth += 1,
                .rparen => {
                    depth -= 1;
                    if (depth == 0) return i;
                },
                else => {},
            }
        }
        return error.UnsupportedSqlShape;
    }

    fn parseSelect(self: *@This()) !LoweredSelect {
        try self.expectKeyword("select");

        const select = try self.parseSelectList();
        errdefer freeStringSlice(self.alloc, select.fields);
        errdefer freeJsonExtract(self.alloc, select.json_extract);
        errdefer freeArrayLengthProjections(self.alloc, select.array_length);
        errdefer freeCoalesceProjections(self.alloc, select.coalesce);
        errdefer freeFieldAliasProjections(self.alloc, select.field_aliases);
        errdefer freeExpressionProjections(self.alloc, select.expressions);

        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
        errdefer {
            freeJsonPathEq(self.alloc, json_path_eq.items);
            json_path_eq.deinit(self.alloc);
        }
        var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
        errdefer {
            freeJsonContains(self.alloc, json_contains.items);
            json_contains.deinit(self.alloc);
        }
        var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
        errdefer {
            freeJsonPathExists(self.alloc, json_path_exists.items);
            json_path_exists.deinit(self.alloc);
        }
        var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
        errdefer {
            freeArrayContains(self.alloc, array_contains.items);
            array_contains.deinit(self.alloc);
        }
        var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
        errdefer {
            freeArrayEq(self.alloc, array_eq.items);
            array_eq.deinit(self.alloc);
        }
        var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
        errdefer {
            freeInPredicates(self.alloc, in_predicates.items);
            in_predicates.deinit(self.alloc);
        }
        var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, or_predicates.items);
            or_predicates.deinit(self.alloc);
        }
        var not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, not_predicates.items);
            not_predicates.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var row_claim: ?db_mod.types.RowClaimRequest = null;
        var limit: ?u32 = null;
        var offset: u32 = 0;

        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseWhere(&predicates, &json_contains, &json_path_eq, &json_path_exists, &array_contains, &array_eq, &in_predicates, &or_predicates, &not_predicates);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseOrderBy(&order_by);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.matchKeyword("for")) {
                try self.expectKeyword("update");
                const skip_locked = if (self.matchKeyword("skip")) blk: {
                    try self.expectKeyword("locked");
                    break :blk true;
                } else false;
                row_claim = .{ .mode = .for_update, .skip_locked = skip_locked };
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        return .{
            .table_name = table_name,
            .query = .{
                .predicates = try predicates.toOwnedSlice(self.alloc),
                .array_contains = try array_contains.toOwnedSlice(self.alloc),
                .array_eq = try array_eq.toOwnedSlice(self.alloc),
                .in_predicates = try in_predicates.toOwnedSlice(self.alloc),
                .json_contains = try json_contains.toOwnedSlice(self.alloc),
                .json_path_eq = try json_path_eq.toOwnedSlice(self.alloc),
                .json_path_exists = try json_path_exists.toOwnedSlice(self.alloc),
                .or_predicates = try or_predicates.toOwnedSlice(self.alloc),
                .not_predicates = try not_predicates.toOwnedSlice(self.alloc),
                .select = select.fields,
                .json_extract = select.json_extract,
                .array_length = select.array_length,
                .coalesce = select.coalesce,
                .field_aliases = select.field_aliases,
                .expressions = select.expressions,
                .select_all = select.select_all,
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .row_claim = row_claim,
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseWindowSelect(self: *@This()) !LoweredWindowPlan {
        try self.expectKeyword("select");

        const select = try self.parseWindowSelectList();
        errdefer freeStringSlice(self.alloc, select.fields);
        errdefer freeWindowSpecs(self.alloc, select.windows);
        errdefer if (select.windows.len > 0) self.alloc.free(select.windows);
        if (select.windows.len == 0) return error.UnsupportedSqlShape;

        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
        errdefer {
            freeJsonPathEq(self.alloc, json_path_eq.items);
            json_path_eq.deinit(self.alloc);
        }
        var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
        errdefer {
            freeJsonContains(self.alloc, json_contains.items);
            json_contains.deinit(self.alloc);
        }
        var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
        errdefer {
            freeJsonPathExists(self.alloc, json_path_exists.items);
            json_path_exists.deinit(self.alloc);
        }
        var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
        errdefer {
            freeArrayContains(self.alloc, array_contains.items);
            array_contains.deinit(self.alloc);
        }
        var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
        errdefer {
            freeArrayEq(self.alloc, array_eq.items);
            array_eq.deinit(self.alloc);
        }
        var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
        errdefer {
            freeInPredicates(self.alloc, in_predicates.items);
            in_predicates.deinit(self.alloc);
        }
        var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, or_predicates.items);
            or_predicates.deinit(self.alloc);
        }
        var not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, not_predicates.items);
            not_predicates.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var limit: ?u32 = null;
        var offset: u32 = 0;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseWhere(&predicates, &json_contains, &json_path_eq, &json_path_exists, &array_contains, &array_eq, &in_predicates, &or_predicates, &not_predicates);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseWindowOutputOrderBy(&order_by, select.fields, select.windows);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.matchKeyword("for")) {
                return error.UnsupportedSqlShape;
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        return .{
            .table_name = table_name,
            .plan = .{
                .window = .{
                    .source = .{
                        .predicates = try predicates.toOwnedSlice(self.alloc),
                        .array_contains = try array_contains.toOwnedSlice(self.alloc),
                        .array_eq = try array_eq.toOwnedSlice(self.alloc),
                        .in_predicates = try in_predicates.toOwnedSlice(self.alloc),
                        .json_contains = try json_contains.toOwnedSlice(self.alloc),
                        .json_path_eq = try json_path_eq.toOwnedSlice(self.alloc),
                        .json_path_exists = try json_path_exists.toOwnedSlice(self.alloc),
                        .or_predicates = try or_predicates.toOwnedSlice(self.alloc),
                        .not_predicates = try not_predicates.toOwnedSlice(self.alloc),
                        .select_all = true,
                    },
                    .windows = select.windows,
                    .select = select.fields,
                    .select_all = select.select_all,
                    .order_by = try order_by.toOwnedSlice(self.alloc),
                    .limit = limit,
                    .offset = offset,
                },
            },
        };
    }

    fn parseAggregate(self: *@This()) !LoweredAggregate {
        try self.expectKeyword("select");

        const select = try self.parseAggregateSelectList();
        defer freeStringSlice(self.alloc, select.group_fields);
        errdefer {
            freeAggregateSpecs(self.alloc, select.aggregations);
            if (select.aggregations.len > 0) self.alloc.free(select.aggregations);
        }
        if (select.aggregations.len == 0) return error.UnsupportedSqlShape;

        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
        errdefer {
            freeJsonPathEq(self.alloc, json_path_eq.items);
            json_path_eq.deinit(self.alloc);
        }
        var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
        errdefer {
            freeJsonContains(self.alloc, json_contains.items);
            json_contains.deinit(self.alloc);
        }
        var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
        errdefer {
            freeJsonPathExists(self.alloc, json_path_exists.items);
            json_path_exists.deinit(self.alloc);
        }
        var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
        errdefer {
            freeArrayContains(self.alloc, array_contains.items);
            array_contains.deinit(self.alloc);
        }
        var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
        errdefer {
            freeArrayEq(self.alloc, array_eq.items);
            array_eq.deinit(self.alloc);
        }
        var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
        errdefer {
            freeInPredicates(self.alloc, in_predicates.items);
            in_predicates.deinit(self.alloc);
        }
        var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, or_predicates.items);
            or_predicates.deinit(self.alloc);
        }
        var not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, not_predicates.items);
            not_predicates.deinit(self.alloc);
        }
        var group_by = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (group_by.items) |field| self.alloc.free(field);
            group_by.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }
        var having_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, having_predicates.items);
            having_predicates.deinit(self.alloc);
        }

        var limit: ?u32 = null;
        var offset: u32 = 0;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseWhere(&predicates, &json_contains, &json_path_eq, &json_path_exists, &array_contains, &array_eq, &in_predicates, &or_predicates, &not_predicates);
            } else if (self.matchKeyword("group")) {
                try self.expectKeyword("by");
                try self.parseGroupBy(&group_by);
            } else if (self.matchKeyword("having")) {
                try self.parseAggregateHaving(&having_predicates, select.group_fields, select.aggregations);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseAggregateOrderBy(&order_by, select.group_fields, select.aggregations);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        try validateAggregateGroupBy(select.group_fields, group_by.items);

        return .{
            .table_name = table_name,
            .aggregate = .{
                .source = .{
                    .predicates = try predicates.toOwnedSlice(self.alloc),
                    .array_contains = try array_contains.toOwnedSlice(self.alloc),
                    .array_eq = try array_eq.toOwnedSlice(self.alloc),
                    .in_predicates = try in_predicates.toOwnedSlice(self.alloc),
                    .json_contains = try json_contains.toOwnedSlice(self.alloc),
                    .json_path_eq = try json_path_eq.toOwnedSlice(self.alloc),
                    .json_path_exists = try json_path_exists.toOwnedSlice(self.alloc),
                    .or_predicates = try or_predicates.toOwnedSlice(self.alloc),
                    .not_predicates = try not_predicates.toOwnedSlice(self.alloc),
                    .select_all = true,
                },
                .group_by = try group_by.toOwnedSlice(self.alloc),
                .aggregations = select.aggregations,
                .having_predicates = try having_predicates.toOwnedSlice(self.alloc),
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseJoin(self: *@This()) !LoweredJoin {
        try self.expectKeyword("select");

        const raw_select = try self.parseJoinProjectionListAlloc();
        defer freeQualifiedProjections(self.alloc, raw_select);

        try self.expectKeyword("from");
        const left_table = try self.parseTableAliasAlloc();
        defer freeTableAlias(self.alloc, left_table);

        const join_type: db_mod.types.RelationalRowsJoinType = if (self.matchKeyword("left")) blk: {
            _ = self.matchKeyword("outer");
            try self.expectKeyword("join");
            break :blk .left;
        } else blk: {
            _ = self.matchKeyword("inner");
            try self.expectKeyword("join");
            break :blk .inner;
        };

        const right_table = try self.parseTableAliasAlloc();
        defer freeTableAlias(self.alloc, right_table);
        if (std.mem.eql(u8, left_table.alias, right_table.alias)) return error.UnsupportedSqlShape;

        try self.expectKeyword("on");
        var on = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinOn).empty;
        errdefer {
            freeJoinOn(self.alloc, on.items);
            on.deinit(self.alloc);
        }
        try self.parseJoinOn(&on, left_table.alias, right_table.alias);

        var left_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, left_predicates.items);
            left_predicates.deinit(self.alloc);
        }
        var right_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, right_predicates.items);
            right_predicates.deinit(self.alloc);
        }

        var select = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection).empty;
        errdefer {
            freeJoinProjections(self.alloc, select.items);
            select.deinit(self.alloc);
        }
        try self.resolveJoinProjectionsAlloc(raw_select, left_table.alias, right_table.alias, &select);

        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var limit: ?u32 = null;
        var offset: u32 = 0;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseJoinWhere(&left_predicates, &right_predicates, left_table.alias, right_table.alias);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseJoinOrderBy(&order_by, select.items);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        return .{
            .left_table_name = try self.alloc.dupe(u8, left_table.name),
            .right_table_name = try self.alloc.dupe(u8, right_table.name),
            .join = .{
                .left = .{
                    .predicates = try left_predicates.toOwnedSlice(self.alloc),
                    .select_all = true,
                },
                .right = .{
                    .predicates = try right_predicates.toOwnedSlice(self.alloc),
                    .select_all = true,
                },
                .on = try on.toOwnedSlice(self.alloc),
                .join_type = join_type,
                .select = try select.toOwnedSlice(self.alloc),
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .limit = limit,
                .offset = offset,
            },
        };
    }

    fn parseLateral(self: *@This()) !LoweredLateralPlan {
        try self.expectKeyword("select");

        const raw_select = try self.parseJoinProjectionListAlloc();
        defer freeQualifiedProjections(self.alloc, raw_select);

        try self.expectKeyword("from");
        const left_table = try self.parseTableAliasAlloc();
        defer freeTableAlias(self.alloc, left_table);

        try self.expectKeyword("left");
        _ = self.matchKeyword("outer");
        try self.expectKeyword("join");
        try self.expectKeyword("lateral");
        try self.expect(.lparen);
        const close_index = try self.findMatchingRParen();
        var sub = Parser{
            .alloc = self.alloc,
            .tokens = self.tokens[self.pos..close_index],
            .schema = self.schema,
            .params = self.params,
            .unique_resolver = self.unique_resolver,
        };
        var lateral_subquery = try sub.parseLateralSubqueryAlloc(left_table.alias);
        errdefer freeLateralSubquery(self.alloc, lateral_subquery);
        self.pos = close_index + 1;

        const lateral_alias = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.parseIdentifierOwned();
        defer self.alloc.free(lateral_alias);

        try self.expectKeyword("on");
        try self.expectKeyword("true");

        var left_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, left_predicates.items);
            left_predicates.deinit(self.alloc);
        }
        var unsupported_right_predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        defer {
            freeRelationalChecks(self.alloc, unsupported_right_predicates.items);
            unsupported_right_predicates.deinit(self.alloc);
        }
        var select = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection).empty;
        errdefer {
            freeJoinProjections(self.alloc, select.items);
            select.deinit(self.alloc);
        }
        try self.resolveJoinProjectionsAlloc(raw_select, left_table.alias, lateral_alias, &select);

        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }
        var limit: ?u32 = null;
        var offset: u32 = 0;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseJoinWhere(&left_predicates, &unsupported_right_predicates, left_table.alias, lateral_alias);
                if (unsupported_right_predicates.items.len != 0) return error.UnsupportedSqlShape;
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseJoinOrderBy(&order_by, select.items);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                offset = try self.parseU32Value();
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }

        const left_table_name = try self.alloc.dupe(u8, left_table.name);
        var left_table_name_transferred = false;
        errdefer if (!left_table_name_transferred) self.alloc.free(left_table_name);
        const right_table_name = try self.alloc.dupe(u8, lateral_subquery.table.name);
        var right_table_name_transferred = false;
        errdefer if (!right_table_name_transferred) self.alloc.free(right_table_name);

        const left_owned_predicates = try left_predicates.toOwnedSlice(self.alloc);
        const select_owned = try select.toOwnedSlice(self.alloc);
        const order_owned = try order_by.toOwnedSlice(self.alloc);

        const right_predicates = lateral_subquery.predicates;
        const correlations = lateral_subquery.correlations;
        const right_order_by = lateral_subquery.order_by;
        const right_limit = lateral_subquery.limit;
        lateral_subquery.predicates = &.{};
        lateral_subquery.correlations = &.{};
        lateral_subquery.order_by = &.{};
        freeTableAlias(self.alloc, lateral_subquery.table);
        lateral_subquery.table = .{ .name = "", .alias = "" };

        left_table_name_transferred = true;
        right_table_name_transferred = true;
        return .{
            .left_table_name = left_table_name,
            .right_table_name = right_table_name,
            .plan = .{
                .lateral = .{
                    .left = .{
                        .predicates = left_owned_predicates,
                        .select_all = true,
                    },
                    .right = .{
                        .predicates = right_predicates,
                        .select_all = true,
                        .order_by = right_order_by,
                        .limit = right_limit,
                    },
                    .correlations = correlations,
                    .select = select_owned,
                    .order_by = order_owned,
                    .limit = limit,
                    .offset = offset,
                },
            },
        };
    }

    fn parseInsert(self: *@This()) !LoweredInsert {
        try self.expectKeyword("insert");
        try self.expectKeyword("into");

        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expect(.lparen);
        var columns = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
        }
        while (true) {
            const column = try self.parseIdentifierOwned();
            var column_transferred = false;
            errdefer if (!column_transferred) self.alloc.free(column);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, column, null) == null) return error.InvalidSqlCatalog;
            try columns.append(self.alloc, column);
            column_transferred = true;
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);

        try self.expectKeyword("values");
        try self.expect(.lparen);

        var row_values = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (row_values.items) |value| self.alloc.free(value);
            row_values.deinit(self.alloc);
        }
        for (columns.items, 0..) |column_name, i| {
            const column = relationalColumnForField(self.schema, column_name, null) orelse return error.InvalidSqlCatalog;
            const value_json = try self.parseSqlColumnValueAlloc(column);
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try row_values.append(self.alloc, value_json);
            value_transferred = true;
            if (i + 1 < columns.items.len) {
                try self.expect(.comma);
            }
        }
        try self.expect(.rparen);

        var conflict: ?ConflictClause = null;
        errdefer if (conflict) |value| freeConflictClause(self.alloc, value);
        if (self.matchKeyword("on")) {
            conflict = try self.parseConflictClause(columns.items, row_values.items);
        }

        var returning: ReturningProjection = .{};
        errdefer returning.deinit(self.alloc);
        if (self.matchKeyword("returning")) {
            returning = try self.parseReturningProjectionAlloc();
        }

        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;
        const returning_expression_count = returning.expressions.len;

        const body_json = try self.insertBodyJsonAlloc(columns.items, row_values.items, conflict, returning);
        defer self.alloc.free(body_json);
        var batch = if (conflict != null)
            try relational_rows.parseRowsBatchRequestWithResolver(self.alloc, table_name, body_json, self.schema, self.unique_resolver orelse return error.UnsupportedRowsSelector)
        else
            try relational_rows.parseRowsBatchRequest(self.alloc, body_json, self.schema);
        errdefer batch.deinit(self.alloc);

        for (columns.items) |column| self.alloc.free(column);
        columns.deinit(self.alloc);
        for (row_values.items) |value| self.alloc.free(value);
        row_values.deinit(self.alloc);
        if (conflict) |value| freeConflictClause(self.alloc, value);
        conflict = null;
        returning.deinit(self.alloc);

        return .{
            .table_name = table_name,
            .batch = batch,
            .returning_expression_count = returning_expression_count,
        };
    }

    fn parseUpdate(self: *@This()) !LoweredMutation {
        try self.expectKeyword("update");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expectKeyword("set");
        var patch = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, patch.items);
            patch.deinit(self.alloc);
        }
        var increment = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, increment.items);
            increment.deinit(self.alloc);
        }
        var json_set = std.ArrayListUnmanaged(JsonSetValue).empty;
        errdefer {
            freeJsonSetValues(self.alloc, json_set.items);
            json_set.deinit(self.alloc);
        }
        var array_update = std.ArrayListUnmanaged(ArrayTransformValue).empty;
        errdefer {
            freeArrayTransformValues(self.alloc, array_update.items);
            array_update.deinit(self.alloc);
        }
        while (true) {
            try self.parseUpdateAssignment(&patch, &increment, &json_set, &array_update);
            if (self.match(.comma) == null) break;
        }
        if (patch.items.len == 0 and increment.items.len == 0 and json_set.items.len == 0 and array_update.items.len == 0) return error.UnsupportedSqlShape;

        try self.expectKeyword("where");
        const where_json = try self.parsePrimaryWhereJsonAlloc();
        defer self.alloc.free(where_json);

        var returning: ReturningProjection = .{};
        errdefer returning.deinit(self.alloc);
        if (self.matchKeyword("returning")) {
            returning = try self.parseReturningProjectionAlloc();
        }

        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;
        const returning_expression_count = returning.expressions.len;

        const explicit_expected_version = if (updateWillLookupExistingRow(self.schema, returning))
            null
        else
            try self.expectedVersionForWhereAlloc(table_name, where_json);

        const body_json = try self.updateBodyJsonAlloc(where_json, patch.items, increment.items, json_set.items, array_update.items, returning, explicit_expected_version);
        defer self.alloc.free(body_json);
        var batch = try relational_rows.parseRowsBatchRequestWithResolver(self.alloc, table_name, body_json, self.schema, self.unique_resolver orelse return error.UnsupportedRowsSelector);
        errdefer batch.deinit(self.alloc);

        freeFieldJsonValues(self.alloc, patch.items);
        patch.deinit(self.alloc);
        freeFieldJsonValues(self.alloc, increment.items);
        increment.deinit(self.alloc);
        freeJsonSetValues(self.alloc, json_set.items);
        json_set.deinit(self.alloc);
        freeArrayTransformValues(self.alloc, array_update.items);
        array_update.deinit(self.alloc);
        returning.deinit(self.alloc);

        return .{
            .table_name = table_name,
            .batch = batch,
            .returning_expression_count = returning_expression_count,
        };
    }

    fn parseDelete(self: *@This()) !LoweredMutation {
        try self.expectKeyword("delete");
        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expectKeyword("where");
        const where_json = try self.parsePrimaryWhereJsonAlloc();
        defer self.alloc.free(where_json);

        var returning: ReturningProjection = .{};
        errdefer returning.deinit(self.alloc);
        if (self.matchKeyword("returning")) {
            returning = try self.parseReturningProjectionAlloc();
        }

        if (self.match(.semicolon) != null and !self.atEnd()) return error.UnsupportedSqlShape;
        if (!self.atEnd()) return error.UnsupportedSqlShape;
        const returning_expression_count = returning.expressions.len;

        const explicit_expected_version = if (returning.hasProjection())
            null
        else
            try self.expectedVersionForWhereAlloc(table_name, where_json);

        const body_json = try self.deleteBodyJsonAlloc(where_json, returning, explicit_expected_version);
        defer self.alloc.free(body_json);
        var batch = try relational_rows.parseRowsBatchRequestWithResolver(self.alloc, table_name, body_json, self.schema, self.unique_resolver orelse return error.UnsupportedRowsSelector);
        errdefer batch.deinit(self.alloc);

        returning.deinit(self.alloc);

        return .{
            .table_name = table_name,
            .batch = batch,
            .returning_expression_count = returning_expression_count,
        };
    }

    fn parseUpdateMutationSource(self: *@This()) !LoweredMutationSource {
        try self.expectKeyword("update");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        try self.expectKeyword("set");
        var patch = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, patch.items);
            patch.deinit(self.alloc);
        }
        var increment = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, increment.items);
            increment.deinit(self.alloc);
        }
        var json_set = std.ArrayListUnmanaged(JsonSetValue).empty;
        errdefer {
            freeJsonSetValues(self.alloc, json_set.items);
            json_set.deinit(self.alloc);
        }
        var array_update = std.ArrayListUnmanaged(ArrayTransformValue).empty;
        errdefer {
            freeArrayTransformValues(self.alloc, array_update.items);
            array_update.deinit(self.alloc);
        }
        while (true) {
            try self.parseUpdateAssignment(&patch, &increment, &json_set, &array_update);
            if (self.match(.comma) == null) break;
        }
        if (patch.items.len == 0 and increment.items.len == 0 and json_set.items.len == 0 and array_update.items.len == 0) return error.UnsupportedSqlShape;

        var source = try self.parseMutationSourceQueryTail();
        defer source.deinit(self.alloc);

        const body_json = try self.mutationSourceBodyJsonAlloc("update", source.query, patch.items, increment.items, json_set.items, array_update.items, source.returning);
        defer self.alloc.free(body_json);
        var mutation = try relational_rows.parseRowsMutationSourceRequest(self.alloc, body_json, self.schema);
        errdefer mutation.deinit(self.alloc);

        freeFieldJsonValues(self.alloc, patch.items);
        patch.deinit(self.alloc);
        freeFieldJsonValues(self.alloc, increment.items);
        increment.deinit(self.alloc);
        freeJsonSetValues(self.alloc, json_set.items);
        json_set.deinit(self.alloc);
        freeArrayTransformValues(self.alloc, array_update.items);
        array_update.deinit(self.alloc);

        return .{
            .table_name = table_name,
            .mutation = mutation,
        };
    }

    fn parseDeleteMutationSource(self: *@This()) !LoweredMutationSource {
        try self.expectKeyword("delete");
        try self.expectKeyword("from");
        const table_name = try self.parseIdentifierOwned();
        errdefer self.alloc.free(table_name);

        var source = try self.parseMutationSourceQueryTail();
        defer source.deinit(self.alloc);

        const body_json = try self.mutationSourceBodyJsonAlloc("delete", source.query, &.{}, &.{}, &.{}, &.{}, source.returning);
        defer self.alloc.free(body_json);
        var mutation = try relational_rows.parseRowsMutationSourceRequest(self.alloc, body_json, self.schema);
        errdefer mutation.deinit(self.alloc);

        return .{
            .table_name = table_name,
            .mutation = mutation,
        };
    }

    const ParsedMutationSourceQuery = struct {
        query: db_mod.types.RelationalRowsQueryRequest,
        returning: ReturningProjection = .{},

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.query.deinit(alloc);
            self.returning.deinit(alloc);
        }
    };

    fn parseMutationSourceQueryTail(self: *@This()) !ParsedMutationSourceQuery {
        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var json_path_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate).empty;
        errdefer {
            freeJsonPathEq(self.alloc, json_path_eq.items);
            json_path_eq.deinit(self.alloc);
        }
        var json_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate).empty;
        errdefer {
            freeJsonContains(self.alloc, json_contains.items);
            json_contains.deinit(self.alloc);
        }
        var json_path_exists = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate).empty;
        errdefer {
            freeJsonPathExists(self.alloc, json_path_exists.items);
            json_path_exists.deinit(self.alloc);
        }
        var array_contains = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate).empty;
        errdefer {
            freeArrayContains(self.alloc, array_contains.items);
            array_contains.deinit(self.alloc);
        }
        var array_eq = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate).empty;
        errdefer {
            freeArrayEq(self.alloc, array_eq.items);
            array_eq.deinit(self.alloc);
        }
        var in_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate).empty;
        errdefer {
            freeInPredicates(self.alloc, in_predicates.items);
            in_predicates.deinit(self.alloc);
        }
        var or_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, or_predicates.items);
            or_predicates.deinit(self.alloc);
        }
        var not_predicates = std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup).empty;
        errdefer {
            freePredicateGroups(self.alloc, not_predicates.items);
            not_predicates.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var row_claim = try self.mutationRowClaimAlloc(false);
        errdefer if (row_claim.owner_id.len > 0) self.alloc.free(row_claim.owner_id);

        var saw_where = false;
        var saw_returning = false;
        var returning: ReturningProjection = .{};
        errdefer returning.deinit(self.alloc);
        var limit: ?u32 = null;
        var offset: u32 = 0;

        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                if (saw_where or saw_returning) return error.UnsupportedSqlShape;
                saw_where = true;
                try self.parseWhere(&predicates, &json_contains, &json_path_eq, &json_path_exists, &array_contains, &array_eq, &in_predicates, &or_predicates, &not_predicates);
            } else if (self.matchKeyword("order")) {
                if (saw_returning) return error.UnsupportedSqlShape;
                try self.expectKeyword("by");
                try self.parseOrderBy(&order_by);
            } else if (self.matchKeyword("limit")) {
                if (saw_returning) return error.UnsupportedSqlShape;
                limit = try self.parseU32Value();
            } else if (self.matchKeyword("offset")) {
                if (saw_returning) return error.UnsupportedSqlShape;
                offset = try self.parseU32Value();
            } else if (self.matchKeyword("for")) {
                if (saw_returning) return error.UnsupportedSqlShape;
                try self.expectKeyword("update");
                row_claim.skip_locked = if (self.matchKeyword("skip")) blk: {
                    try self.expectKeyword("locked");
                    break :blk true;
                } else false;
            } else if (self.matchKeyword("returning")) {
                if (saw_returning) return error.UnsupportedSqlShape;
                saw_returning = true;
                returning = try self.parseReturningProjectionAlloc();
            } else if (self.match(.semicolon) != null) {
                if (!self.atEnd()) return error.UnsupportedSqlShape;
            } else if (self.nextIsUnsupportedQueryKeyword()) {
                return error.UnsupportedSqlShape;
            } else {
                return error.UnsupportedSqlShape;
            }
        }
        if (!saw_where) return error.UnsupportedSqlShape;

        const out = ParsedMutationSourceQuery{
            .query = .{
                .predicates = try predicates.toOwnedSlice(self.alloc),
                .array_contains = try array_contains.toOwnedSlice(self.alloc),
                .array_eq = try array_eq.toOwnedSlice(self.alloc),
                .in_predicates = try in_predicates.toOwnedSlice(self.alloc),
                .json_contains = try json_contains.toOwnedSlice(self.alloc),
                .json_path_eq = try json_path_eq.toOwnedSlice(self.alloc),
                .json_path_exists = try json_path_exists.toOwnedSlice(self.alloc),
                .or_predicates = try or_predicates.toOwnedSlice(self.alloc),
                .not_predicates = try not_predicates.toOwnedSlice(self.alloc),
                .select_all = true,
                .order_by = try order_by.toOwnedSlice(self.alloc),
                .row_claim = row_claim,
                .limit = limit,
                .offset = offset,
            },
            .returning = returning,
        };
        predicates = .empty;
        array_contains = .empty;
        array_eq = .empty;
        in_predicates = .empty;
        json_contains = .empty;
        json_path_eq = .empty;
        json_path_exists = .empty;
        or_predicates = .empty;
        not_predicates = .empty;
        order_by = .empty;
        row_claim.owner_id = "";
        returning = .{};
        return out;
    }

    const SelectList = struct {
        fields: []const []const u8 = &.{},
        json_extract: []const db_mod.types.RelationalRowsJsonExtractProjection = &.{},
        array_length: []const db_mod.types.RelationalRowsArrayLengthProjection = &.{},
        coalesce: []const db_mod.types.RelationalRowsCoalesceProjection = &.{},
        field_aliases: []const db_mod.types.RelationalRowsFieldAliasProjection = &.{},
        expressions: []const db_mod.types.RelationalRowsExpressionProjection = &.{},
        select_all: bool = false,
    };

    const WindowSelectList = struct {
        fields: []const []const u8 = &.{},
        windows: []const db_mod.types.RelationalRowsWindowSpec = &.{},
        select_all: bool = false,
    };

    fn parseSelectList(self: *@This()) !SelectList {
        if (self.match(.star) != null) return .{ .select_all = true };

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(field);
            fields.deinit(self.alloc);
        }
        var json_extract = std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonExtractProjection).empty;
        errdefer {
            for (json_extract.items) |projection| {
                self.alloc.free(projection.output);
                self.alloc.free(projection.field);
                self.alloc.free(projection.path);
            }
            json_extract.deinit(self.alloc);
        }
        var array_length = std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayLengthProjection).empty;
        errdefer {
            for (array_length.items) |projection| {
                self.alloc.free(projection.output);
                self.alloc.free(projection.field);
            }
            array_length.deinit(self.alloc);
        }
        var coalesce = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceProjection).empty;
        errdefer {
            for (coalesce.items) |projection| {
                self.alloc.free(projection.output);
                for (projection.operands) |operand| {
                    switch (operand.kind) {
                        .field => if (operand.field.len > 0) self.alloc.free(operand.field),
                        .value => if (operand.value_json.len > 0) self.alloc.free(operand.value_json),
                    }
                }
                if (projection.operands.len > 0) self.alloc.free(projection.operands);
            }
            coalesce.deinit(self.alloc);
        }
        var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
        errdefer {
            freeExpressionProjections(self.alloc, expressions.items);
            expressions.deinit(self.alloc);
        }
        var field_aliases = std.ArrayListUnmanaged(db_mod.types.RelationalRowsFieldAliasProjection).empty;
        errdefer {
            for (field_aliases.items) |projection| {
                self.alloc.free(projection.output);
                self.alloc.free(projection.field);
            }
            field_aliases.deinit(self.alloc);
        }
        while (true) {
            const item = try self.parseSelectItem();
            var item_transferred = false;
            errdefer if (!item_transferred) freeSelectItem(self.alloc, item);
            switch (item) {
                .field => |field| try fields.append(self.alloc, field),
                .json_extract => |projection| try json_extract.append(self.alloc, projection),
                .array_length => |projection| try array_length.append(self.alloc, projection),
                .coalesce => |projection| {
                    const expression_projection = try expressionProjectionFromCoalesceAlloc(self.alloc, projection);
                    var expression_projection_transferred = false;
                    errdefer if (!expression_projection_transferred) freeExpressionProjection(self.alloc, expression_projection);
                    try expressions.append(self.alloc, expression_projection);
                    expression_projection_transferred = true;
                    try coalesce.append(self.alloc, projection);
                },
                .expression => |projection| try expressions.append(self.alloc, projection),
                .field_alias => |projection| try field_aliases.append(self.alloc, projection),
            }
            item_transferred = true;
            if (self.match(.comma) == null) break;
        }
        return .{
            .fields = try fields.toOwnedSlice(self.alloc),
            .json_extract = try json_extract.toOwnedSlice(self.alloc),
            .array_length = try array_length.toOwnedSlice(self.alloc),
            .coalesce = try coalesce.toOwnedSlice(self.alloc),
            .field_aliases = try field_aliases.toOwnedSlice(self.alloc),
            .expressions = try expressions.toOwnedSlice(self.alloc),
            .select_all = false,
        };
    }

    fn parseWindowSelectList(self: *@This()) !WindowSelectList {
        if (self.match(.star) != null) return error.UnsupportedSqlShape;

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(field);
            fields.deinit(self.alloc);
        }
        var windows = std.ArrayListUnmanaged(db_mod.types.RelationalRowsWindowSpec).empty;
        errdefer {
            freeWindowSpecs(self.alloc, windows.items);
            windows.deinit(self.alloc);
        }

        while (true) {
            if (self.peekKeyword("row_number")) {
                const spec = try self.parseRowNumberWindowSpecAlloc();
                var spec_transferred = false;
                errdefer if (!spec_transferred) freeWindowSpec(self.alloc, spec);
                try windows.append(self.alloc, spec);
                spec_transferred = true;
            } else {
                const field = try self.parseFieldExpressionOwned();
                var field_transferred = false;
                errdefer if (!field_transferred) self.alloc.free(field);
                if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
                if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
                try self.consumeCompatibleProjectionAlias(field);
                try fields.append(self.alloc, field);
                field_transferred = true;
            }
            if (self.match(.comma) == null) break;
        }

        return .{
            .fields = try fields.toOwnedSlice(self.alloc),
            .windows = try windows.toOwnedSlice(self.alloc),
            .select_all = false,
        };
    }

    fn parseRowNumberWindowSpecAlloc(self: *@This()) !db_mod.types.RelationalRowsWindowSpec {
        try self.expectKeyword("row_number");
        try self.expect(.lparen);
        try self.expect(.rparen);
        try self.expectKeyword("over");
        try self.expect(.lparen);

        var partition_by = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (partition_by.items) |field| self.alloc.free(field);
            partition_by.deinit(self.alloc);
        }
        if (self.matchKeyword("partition")) {
            try self.expectKeyword("by");
            try self.parseWindowPartitionBy(&partition_by);
        }
        try self.expectKeyword("order");
        try self.expectKeyword("by");
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }
        try self.parseOrderBy(&order_by);
        if (order_by.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);

        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "row_number");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);

        const owned_partition_by = try partition_by.toOwnedSlice(self.alloc);
        var partition_transferred = false;
        errdefer if (!partition_transferred) freeStringSlice(self.alloc, owned_partition_by);
        const owned_order_by = try order_by.toOwnedSlice(self.alloc);
        var order_transferred = false;
        errdefer if (!order_transferred) {
            freeOrderBy(self.alloc, owned_order_by);
            if (owned_order_by.len > 0) self.alloc.free(owned_order_by);
        };

        output_transferred = true;
        partition_transferred = true;
        order_transferred = true;
        return .{
            .output = output,
            .function = .row_number,
            .partition_by = owned_partition_by,
            .order_by = owned_order_by,
        };
    }

    fn parseWindowPartitionBy(self: *@This(), partition_by: *std.ArrayListUnmanaged([]const u8)) !void {
        while (true) {
            const field = try self.parseFieldExpressionOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try partition_by.append(self.alloc, field);
            field_transferred = true;
            if (self.match(.comma) == null) break;
            if (self.peekKeyword("order")) return error.UnsupportedSqlShape;
        }
    }

    const AggregateSelectList = struct {
        group_fields: []const []const u8 = &.{},
        aggregations: []const db_mod.types.RelationalRowsAggregateSpec = &.{},
    };

    fn parseAggregateSelectList(self: *@This()) !AggregateSelectList {
        var group_fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (group_fields.items) |field| self.alloc.free(field);
            group_fields.deinit(self.alloc);
        }
        var aggregations = std.ArrayListUnmanaged(db_mod.types.RelationalRowsAggregateSpec).empty;
        errdefer {
            freeAggregateSpecs(self.alloc, aggregations.items);
            aggregations.deinit(self.alloc);
        }
        while (true) {
            if (self.nextIsAggregateFunction()) {
                const spec = try self.parseAggregateSpecAlloc();
                var spec_transferred = false;
                errdefer if (!spec_transferred) freeAggregateSpec(self.alloc, spec);
                try aggregations.append(self.alloc, spec);
                spec_transferred = true;
            } else {
                const field = try self.parseFieldExpressionOwned();
                var field_transferred = false;
                errdefer if (!field_transferred) self.alloc.free(field);
                if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
                if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
                try self.consumeCompatibleProjectionAlias(field);
                try group_fields.append(self.alloc, field);
                field_transferred = true;
            }
            if (self.match(.comma) == null) break;
        }
        return .{
            .group_fields = try group_fields.toOwnedSlice(self.alloc),
            .aggregations = try aggregations.toOwnedSlice(self.alloc),
        };
    }

    fn nextIsAggregateFunction(self: *@This()) bool {
        if (self.pos + 1 >= self.tokens.len) return false;
        if (self.tokens[self.pos].kind != .identifier or self.tokens[self.pos + 1].kind != .lparen) return false;
        const name = self.tokens[self.pos].text;
        return std.ascii.eqlIgnoreCase(name, "count") or
            std.ascii.eqlIgnoreCase(name, "sum") or
            std.ascii.eqlIgnoreCase(name, "min") or
            std.ascii.eqlIgnoreCase(name, "max") or
            std.ascii.eqlIgnoreCase(name, "avg") or
            std.ascii.eqlIgnoreCase(name, "array_agg");
    }

    fn parseAggregateSpecAlloc(self: *@This()) !db_mod.types.RelationalRowsAggregateSpec {
        const function_name = self.match(.identifier) orelse return error.UnsupportedSqlShape;
        const op = aggregateOpForName(function_name.text) orelse return error.UnsupportedSqlShape;
        try self.expect(.lparen);
        const distinct = self.matchKeyword("distinct");
        var field: ?[]const u8 = null;
        var field_transferred = false;
        errdefer if (!field_transferred) if (field) |owned| self.alloc.free(owned);
        var expression: ?db_mod.types.RelationalRowsExpression = null;
        var expression_transferred = false;
        errdefer if (!expression_transferred) if (expression) |owned| freeExpression(self.alloc, owned);
        var array_order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, array_order_by.items);
            array_order_by.deinit(self.alloc);
        }
        if (op == .count and self.match(.star) != null) {
            if (distinct) return error.UnsupportedSqlShape;
            field = null;
        } else {
            if (self.peekAggregateExpressionInput()) {
                const parsed_expression = try self.parseAggregateInputExpressionAlloc();
                var parsed_expression_transferred = false;
                errdefer if (!parsed_expression_transferred) freeExpression(self.alloc, parsed_expression);
                try self.validateAggregateInputExpression(op, parsed_expression);
                expression = parsed_expression;
                parsed_expression_transferred = true;
            } else {
                const parsed_field = try self.parseFieldExpressionOwned();
                var parsed_field_transferred = false;
                errdefer if (!parsed_field_transferred) self.alloc.free(parsed_field);
                if (relationalColumnForField(self.schema, parsed_field, null)) |column| {
                    if (op == .count) {
                        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
                    } else if (op == .array_agg) {
                        if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
                    } else if (column.field_type != .numeric) {
                        return error.InvalidSqlCatalog;
                    }
                } else {
                    return error.InvalidSqlCatalog;
                }
                field = parsed_field;
                parsed_field_transferred = true;
            }
        }
        if (op == .array_agg and self.matchKeyword("order")) {
            try self.expectKeyword("by");
            try self.parseOrderBy(&array_order_by);
        }
        try self.expect(.rparen);
        const filter = try self.parseAggregateFilterAlloc();
        var filter_transferred = false;
        errdefer if (!filter_transferred) {
            freeRelationalChecks(self.alloc, filter.predicates);
            if (filter.predicates.len > 0) self.alloc.free(filter.predicates);
            freeExpressionConditions(self.alloc, filter.expressions);
            if (filter.expressions.len > 0) self.alloc.free(filter.expressions);
        };
        const name = try self.parseAggregateAliasOrDefaultAlloc(op, field);
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);
        field_transferred = true;
        expression_transferred = true;
        filter_transferred = true;
        name_transferred = true;
        return .{
            .name = name,
            .op = op,
            .field = field,
            .expression = expression,
            .distinct = distinct,
            .distinct_max_items = if (distinct) db_mod.types.default_relational_rows_aggregate_distinct_max_items else 0,
            .array_max_items = if (op == .array_agg) default_array_agg_max_items else 0,
            .array_order_by = try array_order_by.toOwnedSlice(self.alloc),
            .filter_predicates = filter.predicates,
            .filter_expressions = filter.expressions,
        };
    }

    fn peekAggregateExpressionInput(self: *@This()) bool {
        if (self.peekKeyword("lower") or self.peekKeyword("case") or self.peekKeyword("cast") or self.peekKeyword("nullif")) return true;
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len) {
            return switch (self.tokens[self.pos + 1].kind) {
                .plus, .minus, .star, .slash => true,
                else => false,
            };
        }
        return false;
    }

    fn parseAggregateInputExpressionAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpression {
        if (self.peekKeyword("lower") or self.peekKeyword("case") or self.peekKeyword("cast") or self.peekKeyword("nullif")) {
            return try self.parseRowExpressionOperandAlloc();
        }

        const field = try self.parseFieldExpressionOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (column.field_type != .numeric) return error.InvalidSqlCatalog;
        if (self.peekArithmeticOperator() == null) return error.UnsupportedSqlShape;
        field_transferred = true;
        return try self.parseArithmeticExpressionRestAlloc(.{ .kind = .field, .field = field }, 0);
    }

    fn validateAggregateInputExpression(
        self: *@This(),
        op: db_mod.types.RelationalRowsAggregateOp,
        expression: db_mod.types.RelationalRowsExpression,
    ) !void {
        switch (op) {
            .count, .array_agg => {},
            .sum, .avg, .min, .max => try self.validateNumericRowExpression(expression),
        }
    }

    const AggregateFilter = struct {
        predicates: []const runtime_schema.RelationalCheck = &.{},
        expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    };

    fn parseAggregateFilterAlloc(self: *@This()) !AggregateFilter {
        if (!self.matchKeyword("filter")) return .{};
        try self.expect(.lparen);
        try self.expectKeyword("where");

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCondition).empty;
        errdefer {
            freeExpressionConditions(self.alloc, expressions.items);
            expressions.deinit(self.alloc);
        }

        while (true) {
            if (self.peekAggregateExpressionFilter()) {
                const condition = try self.parseCaseExpressionConditionAlloc();
                var condition_transferred = false;
                errdefer if (!condition_transferred) freeExpressionCondition(self.alloc, condition);
                try expressions.append(self.alloc, condition);
                condition_transferred = true;
            } else {
                const predicate = try self.parseScalarWherePredicateAlloc();
                var predicate_transferred = false;
                errdefer if (!predicate_transferred) freeRelationalCheck(self.alloc, predicate);
                try predicates.append(self.alloc, predicate);
                predicate_transferred = true;
            }
            if (!self.matchKeyword("and")) break;
        }
        if (predicates.items.len == 0 and expressions.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        return .{
            .predicates = try predicates.toOwnedSlice(self.alloc),
            .expressions = try expressions.toOwnedSlice(self.alloc),
        };
    }

    fn peekAggregateExpressionFilter(self: *@This()) bool {
        if (self.peekKeyword("lower") or self.peekKeyword("case") or self.peekKeyword("cast") or self.peekKeyword("nullif")) return true;
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len) {
            return switch (self.tokens[self.pos + 1].kind) {
                .plus, .minus, .star, .slash => true,
                else => false,
            };
        }
        return false;
    }

    fn parseAggregateAliasOrDefaultAlloc(
        self: *@This(),
        op: db_mod.types.RelationalRowsAggregateOp,
        field: ?[]const u8,
    ) ![]const u8 {
        if (self.matchKeyword("as")) return try self.parseIdentifierOwned();
        if (field) |field_name| return try std.fmt.allocPrint(self.alloc, "{s}_{s}", .{ aggregateOpName(op), field_name });
        return try self.alloc.dupe(u8, aggregateOpName(op));
    }

    const TableAlias = struct {
        name: []const u8,
        alias: []const u8,
    };

    const LateralSubquery = struct {
        table: TableAlias,
        predicates: []const runtime_schema.RelationalCheck = &.{},
        correlations: []const db_mod.types.RelationalRowsLateralCorrelation = &.{},
        order_by: []const db_mod.types.RelationalRowsQueryOrder = &.{},
        limit: ?u32 = null,
    };

    const QualifiedField = struct {
        qualifier: []const u8,
        field: []const u8,
    };

    const QualifiedProjection = struct {
        source: QualifiedField,
        output: []const u8,
    };

    fn parseTableAliasAlloc(self: *@This()) !TableAlias {
        const name = try self.parseIdentifierOwned();
        var name_transferred = false;
        errdefer if (!name_transferred) self.alloc.free(name);
        const alias = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else if (self.peekKind(.identifier) and !self.nextIsJoinClauseKeyword())
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, name);
        var alias_transferred = false;
        errdefer if (!alias_transferred) self.alloc.free(alias);
        name_transferred = true;
        alias_transferred = true;
        return .{ .name = name, .alias = alias };
    }

    fn parseLateralSubqueryAlloc(self: *@This(), left_alias: []const u8) !LateralSubquery {
        try self.expectKeyword("select");
        const select = try self.parseSelectList();
        defer {
            freeStringSlice(self.alloc, select.fields);
            freeJsonExtract(self.alloc, select.json_extract);
            freeArrayLengthProjections(self.alloc, select.array_length);
            freeCoalesceProjections(self.alloc, select.coalesce);
            freeFieldAliasProjections(self.alloc, select.field_aliases);
        }
        if (select.select_all) return error.UnsupportedSqlShape;

        try self.expectKeyword("from");
        const right_table = try self.parseTableAliasAlloc();
        errdefer freeTableAlias(self.alloc, right_table);

        var predicates = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        var correlations = std.ArrayListUnmanaged(db_mod.types.RelationalRowsLateralCorrelation).empty;
        errdefer {
            freeLateralCorrelations(self.alloc, correlations.items);
            correlations.deinit(self.alloc);
        }
        var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
        errdefer {
            freeOrderBy(self.alloc, order_by.items);
            order_by.deinit(self.alloc);
        }

        var limit: ?u32 = null;
        while (!self.atEnd()) {
            if (self.matchKeyword("where")) {
                try self.parseLateralWhere(left_alias, right_table.alias, &predicates, &correlations);
            } else if (self.matchKeyword("order")) {
                try self.expectKeyword("by");
                try self.parseOrderBy(&order_by);
            } else if (self.matchKeyword("limit")) {
                limit = try self.parseU32Value();
            } else {
                return error.UnsupportedSqlShape;
            }
        }
        if (limit == null or correlations.items.len == 0) return error.UnsupportedSqlShape;

        return .{
            .table = right_table,
            .predicates = try predicates.toOwnedSlice(self.alloc),
            .correlations = try correlations.toOwnedSlice(self.alloc),
            .order_by = try order_by.toOwnedSlice(self.alloc),
            .limit = limit,
        };
    }

    fn nextIsJoinClauseKeyword(self: *@This()) bool {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].kind != .identifier) return false;
        const token = self.tokens[self.pos].text;
        return std.ascii.eqlIgnoreCase(token, "left") or
            std.ascii.eqlIgnoreCase(token, "outer") or
            std.ascii.eqlIgnoreCase(token, "inner") or
            std.ascii.eqlIgnoreCase(token, "join") or
            std.ascii.eqlIgnoreCase(token, "on") or
            std.ascii.eqlIgnoreCase(token, "where") or
            std.ascii.eqlIgnoreCase(token, "order") or
            std.ascii.eqlIgnoreCase(token, "limit") or
            std.ascii.eqlIgnoreCase(token, "offset") or
            std.ascii.eqlIgnoreCase(token, "group");
    }

    fn parseQualifiedFieldAlloc(self: *@This()) !QualifiedField {
        const identifier = try self.parseIdentifierOwned();
        defer self.alloc.free(identifier);
        const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return error.UnsupportedSqlShape;
        if (dot == 0 or dot + 1 >= identifier.len) return error.UnsupportedSqlShape;
        const qualifier = try self.alloc.dupe(u8, identifier[0..dot]);
        var qualifier_transferred = false;
        errdefer if (!qualifier_transferred) self.alloc.free(qualifier);
        const field = try self.alloc.dupe(u8, identifier[dot + 1 ..]);
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        if (std.mem.indexOfScalar(u8, field, '.') != null) return error.UnsupportedSqlShape;
        qualifier_transferred = true;
        field_transferred = true;
        return .{ .qualifier = qualifier, .field = field };
    }

    fn parseJoinProjectionListAlloc(self: *@This()) ![]const QualifiedProjection {
        var projections = std.ArrayListUnmanaged(QualifiedProjection).empty;
        errdefer {
            freeQualifiedProjections(self.alloc, projections.items);
            projections.deinit(self.alloc);
        }
        while (true) {
            const source = try self.parseQualifiedFieldAlloc();
            var source_transferred = false;
            errdefer if (!source_transferred) freeQualifiedField(self.alloc, source);
            const output = if (self.matchKeyword("as"))
                try self.parseIdentifierOwned()
            else
                try self.alloc.dupe(u8, source.field);
            var output_transferred = false;
            errdefer if (!output_transferred) self.alloc.free(output);
            try projections.append(self.alloc, .{ .source = source, .output = output });
            source_transferred = true;
            output_transferred = true;
            if (self.match(.comma) == null) break;
        }
        return try projections.toOwnedSlice(self.alloc);
    }

    fn resolveJoinProjectionsAlloc(
        self: *@This(),
        raw_select: []const QualifiedProjection,
        left_alias: []const u8,
        right_alias: []const u8,
        select: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinProjection),
    ) !void {
        for (raw_select) |projection| {
            const side = try joinSideForQualifier(projection.source.qualifier, left_alias, right_alias);
            if (relationalColumnForField(self.schema, projection.source.field, null) == null) return error.InvalidSqlCatalog;
            const output = try self.alloc.dupe(u8, projection.output);
            var output_transferred = false;
            errdefer if (!output_transferred) self.alloc.free(output);
            const field = try self.alloc.dupe(u8, projection.source.field);
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            try select.append(self.alloc, .{ .output = output, .side = side, .field = field });
            output_transferred = true;
            field_transferred = true;
        }
    }

    fn parseJoinOn(
        self: *@This(),
        on: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJoinOn),
        left_alias: []const u8,
        right_alias: []const u8,
    ) !void {
        while (true) {
            const lhs = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, lhs);
            try self.expect(.eq);
            const rhs = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, rhs);
            const lhs_side = try joinSideForQualifier(lhs.qualifier, left_alias, right_alias);
            const rhs_side = try joinSideForQualifier(rhs.qualifier, left_alias, right_alias);
            if (lhs_side == rhs_side) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, lhs.field, null) == null or relationalColumnForField(self.schema, rhs.field, null) == null) return error.InvalidSqlCatalog;
            const left_field_source = if (lhs_side == .left) lhs.field else rhs.field;
            const right_field_source = if (lhs_side == .right) lhs.field else rhs.field;
            const left_field = try self.alloc.dupe(u8, left_field_source);
            var left_transferred = false;
            errdefer if (!left_transferred) self.alloc.free(left_field);
            const right_field = try self.alloc.dupe(u8, right_field_source);
            var right_transferred = false;
            errdefer if (!right_transferred) self.alloc.free(right_field);
            try on.append(self.alloc, .{ .left_field = left_field, .right_field = right_field });
            left_transferred = true;
            right_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseJoinWhere(
        self: *@This(),
        left_predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        right_predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        left_alias: []const u8,
        right_alias: []const u8,
    ) !void {
        while (true) {
            const source = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, source);
            const side = try joinSideForQualifier(source.qualifier, left_alias, right_alias);
            const column = relationalColumnForField(self.schema, source.field, null) orelse return error.InvalidSqlCatalog;
            const target = if (side == .left) left_predicates else right_predicates;
            const op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("is")) blk: {
                if (self.matchKeyword("not")) {
                    try self.expectKeyword("null");
                    break :blk .is_not_null;
                }
                try self.expectKeyword("null");
                break :blk .is_null;
            } else try self.parseComparisonOp();
            const value_json = if (op == .is_null or op == .is_not_null)
                null
            else
                try self.parseSqlColumnValueAlloc(column);
            var value_transferred = false;
            errdefer if (!value_transferred) if (value_json) |json| self.alloc.free(json);
            const field = try self.alloc.dupe(u8, source.field);
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            try target.append(self.alloc, .{ .name = "", .field = field, .op = op, .value_json = value_json });
            field_transferred = true;
            value_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseLateralWhere(
        self: *@This(),
        left_alias: []const u8,
        right_alias: []const u8,
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        correlations: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsLateralCorrelation),
    ) !void {
        while (true) {
            const lhs = try self.parseQualifiedFieldAlloc();
            defer freeQualifiedField(self.alloc, lhs);
            const lhs_side = try joinSideForQualifier(lhs.qualifier, left_alias, right_alias);
            const column = relationalColumnForField(self.schema, lhs.field, null) orelse return error.InvalidSqlCatalog;
            const op = try self.parseComparisonOp();
            if (self.peekKind(.identifier) and identifierHasQualifier(self.tokens[self.pos].text)) {
                if (op != .eq) return error.UnsupportedSqlShape;
                const rhs = try self.parseQualifiedFieldAlloc();
                defer freeQualifiedField(self.alloc, rhs);
                const rhs_side = try joinSideForQualifier(rhs.qualifier, left_alias, right_alias);
                if (lhs_side == rhs_side) return error.UnsupportedSqlShape;
                if (relationalColumnForField(self.schema, rhs.field, null) == null) return error.InvalidSqlCatalog;
                const left_source = if (lhs_side == .left) lhs.field else rhs.field;
                const right_source = if (lhs_side == .right) lhs.field else rhs.field;
                const left_field = try self.alloc.dupe(u8, left_source);
                var left_transferred = false;
                errdefer if (!left_transferred) self.alloc.free(left_field);
                const right_field = try self.alloc.dupe(u8, right_source);
                var right_transferred = false;
                errdefer if (!right_transferred) self.alloc.free(right_field);
                try correlations.append(self.alloc, .{ .left_field = left_field, .right_field = right_field });
                left_transferred = true;
                right_transferred = true;
            } else {
                if (lhs_side != .right) return error.UnsupportedSqlShape;
                const value_json = try self.parseSqlColumnValueAlloc(column);
                var value_transferred = false;
                errdefer if (!value_transferred) self.alloc.free(value_json);
                const field = try self.alloc.dupe(u8, lhs.field);
                var field_transferred = false;
                errdefer if (!field_transferred) self.alloc.free(field);
                try predicates.append(self.alloc, .{ .name = "", .field = field, .op = op, .value_json = value_json });
                field_transferred = true;
                value_transferred = true;
            }
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseJoinOrderBy(
        self: *@This(),
        order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
        select: []const db_mod.types.RelationalRowsJoinProjection,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!joinProjectionContainsOutput(select, field)) return error.UnsupportedSqlShape;
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            try order_by.append(self.alloc, .{ .field = field, .direction = direction });
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    const SelectItem = union(enum) {
        field: []const u8,
        json_extract: db_mod.types.RelationalRowsJsonExtractProjection,
        array_length: db_mod.types.RelationalRowsArrayLengthProjection,
        coalesce: db_mod.types.RelationalRowsCoalesceProjection,
        expression: db_mod.types.RelationalRowsExpressionProjection,
        field_alias: db_mod.types.RelationalRowsFieldAliasProjection,
    };

    const FieldJsonValue = struct {
        field: []const u8,
        value_json: []const u8,
    };

    const FieldPredicate = struct {
        field: []const u8,
        op: runtime_schema.UniquePredicateOp,
        value_json: ?[]const u8 = null,
    };

    const JsonSetValue = struct {
        field: []const u8,
        path: []const []const u8,
        value_json: []const u8,
    };

    const ArrayTransformValue = struct {
        field: []const u8,
        op: db_mod.types.TransformOpType,
        value_json: []const u8,
    };

    const ConflictAction = enum {
        nothing,
        update,
    };

    const ConflictTarget = union(enum) {
        primary,
        unique: UniqueConflictTarget,
    };

    const UniqueConflictTarget = struct {
        name: []const u8,
        where_json: []const u8 = "",
    };

    const ConflictClause = struct {
        target: ConflictTarget,
        action: ConflictAction,
        patch: []const FieldJsonValue = &.{},
        increment: []const FieldJsonValue = &.{},
        json_set: []const JsonSetValue = &.{},
        array_update: []const ArrayTransformValue = &.{},
    };

    fn parseConflictClause(self: *@This(), insert_columns: []const []const u8, insert_values: []const []const u8) !ConflictClause {
        try self.expectKeyword("conflict");
        const target = try self.parseConflictTarget();
        errdefer freeConflictTarget(self.alloc, target);
        try self.expectKeyword("do");

        if (self.matchKeyword("nothing")) {
            return .{
                .target = target,
                .action = .nothing,
            };
        }

        try self.expectKeyword("update");
        try self.expectKeyword("set");
        var patch = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, patch.items);
            patch.deinit(self.alloc);
        }
        var increment = std.ArrayListUnmanaged(FieldJsonValue).empty;
        errdefer {
            freeFieldJsonValues(self.alloc, increment.items);
            increment.deinit(self.alloc);
        }
        var json_set = std.ArrayListUnmanaged(JsonSetValue).empty;
        errdefer {
            freeJsonSetValues(self.alloc, json_set.items);
            json_set.deinit(self.alloc);
        }
        var array_update = std.ArrayListUnmanaged(ArrayTransformValue).empty;
        errdefer {
            freeArrayTransformValues(self.alloc, array_update.items);
            array_update.deinit(self.alloc);
        }
        while (true) {
            try self.parseConflictUpdateAssignment(insert_columns, insert_values, &patch, &increment, &json_set, &array_update);
            if (self.match(.comma) == null) break;
        }
        if (patch.items.len == 0 and increment.items.len == 0 and json_set.items.len == 0 and array_update.items.len == 0) return error.UnsupportedSqlShape;

        return .{
            .target = target,
            .action = .update,
            .patch = try patch.toOwnedSlice(self.alloc),
            .increment = try increment.toOwnedSlice(self.alloc),
            .json_set = try json_set.toOwnedSlice(self.alloc),
            .array_update = try array_update.toOwnedSlice(self.alloc),
        };
    }

    fn parseConflictTarget(self: *@This()) !ConflictTarget {
        try self.expect(.lparen);
        if (self.matchKeyword("lower")) {
            try self.expect(.lparen);
            const field = try self.parseIdentifierOwned();
            defer self.alloc.free(field);
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try self.expect(.rparen);
            try self.expect(.rparen);
            const constraint = findUniqueConstraintByLowerExpression(self.schema, field) orelse return error.InvalidSqlCatalog;
            if (constraint.where.len != 0) return error.UnsupportedSqlShape;
            const name = try self.alloc.dupe(u8, constraint.name);
            errdefer self.alloc.free(name);
            return .{ .unique = .{ .name = name } };
        }

        var columns = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
        }
        while (true) {
            const column = try self.parseIdentifierOwned();
            var column_transferred = false;
            errdefer if (!column_transferred) self.alloc.free(column);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, column, null) == null) return error.InvalidSqlCatalog;
            try columns.append(self.alloc, column);
            column_transferred = true;
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);

        var where_json: []const u8 = "";
        errdefer if (where_json.len > 0) self.alloc.free(where_json);
        if (self.matchKeyword("where")) {
            where_json = try self.parseUniquePredicateWhereJsonAlloc();
        }

        if (columnsMatchPrimaryKey(self.schema.primary_key.?, columns.items)) {
            if (where_json.len > 0) return error.UnsupportedSqlShape;
            for (columns.items) |column| self.alloc.free(column);
            columns.deinit(self.alloc);
            return .primary;
        }

        const constraint = findUniqueConstraintByColumns(self.schema, columns.items, where_json.len > 0) orelse return error.InvalidSqlCatalog;
        if (constraint.expressions.len != 0) return error.UnsupportedSqlShape;
        if (where_json.len == 0 and constraint.where.len != 0) return error.UnsupportedSqlShape;
        if (where_json.len > 0 and constraint.where.len == 0) return error.UnsupportedSqlShape;
        if (where_json.len > 0) try validateUniqueWhereJsonMatches(self.alloc, where_json, constraint.where);

        const name = try self.alloc.dupe(u8, constraint.name);
        errdefer self.alloc.free(name);
        for (columns.items) |column| self.alloc.free(column);
        columns.deinit(self.alloc);
        const out_where = where_json;
        where_json = "";
        return .{ .unique = .{
            .name = name,
            .where_json = out_where,
        } };
    }

    fn parseConflictUpdateAssignment(
        self: *@This(),
        insert_columns: []const []const u8,
        insert_values: []const []const u8,
        patch: *std.ArrayListUnmanaged(FieldJsonValue),
        increment: *std.ArrayListUnmanaged(FieldJsonValue),
        json_set: *std.ArrayListUnmanaged(JsonSetValue),
        array_update: *std.ArrayListUnmanaged(ArrayTransformValue),
    ) !void {
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        defer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (primaryKeyContains(self.schema.primary_key.?, field)) return error.UnsupportedSqlShape;
        try self.expect(.eq);

        if (self.matchKeyword("jsonb_set")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const json_field = try self.parseIdentifierOwned();
            defer self.alloc.free(json_field);
            if (!std.mem.eql(u8, json_field, field)) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const path = try self.parsePostgresJsonPathAlloc();
            var path_transferred = false;
            errdefer if (!path_transferred) freeStringSlice(self.alloc, path);
            try self.expect(.comma);
            const value_json = try self.parseJsonValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (self.match(.comma) != null) {
                if (!self.matchKeyword("true") and !self.matchKeyword("false")) return error.UnsupportedSqlShape;
            }
            try self.expect(.rparen);
            try json_set.append(self.alloc, .{
                .field = field,
                .path = path,
                .value_json = value_json,
            });
            field_transferred = true;
            path_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.matchKeyword("array_append") or self.matchKeyword("array_remove")) {
            const op: db_mod.types.TransformOpType = if (std.ascii.eqlIgnoreCase(self.tokens[self.pos - 1].text, "array_append")) .push else .pull;
            if (column.field_type != .array) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const array_field = try self.parseIdentifierOwned();
            defer self.alloc.free(array_field);
            if (!std.mem.eql(u8, array_field, field)) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const value_json = try self.parseConflictValueJsonAlloc(column, insert_columns, insert_values);
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try self.expect(.rparen);
            try array_update.append(self.alloc, .{
                .field = field,
                .op = op,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .pipe_concat) {
            const json_field_token = self.match(.identifier).?;
            if (!std.mem.eql(u8, json_field_token.text, field)) return error.UnsupportedSqlShape;
            try self.expect(.pipe_concat);
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.appendJsonObjectConcatSetValues(field, json_set);
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].kind == .plus or self.tokens[self.pos + 1].kind == .minus)) {
            try self.parseIncrementAssignment(field, column, increment);
            return;
        }

        const value_json = try self.parseConflictValueJsonAlloc(column, insert_columns, insert_values);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        try patch.append(self.alloc, .{
            .field = field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn parseConflictValueJsonAlloc(
        self: *@This(),
        column: runtime_schema.RelationalColumn,
        insert_columns: []const []const u8,
        insert_values: []const []const u8,
    ) ![]const u8 {
        if (self.peekKind(.identifier) and self.pos < self.tokens.len) {
            const token = self.tokens[self.pos];
            if (std.mem.startsWith(u8, token.text, "excluded.")) {
                self.pos += 1;
                const source = token.text["excluded.".len..];
                const source_column = relationalColumnForField(self.schema, source, null) orelse return error.InvalidSqlCatalog;
                if (source_column.field_type != column.field_type) return error.UnsupportedSqlShape;
                for (insert_columns, insert_values) |insert_column, insert_value| {
                    if (std.mem.eql(u8, insert_column, source)) return try self.alloc.dupe(u8, insert_value);
                }
                return error.UnsupportedSqlShape;
            }
        }
        return try self.parseSqlColumnValueAlloc(column);
    }

    fn parseIncrementAssignment(
        self: *@This(),
        field: []const u8,
        column: runtime_schema.RelationalColumn,
        increment: *std.ArrayListUnmanaged(FieldJsonValue),
    ) !void {
        if (column.field_type != .numeric) return error.InvalidSqlCatalog;
        const source = try self.parseIdentifierOwned();
        defer self.alloc.free(source);
        if (!std.mem.eql(u8, source, field)) return error.UnsupportedSqlShape;
        const negated = if (self.match(.plus) != null)
            false
        else if (self.match(.minus) != null)
            true
        else
            return error.UnsupportedSqlShape;
        const raw_value_json = try self.parseSqlColumnValueAlloc(column);
        defer self.alloc.free(raw_value_json);
        const value_json = try self.normalizedIncrementJsonAlloc(raw_value_json, negated);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        const owned_field = try self.alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(owned_field);
        try increment.append(self.alloc, .{
            .field = owned_field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn normalizedIncrementJsonAlloc(self: *@This(), value_json: []const u8, negated: bool) ![]const u8 {
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, value_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (!negated) {
            switch (parsed.value) {
                .integer, .float, .number_string => return try self.alloc.dupe(u8, value_json),
                else => return error.UnsupportedSqlShape,
            }
        }
        return switch (parsed.value) {
            .integer => |value| if (value == std.math.minInt(i64))
                error.UnsupportedSqlShape
            else
                try std.fmt.allocPrint(self.alloc, "{d}", .{-value}),
            .float => |value| try std.fmt.allocPrint(self.alloc, "{d}", .{-value}),
            .number_string => |text| blk: {
                const value = std.fmt.parseFloat(f64, text) catch return error.UnsupportedSqlShape;
                break :blk try std.fmt.allocPrint(self.alloc, "{d}", .{-value});
            },
            else => error.UnsupportedSqlShape,
        };
    }

    fn parseUniquePredicateWhereJsonAlloc(self: *@This()) ![]const u8 {
        var predicates = std.ArrayListUnmanaged(FieldPredicate).empty;
        defer {
            freeFieldPredicates(self.alloc, predicates.items);
            predicates.deinit(self.alloc);
        }
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            _ = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
            if (self.matchKeyword("is")) {
                const op: runtime_schema.UniquePredicateOp = if (self.matchKeyword("not")) blk: {
                    try self.expectKeyword("null");
                    break :blk .is_not_null;
                } else blk: {
                    try self.expectKeyword("null");
                    break :blk .is_null;
                };
                try predicates.append(self.alloc, .{ .field = field, .op = op });
                field_transferred = true;
            } else {
                const op: runtime_schema.UniquePredicateOp = if (self.match(.eq) != null) .eq else if (self.match(.neq) != null) .ne else return error.UnsupportedSqlShape;
                const value_json = try self.parseJsonValueAlloc();
                var value_transferred = false;
                errdefer if (!value_transferred) self.alloc.free(value_json);
                try predicates.append(self.alloc, .{ .field = field, .op = op, .value_json = value_json });
                field_transferred = true;
                value_transferred = true;
            }
            if (!self.matchKeyword("and")) break;
        }

        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"all\":[");
        for (predicates.items, 0..) |predicate, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"field\":{f},\"op\":{f}", .{ std.json.fmt(predicate.field, .{}), std.json.fmt(uniquePredicateOpToken(predicate.op), .{}) });
            if (predicate.value_json) |value_json| {
                try writer.writeAll(",\"value\":");
                try writer.writeAll(value_json);
            }
            try writer.writeByte('}');
        }
        try writer.writeAll("]}");
        return try out.toOwnedSlice();
    }

    fn parseUpdateAssignment(
        self: *@This(),
        patch: *std.ArrayListUnmanaged(FieldJsonValue),
        increment: *std.ArrayListUnmanaged(FieldJsonValue),
        json_set: *std.ArrayListUnmanaged(JsonSetValue),
        array_update: *std.ArrayListUnmanaged(ArrayTransformValue),
    ) !void {
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        defer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (primaryKeyContains(self.schema.primary_key.?, field)) return error.UnsupportedSqlShape;
        try self.expect(.eq);

        if (self.matchKeyword("jsonb_set")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const json_field = try self.parseIdentifierOwned();
            defer self.alloc.free(json_field);
            if (!std.mem.eql(u8, json_field, field)) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const path = try self.parsePostgresJsonPathAlloc();
            var path_transferred = false;
            errdefer if (!path_transferred) freeStringSlice(self.alloc, path);
            try self.expect(.comma);
            const value_json = try self.parseJsonValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (self.match(.comma) != null) {
                if (!self.matchKeyword("true") and !self.matchKeyword("false")) return error.UnsupportedSqlShape;
            }
            try self.expect(.rparen);
            try json_set.append(self.alloc, .{
                .field = field,
                .path = path,
                .value_json = value_json,
            });
            field_transferred = true;
            path_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.matchKeyword("array_append") or self.matchKeyword("array_remove")) {
            const op: db_mod.types.TransformOpType = if (std.ascii.eqlIgnoreCase(self.tokens[self.pos - 1].text, "array_append")) .push else .pull;
            if (column.field_type != .array) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const array_field = try self.parseIdentifierOwned();
            defer self.alloc.free(array_field);
            if (!std.mem.eql(u8, array_field, field)) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const value_json = try self.parseJsonValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try self.expect(.rparen);
            try array_update.append(self.alloc, .{
                .field = field,
                .op = op,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .pipe_concat) {
            const json_field_token = self.match(.identifier).?;
            if (!std.mem.eql(u8, json_field_token.text, field)) return error.UnsupportedSqlShape;
            try self.expect(.pipe_concat);
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try self.appendJsonObjectConcatSetValues(field, json_set);
            return;
        }
        if (self.peekKind(.identifier) and self.pos + 1 < self.tokens.len and (self.tokens[self.pos + 1].kind == .plus or self.tokens[self.pos + 1].kind == .minus)) {
            try self.parseIncrementAssignment(field, column, increment);
            return;
        }

        const value_json = try self.parseSqlColumnValueAlloc(column);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        try patch.append(self.alloc, .{
            .field = field,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn parsePrimaryWhereJsonAlloc(self: *@This()) ![]u8 {
        const primary_key = self.schema.primary_key orelse return error.InvalidSqlCatalog;
        var values = std.ArrayListUnmanaged(FieldJsonValue).empty;
        defer {
            freeFieldJsonValues(self.alloc, values.items);
            values.deinit(self.alloc);
        }

        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
            if (!primaryKeyContains(primary_key, field)) return error.UnsupportedSqlShape;
            try self.expect(.eq);
            const value_json = try self.parseSqlColumnValueAlloc(column);
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try values.append(self.alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            if (!self.matchKeyword("and")) break;
        }

        if (values.items.len != primary_key.columns.len) return error.UnsupportedSqlShape;
        for (primary_key.columns) |column_name| {
            var found = false;
            for (values.items) |value| {
                if (std.mem.eql(u8, value.field, column_name)) {
                    found = true;
                    break;
                }
            }
            if (!found) return error.UnsupportedSqlShape;
        }

        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"primary\":{");
        for (values.items, 0..) |value, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(value.field, .{})});
            try writer.writeAll(value.value_json);
        }
        try writer.writeAll("}}");
        return try out.toOwnedSlice();
    }

    fn expectedVersionForWhereAlloc(self: *@This(), table_name: []const u8, where_json: []const u8) !u64 {
        const resolver = self.unique_resolver orelse return error.UnsupportedRowsSelector;
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        const key = (try relational_rows.physicalPrimaryKeyFromWhereAlloc(self.alloc, table_name, self.schema, parsed.value, resolver, false)) orelse return error.RowSelectorNotFound;
        defer self.alloc.free(key);
        var row = (try resolver.lookupPrimary(self.alloc, table_name, key)) orelse return error.RowSelectorNotFound;
        defer row.deinit(self.alloc);
        return row.version;
    }

    fn updateBodyJsonAlloc(
        self: *@This(),
        where_json: []const u8,
        patch: []const FieldJsonValue,
        increment: []const FieldJsonValue,
        json_set: []const JsonSetValue,
        array_update: []const ArrayTransformValue,
        returning: ReturningProjection,
        expected_version: ?u64,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"operations\":[{\"op\":\"update\",\"where\":");
        try writer.writeAll(where_json);
        if (expected_version) |version| try writer.print(",\"expected_version\":{d}", .{version});
        if (patch.len > 0) {
            try writer.writeAll(",\"patch\":{");
            for (patch, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                try writer.writeAll(item.value_json);
            }
            try writer.writeByte('}');
        }
        if (increment.len > 0) {
            try writer.writeAll(",\"increment\":{");
            for (increment, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                try writer.writeAll(item.value_json);
            }
            try writer.writeByte('}');
        }
        if (json_set.len > 0) {
            try writer.writeAll(",\"json_set\":[");
            for (json_set, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{{\"field\":{f},\"path\":[", .{std.json.fmt(item.field, .{})});
                for (item.path, 0..) |segment, segment_i| {
                    if (segment_i != 0) try writer.writeByte(',');
                    try writer.print("{f}", .{std.json.fmt(segment, .{})});
                }
                try writer.writeAll("],\"value\":");
                try writer.writeAll(item.value_json);
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
        }
        if (array_update.len > 0) {
            try writer.writeAll(",\"array_update\":[");
            for (array_update, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
                    std.json.fmt(item.field, .{}),
                    std.json.fmt(arrayTransformOpToken(item.op), .{}),
                });
                try writer.writeAll(item.value_json);
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
        }
        try self.writeReturningProjectionJson(writer, returning);
        try writer.writeAll("}]}");
        return try out.toOwnedSlice();
    }

    fn deleteBodyJsonAlloc(
        self: *@This(),
        where_json: []const u8,
        returning: ReturningProjection,
        expected_version: ?u64,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"operations\":[{\"op\":\"delete\",\"where\":");
        try writer.writeAll(where_json);
        if (expected_version) |version| try writer.print(",\"expected_version\":{d}", .{version});
        try self.writeReturningProjectionJson(writer, returning);
        try writer.writeAll("}]}");
        return try out.toOwnedSlice();
    }

    fn mutationRowClaimAlloc(self: *@This(), skip_locked: bool) !db_mod.types.RowClaimRequest {
        const claim = self.mutation_claim orelse return error.UnsupportedRowsQuery;
        if (claim.txn_id == null) return error.UnsupportedRowsQuery;
        return .{
            .mode = claim.mode,
            .skip_locked = skip_locked or claim.skip_locked,
            .lease_ms = claim.lease_ms,
            .owner_id = try self.alloc.dupe(u8, claim.owner_id),
            .txn_id = claim.txn_id,
        };
    }

    fn mutationSourceBodyJsonAlloc(
        self: *@This(),
        op: []const u8,
        source: db_mod.types.RelationalRowsQueryRequest,
        patch: []const FieldJsonValue,
        increment: []const FieldJsonValue,
        json_set: []const JsonSetValue,
        array_update: []const ArrayTransformValue,
        returning: ReturningProjection,
    ) ![]u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.print("{{\"op\":{f},\"source\":", .{std.json.fmt(op, .{})});
        try self.writeMutationSourceQueryJson(writer, source);
        if (patch.len > 0) {
            try writer.writeAll(",\"patch\":{");
            for (patch, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                try writer.writeAll(item.value_json);
            }
            try writer.writeByte('}');
        }
        if (increment.len > 0) {
            try writer.writeAll(",\"increment\":{");
            for (increment, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                try writer.writeAll(item.value_json);
            }
            try writer.writeByte('}');
        }
        if (json_set.len > 0) {
            try writer.writeAll(",\"json_set\":[");
            for (json_set, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{{\"field\":{f},\"path\":[", .{std.json.fmt(item.field, .{})});
                for (item.path, 0..) |segment, segment_i| {
                    if (segment_i != 0) try writer.writeByte(',');
                    try writer.print("{f}", .{std.json.fmt(segment, .{})});
                }
                try writer.writeAll("],\"value\":");
                try writer.writeAll(item.value_json);
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
        }
        if (array_update.len > 0) {
            try writer.writeAll(",\"array_update\":[");
            for (array_update, 0..) |item, i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
                    std.json.fmt(item.field, .{}),
                    std.json.fmt(arrayTransformOpToken(item.op), .{}),
                });
                try writer.writeAll(item.value_json);
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
        }
        try self.writeReturningProjectionJson(writer, returning);
        try writer.writeByte('}');
        return try out.toOwnedSlice();
    }

    fn writeMutationSourceQueryJson(
        self: *@This(),
        writer: *std.Io.Writer,
        source: db_mod.types.RelationalRowsQueryRequest,
    ) !void {
        _ = self;
        try writer.writeByte('{');
        var wrote = false;
        if (source.predicates.len > 0 or
            source.array_contains.len > 0 or
            source.array_eq.len > 0 or
            source.in_predicates.len > 0 or
            source.json_contains.len > 0 or
            source.json_path_eq.len > 0 or
            source.json_path_exists.len > 0 or
            source.or_predicates.len > 0 or
            source.not_predicates.len > 0)
        {
            try writer.writeAll("\"where\":{");
            var wrote_where = false;
            if (source.predicates.len > 0 or source.array_contains.len > 0 or source.array_eq.len > 0 or source.in_predicates.len > 0 or source.json_contains.len > 0 or source.json_path_eq.len > 0 or source.json_path_exists.len > 0) {
                try writer.writeAll("\"all\":[");
                var wrote_atom = false;
                try writeRelationalCheckAtomsJson(writer, &wrote_atom, source.predicates);
                try writeInPredicateAtomsJson(writer, &wrote_atom, source.in_predicates);
                try writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "array_contains", source.array_contains);
                try writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "array_eq", source.array_eq);
                try writeStructuredValuePredicateAtomsJson(writer, &wrote_atom, "json_contains", source.json_contains);
                try writeJsonPathEqPredicateAtomsJson(writer, &wrote_atom, source.json_path_eq);
                try writeJsonPathExistsPredicateAtomsJson(writer, &wrote_atom, source.json_path_exists);
                try writer.writeByte(']');
                wrote_where = true;
            }
            if (source.or_predicates.len > 0) {
                if (wrote_where) try writer.writeByte(',');
                try writer.writeAll("\"any\":[");
                for (source.or_predicates, 0..) |group, group_i| {
                    if (group_i != 0) try writer.writeByte(',');
                    if (group.predicates.len == 1) {
                        try writeRelationalCheckAtomJson(writer, group.predicates[0]);
                    } else {
                        try writer.writeAll("{\"all\":[");
                        var wrote_atom = false;
                        try writeRelationalCheckAtomsJson(writer, &wrote_atom, group.predicates);
                        try writer.writeAll("]}");
                    }
                }
                try writer.writeByte(']');
                wrote_where = true;
            }
            if (source.not_predicates.len > 0) {
                if (wrote_where) try writer.writeByte(',');
                try writer.writeAll("\"not\":[");
                for (source.not_predicates, 0..) |group, group_i| {
                    if (group_i != 0) try writer.writeByte(',');
                    if (group.predicates.len == 1) {
                        try writeRelationalCheckAtomJson(writer, group.predicates[0]);
                    } else {
                        try writer.writeAll("{\"all\":[");
                        var wrote_atom = false;
                        try writeRelationalCheckAtomsJson(writer, &wrote_atom, group.predicates);
                        try writer.writeAll("]}");
                    }
                }
                try writer.writeByte(']');
            }
            try writer.writeByte('}');
            wrote = true;
        }
        if (source.order_by.len > 0) {
            if (wrote) try writer.writeByte(',');
            try writer.writeAll("\"order_by\":[");
            for (source.order_by, 0..) |order, i| {
                if (i != 0) try writer.writeByte(',');
                if (order.expression) |expression| {
                    try writer.writeAll("{\"expr\":");
                    try writeRowExpressionJson(writer, expression);
                } else {
                    try writer.print("{{\"field\":{f}", .{std.json.fmt(order.field, .{})});
                }
                if (order.direction == .desc) try writer.writeAll(",\"direction\":\"desc\"");
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
            wrote = true;
        }
        if (source.limit) |limit| {
            if (wrote) try writer.writeByte(',');
            try writer.print("\"limit\":{d}", .{limit});
            wrote = true;
        }
        if (source.offset != 0) {
            if (wrote) try writer.writeByte(',');
            try writer.print("\"offset\":{d}", .{source.offset});
            wrote = true;
        }
        if (source.row_claim) |claim| {
            if (wrote) try writer.writeByte(',');
            const txn_id = claim.txn_id orelse return error.UnsupportedRowsQuery;
            const txn_hex = encodeSqlTxnIdHex(txn_id);
            try writer.print("\"row_claim\":{{\"mode\":\"for_update\",\"skip_locked\":{},\"lease_ms\":{d},\"owner_id\":{f},\"transaction_id\":\"{s}\"}}", .{
                claim.skip_locked,
                claim.lease_ms,
                std.json.fmt(claim.owner_id, .{}),
                txn_hex,
            });
        }
        try writer.writeByte('}');
    }

    fn writeReturningJson(self: *@This(), writer: *std.Io.Writer, returning_fields: []const []const u8) !void {
        _ = self;
        if (returning_fields.len == 0) return;
        try writer.writeAll(",\"returning\":[");
        for (returning_fields, 0..) |field, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}", .{std.json.fmt(field, .{})});
        }
        try writer.writeByte(']');
    }

    fn writeReturningProjectionJson(self: *@This(), writer: *std.Io.Writer, returning: ReturningProjection) !void {
        if (returning.fields.len > 0) try self.writeReturningJson(writer, returning.fields);
        if (returning.expressions.len == 0) return;
        try writer.writeAll(",\"returning_expressions\":[");
        for (returning.expressions, 0..) |projection, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"as\":{f},\"expr\":", .{std.json.fmt(projection.output, .{})});
            try writeRowExpressionJson(writer, projection.expression);
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }

    fn writeRowExpressionJson(writer: *std.Io.Writer, expression: db_mod.types.RelationalRowsExpression) !void {
        switch (expression.kind) {
            .field => try writer.print("{{\"field\":{f}}}", .{std.json.fmt(expression.field, .{})}),
            .value => {
                try writer.writeAll("{\"value\":");
                try writer.writeAll(expression.value_json);
                try writer.writeByte('}');
            },
            .case => {
                try writer.writeAll("{\"op\":\"case\",\"cases\":[");
                for (expression.case_branches, 0..) |branch, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.writeAll("{\"when\":{\"lhs\":");
                    try writeRowExpressionJson(writer, branch.when.lhs);
                    try writer.print(",\"op\":{f}", .{std.json.fmt(rowExpressionConditionOpName(branch.when.op), .{})});
                    if (branch.when.rhs.len == 1) {
                        try writer.writeAll(",\"rhs\":");
                        try writeRowExpressionJson(writer, branch.when.rhs[0]);
                    }
                    try writer.writeAll("},\"then\":");
                    try writeRowExpressionJson(writer, branch.then);
                    try writer.writeByte('}');
                }
                try writer.writeAll("],\"else\":");
                if (expression.case_else.len != 1) return error.UnsupportedSqlShape;
                try writeRowExpressionJson(writer, expression.case_else[0]);
                try writer.writeByte('}');
            },
            .cast => {
                if (expression.operands.len != 1) return error.UnsupportedSqlShape;
                const cast_type = expression.cast_type orelse return error.UnsupportedSqlShape;
                try writer.print("{{\"op\":\"cast\",\"to\":{f},\"args\":[", .{std.json.fmt(rowExpressionCastTypeName(cast_type), .{})});
                try writeRowExpressionJson(writer, expression.operands[0]);
                try writer.writeAll("]}");
            },
            .json_extract => {
                if (expression.operands.len != 1 or expression.json_path.len == 0) return error.UnsupportedSqlShape;
                try writer.writeAll("{\"op\":\"json_extract\",\"args\":[");
                try writeRowExpressionJson(writer, expression.operands[0]);
                try writer.print("],\"path\":{f}", .{std.json.fmt(expression.json_path, .{})});
                if (expression.json_as_text) try writer.writeAll(",\"as_text\":true");
                try writer.writeByte('}');
            },
            else => {
                try writer.print("{{\"op\":{f},\"args\":[", .{std.json.fmt(rowExpressionOpName(expression.kind), .{})});
                for (expression.operands, 0..) |operand, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writeRowExpressionJson(writer, operand);
                }
                try writer.writeAll("]}");
            },
        }
    }

    fn rowExpressionOpName(kind: db_mod.types.RelationalRowsExpressionKind) []const u8 {
        return switch (kind) {
            .field, .value => unreachable,
            .coalesce => "coalesce",
            .lower => "lower",
            .concat => "concat",
            .nullif => "nullif",
            .add => "add",
            .sub => "sub",
            .mul => "mul",
            .div => "div",
            .case => unreachable,
            .cast => unreachable,
            .json_extract => unreachable,
            .array_length => "array_length",
        };
    }

    fn rowExpressionCastTypeName(cast_type: db_mod.types.RelationalRowsExpressionCastType) []const u8 {
        return switch (cast_type) {
            .text => "text",
            .numeric => "numeric",
            .bool => "bool",
        };
    }

    fn rowExpressionConditionOpName(op: runtime_schema.RelationalCheckOp) []const u8 {
        return switch (op) {
            .eq => "eq",
            .ne => "ne",
            .gt => "gt",
            .gte => "gte",
            .lt => "lt",
            .lte => "lte",
            .is_null => "is_null",
            .is_not_null => "is_not_null",
        };
    }

    fn parseWhere(
        self: *@This(),
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
        json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
        json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
        array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
        array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
        in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
        or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
        not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    ) !void {
        if (self.whereHasTopLevelOr()) {
            try self.parseScalarOrWhere(or_predicates);
            return;
        }
        while (true) {
            if (self.canParseScalarNotWhere()) {
                try self.parseScalarNotWhere(not_predicates);
            } else {
                try self.parseWhereAtom(predicates, json_contains, json_path_eq, json_path_exists, array_contains, array_eq, in_predicates, false);
            }
            if (!self.matchKeyword("and")) break;
        }
    }

    fn canParseScalarNotWhere(self: *@This()) bool {
        if (!self.peekKeyword("not") or self.pos + 1 >= self.tokens.len or self.tokens[self.pos + 1].kind != .lparen) return false;
        var i = self.pos + 2;
        var depth: usize = 1;
        while (i < self.tokens.len) : (i += 1) {
            const token = self.tokens[i];
            switch (token.kind) {
                .lparen => return false,
                .rparen => {
                    depth -= 1;
                    if (depth == 0) return true;
                },
                .arrow_text, .arrow_json, .at_contains, .question => return false,
                .identifier => {
                    if (std.ascii.eqlIgnoreCase(token.text, "or") or
                        std.ascii.eqlIgnoreCase(token.text, "any") or
                        std.ascii.eqlIgnoreCase(token.text, "in") or
                        std.ascii.eqlIgnoreCase(token.text, "exists"))
                    {
                        return false;
                    }
                    if (std.ascii.eqlIgnoreCase(token.text, "not")) {
                        if (i == 0 or self.tokens[i - 1].kind != .identifier or
                            !std.ascii.eqlIgnoreCase(self.tokens[i - 1].text, "is"))
                        {
                            return false;
                        }
                    }
                },
                else => {},
            }
        }
        return false;
    }

    fn parseScalarNotWhere(
        self: *@This(),
        not_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    ) !void {
        try self.expectKeyword("not");
        try self.expect(.lparen);
        var branch = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
        errdefer {
            freeRelationalChecks(self.alloc, branch.items);
            branch.deinit(self.alloc);
        }
        while (true) {
            const predicate = try self.parseScalarWherePredicateAlloc();
            var predicate_transferred = false;
            errdefer if (!predicate_transferred) freeRelationalCheck(self.alloc, predicate);
            try branch.append(self.alloc, predicate);
            predicate_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
        if (branch.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);

        const predicates = try branch.toOwnedSlice(self.alloc);
        var predicates_transferred = false;
        errdefer if (!predicates_transferred) {
            freeRelationalChecks(self.alloc, predicates);
            if (predicates.len > 0) self.alloc.free(predicates);
        };
        try not_predicates.append(self.alloc, .{ .predicates = predicates });
        predicates_transferred = true;
    }

    fn parseScalarOrWhere(
        self: *@This(),
        or_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsPredicateGroup),
    ) !void {
        while (true) {
            var branch = std.ArrayListUnmanaged(runtime_schema.RelationalCheck).empty;
            errdefer {
                freeRelationalChecks(self.alloc, branch.items);
                branch.deinit(self.alloc);
            }

            while (true) {
                const predicate = try self.parseScalarWherePredicateAlloc();
                var predicate_transferred = false;
                errdefer if (!predicate_transferred) freeRelationalCheck(self.alloc, predicate);
                try branch.append(self.alloc, predicate);
                predicate_transferred = true;
                if (!self.matchKeyword("and")) break;
            }

            if (branch.items.len == 0) return error.UnsupportedSqlShape;
            const predicates = try branch.toOwnedSlice(self.alloc);
            var predicates_transferred = false;
            errdefer if (!predicates_transferred) {
                freeRelationalChecks(self.alloc, predicates);
                if (predicates.len > 0) self.alloc.free(predicates);
            };
            try or_predicates.append(self.alloc, .{ .predicates = predicates });
            predicates_transferred = true;

            if (!self.matchKeyword("or")) break;
        }
    }

    fn parseScalarWherePredicateAlloc(self: *@This()) !runtime_schema.RelationalCheck {
        const field = try self.parseFieldExpressionOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        if (self.peekKind(.arrow_text) or self.peekKind(.arrow_json) or self.peekKind(.at_contains) or self.peekKind(.question)) return error.UnsupportedSqlShape;
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (column.field_type == .array or column.field_type == .json) return error.UnsupportedSqlShape;

        if (self.matchKeyword("is")) {
            const not = self.matchKeyword("not");
            if (not and self.matchKeyword("distinct")) {
                try self.expectKeyword("from");
                const value_json = try self.parseSqlColumnValueAlloc(column);
                var value_transferred = false;
                errdefer if (!value_transferred) self.alloc.free(value_json);
                const op: runtime_schema.RelationalCheckOp = if (std.mem.eql(u8, value_json, "null")) .is_null else .eq;
                field_transferred = true;
                if (op == .is_null) {
                    self.alloc.free(value_json);
                    value_transferred = true;
                    return .{
                        .name = "",
                        .field = field,
                        .op = op,
                        .value_json = null,
                    };
                }
                value_transferred = true;
                return .{
                    .name = "",
                    .field = field,
                    .op = op,
                    .value_json = value_json,
                };
            }
            if (!not and self.peekKeyword("distinct")) return error.UnsupportedSqlShape;
            const op: runtime_schema.RelationalCheckOp = if (not) blk: {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            } else blk: {
                try self.expectKeyword("null");
                break :blk .is_null;
            };
            field_transferred = true;
            return .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = null,
            };
        }

        if (self.peekKeyword("in") or self.peekKeyword("not")) return error.UnsupportedSqlShape;
        const op = try self.parseComparisonOp();
        if (self.peekKeyword("any")) return error.UnsupportedSqlShape;
        const value_json = try self.parseSqlColumnValueAlloc(column);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        field_transferred = true;
        value_transferred = true;
        return .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = value_json,
        };
    }

    fn parseWhereAtom(
        self: *@This(),
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        json_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonContainsPredicate),
        json_path_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathEqPredicate),
        json_path_exists: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsJsonPathExistsPredicate),
        array_contains: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayContainsPredicate),
        array_eq: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsArrayEqPredicate),
        in_predicates: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsInPredicate),
        negated: bool,
    ) !void {
        if (!negated and self.matchKeyword("not")) {
            try self.expect(.lparen);
            try self.parseWhereAtom(predicates, json_contains, json_path_eq, json_path_exists, array_contains, array_eq, in_predicates, true);
            try self.expect(.rparen);
            return;
        }
        const field = try self.parseFieldExpressionOwned();
        var field_transferred = false;
        defer if (!field_transferred) self.alloc.free(field);
        const maybe_column = relationalColumnForField(self.schema, field, null);

        if (self.match(.arrow_text) != null) {
            const path = try self.parseJsonPathOwned();
            var path_transferred = false;
            errdefer if (!path_transferred) self.alloc.free(path);
            try self.expect(.eq);
            const value_json = try self.parseJsonValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (relationalColumnForField(self.schema, field, .json) == null) return error.InvalidSqlCatalog;
            const predicate_field = try self.alloc.dupe(u8, field);
            var predicate_field_transferred = false;
            errdefer if (!predicate_field_transferred) self.alloc.free(predicate_field);
            try json_path_eq.append(self.alloc, .{
                .field = predicate_field,
                .path = path,
                .value_json = value_json,
            });
            predicate_field_transferred = true;
            path_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.match(.at_contains) != null) {
            const column = maybe_column orelse return error.InvalidSqlCatalog;
            const value_json = try self.parseJsonDocumentValueAlloc();
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            if (column.field_type == .array) {
                try validateJsonArray(self.alloc, value_json);
                try array_contains.append(self.alloc, .{
                    .field = field,
                    .value_json = value_json,
                });
                field_transferred = true;
                value_transferred = true;
                return;
            }
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            try json_contains.append(self.alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        if (self.match(.question) != null) {
            if (relationalColumnForField(self.schema, field, .json) == null) return error.InvalidSqlCatalog;
            const path = try self.parseJsonPathOwned();
            var path_transferred = false;
            errdefer if (!path_transferred) self.alloc.free(path);
            try json_path_exists.append(self.alloc, .{
                .field = field,
                .path = path,
            });
            field_transferred = true;
            path_transferred = true;
            return;
        }
        if (self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;

        const column = maybe_column orelse return error.InvalidSqlCatalog;
        if (self.matchKeyword("is")) {
            const not = self.matchKeyword("not");
            if (not and self.matchKeyword("distinct")) {
                try self.expectKeyword("from");
                const value_json = try self.parseSqlColumnValueAlloc(column);
                var value_transferred = false;
                errdefer if (!value_transferred) self.alloc.free(value_json);
                if (std.mem.eql(u8, value_json, "null")) {
                    self.alloc.free(value_json);
                    value_transferred = true;
                    try predicates.append(self.alloc, .{
                        .name = "",
                        .field = field,
                        .op = .is_null,
                        .value_json = null,
                    });
                } else {
                    try predicates.append(self.alloc, .{
                        .name = "",
                        .field = field,
                        .op = .eq,
                        .value_json = value_json,
                    });
                    value_transferred = true;
                }
                field_transferred = true;
                return;
            }
            if (!not and self.peekKeyword("distinct")) return error.UnsupportedSqlShape;
            const op: runtime_schema.RelationalCheckOp = if (not) blk: {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            } else blk: {
                try self.expectKeyword("null");
                break :blk .is_null;
            };
            try predicates.append(self.alloc, .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = null,
            });
            field_transferred = true;
            return;
        }
        if (self.matchKeyword("not")) {
            try self.expectKeyword("in");
            if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
            const values_json = try self.parseSqlInValuesJsonAlloc();
            var values_transferred = false;
            errdefer if (!values_transferred) self.alloc.free(values_json);
            try in_predicates.append(self.alloc, .{
                .field = field,
                .values_json = values_json,
                .negated = true,
            });
            field_transferred = true;
            values_transferred = true;
            return;
        }
        if (self.matchKeyword("in")) {
            if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
            const values_json = try self.parseSqlInValuesJsonAlloc();
            var values_transferred = false;
            errdefer if (!values_transferred) self.alloc.free(values_json);
            try in_predicates.append(self.alloc, .{
                .field = field,
                .values_json = values_json,
                .negated = false,
            });
            field_transferred = true;
            values_transferred = true;
            return;
        }

        const op = try self.parseComparisonOp();
        if (op == .eq and self.matchKeyword("any")) {
            if (column.field_type == .array or column.field_type == .json) return error.InvalidSqlCatalog;
            try self.expect(.lparen);
            const values_json = try self.parseJsonArrayValueAlloc();
            var values_transferred = false;
            errdefer if (!values_transferred) self.alloc.free(values_json);
            try self.expect(.rparen);
            try in_predicates.append(self.alloc, .{
                .field = field,
                .values_json = values_json,
                .negated = negated,
            });
            field_transferred = true;
            values_transferred = true;
            return;
        }
        if (negated) return error.UnsupportedSqlShape;
        const value_json = try self.parseSqlColumnValueAlloc(column);
        var value_transferred = false;
        errdefer if (!value_transferred) self.alloc.free(value_json);
        if (column.field_type == .array) {
            if (op != .eq) return error.UnsupportedSqlShape;
            try validateJsonArray(self.alloc, value_json);
            try array_eq.append(self.alloc, .{
                .field = field,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            return;
        }
        try predicates.append(self.alloc, .{
            .name = "",
            .field = field,
            .op = op,
            .value_json = value_json,
        });
        field_transferred = true;
        value_transferred = true;
    }

    fn parseOrderBy(self: *@This(), order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder)) !void {
        while (true) {
            var order = try self.parseOrderExpressionAlloc();
            var order_transferred = false;
            errdefer if (!order_transferred) {
                if (order.field.len > 0) self.alloc.free(order.field);
                if (order.expression) |expression| freeExpression(self.alloc, expression);
            };
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            order.direction = direction;
            try order_by.append(self.alloc, order);
            order_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseOrderExpressionAlloc(self: *@This()) !db_mod.types.RelationalRowsQueryOrder {
        if (self.match(.lparen) != null) {
            const field = try self.parseFieldExpressionOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try self.expectKeyword("is");
            const null_test: db_mod.types.RelationalRowsQueryOrderNullTest = if (self.matchKeyword("not")) blk: {
                try self.expectKeyword("null");
                break :blk .is_not_null;
            } else blk: {
                try self.expectKeyword("null");
                break :blk .is_null;
            };
            try self.expect(.rparen);
            field_transferred = true;
            return .{ .field = field, .null_test = null_test };
        }

        if (self.peekKeyword("lower")) {
            const expression = try self.parseLowerRowExpressionAlloc();
            var expression_transferred = false;
            errdefer if (!expression_transferred) freeExpression(self.alloc, expression);
            if (expression.operands.len == 1 and expression.operands[0].kind == .field) {
                if (generatedLowerColumnForField(self.schema, expression.operands[0].field)) |generated| {
                    const field = try self.alloc.dupe(u8, generated.name);
                    freeExpression(self.alloc, expression);
                    expression_transferred = true;
                    return .{ .field = field };
                }
            }
            expression_transferred = true;
            return .{ .expression = expression };
        }
        if (self.peekKeyword("case") or self.peekKeyword("cast") or self.peekKeyword("nullif")) {
            const expression = try self.parseRowExpressionOperandAlloc();
            return .{ .expression = expression };
        }

        const field = try self.parseFieldExpressionOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
        if (self.peekArithmeticOperator()) |_| {
            const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
            if (column.field_type != .numeric) return error.InvalidSqlCatalog;
            field_transferred = true;
            const expression = try self.parseArithmeticExpressionRestAlloc(.{ .kind = .field, .field = field }, 0);
            return .{ .expression = expression };
        }
        field_transferred = true;
        return .{ .field = field };
    }

    fn parseGroupBy(self: *@This(), group_by: *std.ArrayListUnmanaged([]const u8)) !void {
        while (true) {
            const field = try self.parseFieldExpressionOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            try group_by.append(self.alloc, field);
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseAggregateOrderBy(
        self: *@This(),
        order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
        group_fields: []const []const u8,
        aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!aggregateOutputContainsField(group_fields, aggregations, field)) return error.UnsupportedSqlShape;
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            try order_by.append(self.alloc, .{ .field = field, .direction = direction });
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseWindowOutputOrderBy(
        self: *@This(),
        order_by: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder),
        fields: []const []const u8,
        windows: []const db_mod.types.RelationalRowsWindowSpec,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!windowOutputContainsField(fields, windows, field)) return error.UnsupportedSqlShape;
            const direction: db_mod.types.RelationalRowsQueryOrderDirection = if (self.matchKeyword("desc"))
                .desc
            else blk: {
                _ = self.matchKeyword("asc");
                break :blk .asc;
            };
            try order_by.append(self.alloc, .{ .field = field, .direction = direction });
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
    }

    fn parseAggregateHaving(
        self: *@This(),
        predicates: *std.ArrayListUnmanaged(runtime_schema.RelationalCheck),
        group_fields: []const []const u8,
        aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ) !void {
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (!aggregateOutputContainsField(group_fields, aggregations, field)) return error.UnsupportedSqlShape;
            const op = if (self.matchKeyword("is")) blk: {
                const null_op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("not")) op_blk: {
                    try self.expectKeyword("null");
                    break :op_blk .is_not_null;
                } else op_blk: {
                    try self.expectKeyword("null");
                    break :op_blk .is_null;
                };
                break :blk null_op;
            } else try self.parseComparisonOp();
            const value_json = switch (op) {
                .is_null, .is_not_null => null,
                else => try self.parseJsonValueAlloc(),
            };
            var value_transferred = false;
            errdefer if (!value_transferred) if (value_json) |json| self.alloc.free(json);
            try predicates.append(self.alloc, .{
                .name = "",
                .field = field,
                .op = op,
                .value_json = value_json,
            });
            field_transferred = true;
            value_transferred = true;
            if (!self.matchKeyword("and")) break;
        }
    }

    fn parseComparisonOp(self: *@This()) !runtime_schema.RelationalCheckOp {
        if (self.match(.eq) != null) return .eq;
        if (self.match(.neq) != null) return .ne;
        if (self.match(.gt) != null) return .gt;
        if (self.match(.gte) != null) return .gte;
        if (self.match(.lt) != null) return .lt;
        if (self.match(.lte) != null) return .lte;
        return error.UnsupportedSqlShape;
    }

    fn parseJsonValueAlloc(self: *@This()) ![]const u8 {
        if (self.matchKeyword("null")) return try self.alloc.dupe(u8, "null");
        if (self.matchKeyword("true")) return try self.alloc.dupe(u8, "true");
        if (self.matchKeyword("false")) return try self.alloc.dupe(u8, "false");
        if (self.match(.string)) |token| return try std.json.Stringify.valueAlloc(self.alloc, token.text, .{});
        if (self.match(.number)) |token| return try self.alloc.dupe(u8, token.text);
        if (self.match(.placeholder)) |token| return try self.boundValueJsonAlloc(token);
        return error.UnsupportedSqlShape;
    }

    fn parseJsonDocumentValueAlloc(self: *@This()) ![]const u8 {
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            return switch (value) {
                .json => |json| blk: {
                    try validateJsonDocument(self.alloc, json);
                    break :blk try self.alloc.dupe(u8, json);
                },
                else => error.UnsupportedSqlShape,
            };
        }
        if (self.match(.string)) |token| {
            try validateJsonDocument(self.alloc, token.text);
            return try self.alloc.dupe(u8, token.text);
        }
        return error.UnsupportedSqlShape;
    }

    fn appendJsonObjectConcatSetValues(
        self: *@This(),
        field: []const u8,
        json_set: *std.ArrayListUnmanaged(JsonSetValue),
    ) !void {
        const object_json = try self.parseJsonDocumentValueAlloc();
        defer self.alloc.free(object_json);
        var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, object_json, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (parsed.value != .object) return error.UnsupportedSqlShape;
        if (parsed.value.object.count() == 0) return error.UnsupportedSqlShape;

        var it = parsed.value.object.iterator();
        while (it.next()) |entry| {
            if (std.mem.indexOfScalar(u8, entry.key_ptr.*, '.') != null) return error.UnsupportedSqlShape;
            const owned_field = try self.alloc.dupe(u8, field);
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(owned_field);
            const path = try self.alloc.alloc([]const u8, 1);
            var path_transferred = false;
            errdefer if (!path_transferred) self.alloc.free(path);
            path[0] = try self.alloc.dupe(u8, entry.key_ptr.*);
            var path_item_transferred = false;
            errdefer if (!path_item_transferred) self.alloc.free(path[0]);
            const value_json = try std.json.Stringify.valueAlloc(self.alloc, entry.value_ptr.*, .{});
            var value_transferred = false;
            errdefer if (!value_transferred) self.alloc.free(value_json);
            try json_set.append(self.alloc, .{
                .field = owned_field,
                .path = path,
                .value_json = value_json,
            });
            field_transferred = true;
            path_transferred = true;
            path_item_transferred = true;
            value_transferred = true;
        }
    }

    fn parseJsonArrayValueAlloc(self: *@This()) ![]const u8 {
        const value_json = try self.parseJsonDocumentValueAlloc();
        errdefer self.alloc.free(value_json);
        try validateJsonArray(self.alloc, value_json);
        return value_json;
    }

    fn parseSqlInValuesJsonAlloc(self: *@This()) ![]const u8 {
        try self.expect(.lparen);
        if (self.peekKind(.rparen)) return error.UnsupportedSqlShape;

        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            if (self.match(.rparen) != null) {
                return switch (value) {
                    .json => |json| blk: {
                        try validateJsonArray(self.alloc, json);
                        break :blk try self.alloc.dupe(u8, json);
                    },
                    else => try self.singleValueJsonArrayAlloc(value),
                };
            }
            const first_json = try value.jsonAlloc(self.alloc);
            defer self.alloc.free(first_json);
            return try self.parseSqlInRemainingValuesJsonAlloc(first_json);
        }

        const first_json = try self.parseJsonValueAlloc();
        defer self.alloc.free(first_json);
        return try self.parseSqlInRemainingValuesJsonAlloc(first_json);
    }

    fn parseSqlInRemainingValuesJsonAlloc(self: *@This(), first_json: []const u8) ![]const u8 {
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('[');
        try writer.writeAll(first_json);
        while (self.match(.comma) != null) {
            const value_json = try self.parseJsonValueAlloc();
            defer self.alloc.free(value_json);
            try writer.writeByte(',');
            try writer.writeAll(value_json);
        }
        try self.expect(.rparen);
        try writer.writeByte(']');
        return try out.toOwnedSlice();
    }

    fn singleValueJsonArrayAlloc(self: *@This(), value: SqlValue) ![]const u8 {
        const value_json = try value.jsonAlloc(self.alloc);
        defer self.alloc.free(value_json);
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('[');
        try writer.writeAll(value_json);
        try writer.writeByte(']');
        return try out.toOwnedSlice();
    }

    fn parseSqlColumnValueAlloc(self: *@This(), column: runtime_schema.RelationalColumn) ![]const u8 {
        if (self.matchKeyword("default")) {
            const default_value = column.default_value orelse return error.UnsupportedSqlShape;
            return try relational_rows.relationalDefaultValueJsonAlloc(self.alloc, default_value);
        }
        if (self.peekKeyword("now")) {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
            return try self.parseNowValueJsonAlloc();
        }
        if (self.peekKeyword("convert_from")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            return try self.parseConvertFromJsonAlloc();
        }
        if (self.peekKeyword("jsonb_build_object")) {
            if (column.field_type != .json) return error.InvalidSqlCatalog;
            return try self.parseJsonbBuildObjectAlloc();
        }
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            if (column.field_type == .json) {
                return switch (value) {
                    .json => |json| try self.alloc.dupe(u8, json),
                    else => try value.jsonAlloc(self.alloc),
                };
            }
            return try value.jsonAlloc(self.alloc);
        }
        if (self.match(.string)) |token| {
            if (column.field_type == .json) {
                if (jsonValueIsValid(self.alloc, token.text)) return try self.alloc.dupe(u8, token.text);
            }
            return try std.json.Stringify.valueAlloc(self.alloc, token.text, .{});
        }
        if (self.matchKeyword("null")) return try self.alloc.dupe(u8, "null");
        if (self.matchKeyword("true")) return try self.alloc.dupe(u8, "true");
        if (self.matchKeyword("false")) return try self.alloc.dupe(u8, "false");
        if (self.match(.number)) |token| return try self.alloc.dupe(u8, token.text);
        return error.UnsupportedSqlShape;
    }

    fn parseNowValueJsonAlloc(self: *@This()) ![]const u8 {
        try self.expectKeyword("now");
        try self.expect(.lparen);
        try self.expect(.rparen);
        return try std.fmt.allocPrint(self.alloc, "{d}", .{platform_time.realtimeNs()});
    }

    fn parseConvertFromJsonAlloc(self: *@This()) ![]const u8 {
        try self.expectKeyword("convert_from");
        try self.expect(.lparen);
        const decoded = try self.parseConvertFromInputAlloc();
        defer self.alloc.free(decoded);
        try self.expect(.comma);
        const encoding = self.match(.string) orelse return error.UnsupportedSqlShape;
        if (!std.ascii.eqlIgnoreCase(encoding.text, "UTF8") and !std.ascii.eqlIgnoreCase(encoding.text, "UTF-8")) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        if (!jsonValueIsValid(self.alloc, decoded)) return error.UnsupportedSqlShape;
        return try self.alloc.dupe(u8, decoded);
    }

    fn parseConvertFromInputAlloc(self: *@This()) ![]const u8 {
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            return switch (value) {
                .string => |text| try self.alloc.dupe(u8, text),
                .json => |json| try self.alloc.dupe(u8, json),
                else => error.UnsupportedSqlShape,
            };
        }
        if (self.match(.string)) |token| return try self.alloc.dupe(u8, token.text);
        return error.UnsupportedSqlShape;
    }

    fn parseJsonbBuildObjectAlloc(self: *@This()) ![]const u8 {
        try self.expectKeyword("jsonb_build_object");
        try self.expect(.lparen);
        if (self.match(.rparen) != null) return try self.alloc.dupe(u8, "{}");

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(self.alloc);
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeByte('{');
        var first = true;
        while (true) {
            const key_token = self.match(.string) orelse return error.UnsupportedSqlShape;
            const entry = try seen.getOrPut(self.alloc, key_token.text);
            if (entry.found_existing) return error.UnsupportedSqlShape;
            try self.expect(.comma);
            const value_json = try self.parseJsonValueAlloc();
            defer self.alloc.free(value_json);
            if (!first) try writer.writeByte(',');
            first = false;
            try writer.print("{f}:", .{std.json.fmt(key_token.text, .{})});
            try writer.writeAll(value_json);
            if (self.match(.comma) == null) break;
        }
        try self.expect(.rparen);
        try writer.writeByte('}');
        return try out.toOwnedSlice();
    }

    fn parseReturningListAlloc(self: *@This()) ![]const []const u8 {
        if (self.match(.star) != null) {
            const fields = try self.alloc.alloc([]const u8, 1);
            errdefer self.alloc.free(fields);
            fields[0] = try self.alloc.dupe(u8, "*");
            return fields;
        }

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(field);
            fields.deinit(self.alloc);
        }
        while (true) {
            const field = try self.parseIdentifierOwned();
            var field_transferred = false;
            errdefer if (!field_transferred) self.alloc.free(field);
            if (self.peekKind(.lparen) or self.peekKind(.arrow_text) or self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
            if (relationalColumnForReturningField(self.schema, field) == null) return error.InvalidSqlCatalog;
            try fields.append(self.alloc, field);
            field_transferred = true;
            if (self.match(.comma) == null) break;
        }
        return try fields.toOwnedSlice(self.alloc);
    }

    fn parseReturningProjectionAlloc(self: *@This()) !ReturningProjection {
        if (self.match(.star) != null) {
            const fields = try self.alloc.alloc([]const u8, 1);
            errdefer self.alloc.free(fields);
            fields[0] = try self.alloc.dupe(u8, "*");
            return .{ .fields = fields };
        }

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(field);
            fields.deinit(self.alloc);
        }
        var expressions = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionProjection).empty;
        errdefer {
            for (expressions.items) |projection| freeExpressionProjection(self.alloc, projection);
            expressions.deinit(self.alloc);
        }

        while (true) {
            if (self.peekSimpleReturningField()) {
                const field = try self.parseFieldExpressionOwned();
                var field_owned = true;
                errdefer if (field_owned) self.alloc.free(field);
                if (relationalColumnForReturningField(self.schema, field) == null) return error.InvalidSqlCatalog;
                const alias = try self.parseOptionalProjectionAliasAlloc();
                var alias_owned = true;
                errdefer if (alias_owned) if (alias) |owned| self.alloc.free(owned);
                if (alias) |output| {
                    if (std.mem.eql(u8, output, field)) {
                        self.alloc.free(output);
                        alias_owned = false;
                        try fields.append(self.alloc, field);
                        field_owned = false;
                    } else {
                        try expressions.append(self.alloc, .{
                            .output = output,
                            .expression = .{
                                .kind = .field,
                                .field = field,
                            },
                        });
                        alias_owned = false;
                        field_owned = false;
                    }
                } else {
                    try fields.append(self.alloc, field);
                    field_owned = false;
                }
                if (self.match(.comma) == null) break;
                continue;
            }
            const item = try self.parseSelectItem();
            var item_owned = true;
            errdefer if (item_owned) freeSelectItem(self.alloc, item);
            switch (item) {
                .field => |field| {
                    if (relationalColumnForReturningField(self.schema, field) == null) return error.InvalidSqlCatalog;
                    try fields.append(self.alloc, field);
                    item_owned = false;
                },
                .field_alias => |projection| {
                    if (relationalColumnForReturningField(self.schema, projection.field) == null) return error.InvalidSqlCatalog;
                    try expressions.append(self.alloc, .{
                        .output = projection.output,
                        .expression = .{
                            .kind = .field,
                            .field = projection.field,
                        },
                    });
                    item_owned = false;
                },
                .expression => |projection| {
                    try expressions.append(self.alloc, projection);
                    item_owned = false;
                },
                .coalesce => |projection| {
                    const expression_projection = try expressionProjectionFromCoalesceAlloc(self.alloc, projection);
                    var expression_projection_owned = true;
                    errdefer if (expression_projection_owned) freeExpressionProjection(self.alloc, expression_projection);
                    try expressions.append(self.alloc, expression_projection);
                    expression_projection_owned = false;
                    freeCoalesceProjection(self.alloc, projection);
                    item_owned = false;
                },
                else => return error.UnsupportedSqlShape,
            }
            if (self.match(.comma) == null) break;
        }

        const owned_fields = try fields.toOwnedSlice(self.alloc);
        var fields_owned = true;
        errdefer if (fields_owned) freeStringSlice(self.alloc, owned_fields);
        const owned_expressions = try expressions.toOwnedSlice(self.alloc);
        fields_owned = false;
        return .{
            .fields = owned_fields,
            .expressions = owned_expressions,
        };
    }

    fn peekSimpleReturningField(self: *@This()) bool {
        if (!self.peekKind(.identifier)) return false;
        if (self.peekKeyword("lower") or self.peekKeyword("concat") or self.peekKeyword("coalesce") or self.peekKeyword("nullif") or self.peekKeyword("case") or self.peekKeyword("cast")) return false;
        if (self.pos + 1 >= self.tokens.len) return true;
        return switch (self.tokens[self.pos + 1].kind) {
            .plus, .minus, .star, .slash, .lparen, .arrow_json, .arrow_text, .pipe_concat => false,
            else => true,
        };
    }

    fn insertBodyJsonAlloc(self: *@This(), columns: []const []const u8, values: []const []const u8, conflict: ?ConflictClause, returning: ReturningProjection) ![]u8 {
        if (columns.len == 0 or columns.len != values.len) return error.UnsupportedSqlShape;
        var out: std.Io.Writer.Allocating = .init(self.alloc);
        errdefer out.deinit();
        const writer = &out.writer;
        try writer.writeAll("{\"operations\":[{\"op\":\"insert\",\"row\":{");
        for (columns, values, 0..) |column, value_json, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(column, .{})});
            try writer.writeAll(value_json);
        }
        try writer.writeByte('}');
        if (conflict) |clause| {
            try writer.writeAll(",\"on_conflict\":{\"target\":");
            try self.writeConflictTargetJson(writer, clause.target);
            try writer.print(",\"action\":{f}", .{std.json.fmt(conflictActionToken(clause.action), .{})});
            if (clause.patch.len > 0) {
                try writer.writeAll(",\"patch\":{");
                for (clause.patch, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                    try writer.writeAll(item.value_json);
                }
                try writer.writeByte('}');
            }
            if (clause.increment.len > 0) {
                try writer.writeAll(",\"increment\":{");
                for (clause.increment, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}:", .{std.json.fmt(item.field, .{})});
                    try writer.writeAll(item.value_json);
                }
                try writer.writeByte('}');
            }
            if (clause.json_set.len > 0) {
                try writer.writeAll(",\"json_set\":[");
                for (clause.json_set, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{{\"field\":{f},\"path\":[", .{std.json.fmt(item.field, .{})});
                    for (item.path, 0..) |segment, segment_i| {
                        if (segment_i != 0) try writer.writeByte(',');
                        try writer.print("{f}", .{std.json.fmt(segment, .{})});
                    }
                    try writer.writeAll("],\"value\":");
                    try writer.writeAll(item.value_json);
                    try writer.writeByte('}');
                }
                try writer.writeByte(']');
            }
            if (clause.array_update.len > 0) {
                try writer.writeAll(",\"array_update\":[");
                for (clause.array_update, 0..) |item, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
                        std.json.fmt(item.field, .{}),
                        std.json.fmt(arrayTransformOpToken(item.op), .{}),
                    });
                    try writer.writeAll(item.value_json);
                    try writer.writeByte('}');
                }
                try writer.writeByte(']');
            }
            try writer.writeByte('}');
        }
        try self.writeReturningProjectionJson(writer, returning);
        try writer.writeAll("}]}");
        return try out.toOwnedSlice();
    }

    fn writeConflictTargetJson(self: *@This(), writer: *std.Io.Writer, target: ConflictTarget) !void {
        _ = self;
        switch (target) {
            .primary => try writer.writeAll("{\"primary\":true}"),
            .unique => |unique| {
                try writer.print("{{\"unique\":{{\"name\":{f}", .{std.json.fmt(unique.name, .{})});
                if (unique.where_json.len > 0) {
                    try writer.writeAll(",\"where\":");
                    try writer.writeAll(unique.where_json);
                }
                try writer.writeAll("}}");
            },
        }
    }

    fn parseU32Value(self: *@This()) !u32 {
        if (self.match(.number)) |token| {
            return try std.fmt.parseInt(u32, token.text, 10);
        }
        if (self.match(.placeholder)) |token| {
            const value = try self.boundValue(token);
            return try value.asU32();
        }
        return error.UnsupportedSqlShape;
    }

    fn parseSelectItem(self: *@This()) !SelectItem {
        if (self.peekKeyword("array_length")) return .{ .expression = try self.parseArrayLengthExpressionProjectionAlloc() };
        if (self.peekKeyword("coalesce")) return .{ .expression = try self.parseCoalesceExpressionProjectionAlloc() };
        if (self.peekKeyword("lower")) return .{ .expression = try self.parseLowerExpressionProjectionAlloc() };
        if (self.peekKeyword("concat")) return .{ .expression = try self.parseConcatExpressionProjectionAlloc() };
        if (self.peekKeyword("nullif")) return .{ .expression = try self.parseNullifExpressionProjectionAlloc() };
        if (self.peekKeyword("case")) return .{ .expression = try self.parseCaseExpressionProjectionAlloc() };
        if (self.peekKeyword("cast")) return .{ .expression = try self.parseCastExpressionProjectionAlloc() };

        const field = try self.parseFieldExpressionOwned();
        var field_owned = true;
        errdefer if (field_owned) self.alloc.free(field);
        if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
        if (self.match(.arrow_text) != null) {
            if (relationalColumnForField(self.schema, field, .json) == null) return error.InvalidSqlCatalog;
            const path = try self.parseJsonPathOwned();
            var path_owned = true;
            errdefer if (path_owned) self.alloc.free(path);
            const output = if (self.matchKeyword("as"))
                try self.parseIdentifierOwned()
            else
                try self.alloc.dupe(u8, path);
            var output_owned = true;
            errdefer if (output_owned) self.alloc.free(output);
            const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
            var operands_owned = true;
            errdefer if (operands_owned) self.alloc.free(operands);
            operands[0] = .{ .kind = .field, .field = field };
            field_owned = false;
            path_owned = false;
            output_owned = false;
            operands_owned = false;
            return .{ .expression = .{
                .output = output,
                .expression = .{
                    .kind = .json_extract,
                    .json_path = path,
                    .json_as_text = true,
                    .operands = operands,
                },
            } };
        }
        if (self.peekKind(.arrow_json)) return error.UnsupportedSqlShape;
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (self.peekArithmeticOperator()) |_| {
            if (column.field_type != .numeric) return error.InvalidSqlCatalog;
            field_owned = false;
            return .{ .expression = try self.parseArithmeticExpressionProjectionFromFieldAlloc(field) };
        }
        const alias = try self.parseOptionalProjectionAliasAlloc();
        var alias_transferred = false;
        errdefer if (!alias_transferred) if (alias) |owned| self.alloc.free(owned);
        if (alias) |output| {
            if (!std.mem.eql(u8, output, field)) {
                alias_transferred = true;
                field_owned = false;
                return .{ .expression = .{
                    .output = output,
                    .expression = .{
                        .kind = .field,
                        .field = field,
                    },
                } };
            }
            self.alloc.free(output);
            alias_transferred = true;
        }
        field_owned = false;
        return .{ .field = field };
    }

    fn parseArithmeticExpressionProjectionFromFieldAlloc(
        self: *@This(),
        lhs_field: []const u8,
    ) !db_mod.types.RelationalRowsExpressionProjection {
        var current: db_mod.types.RelationalRowsExpression = .{
            .kind = .field,
            .field = lhs_field,
        };
        var current_owned = true;
        errdefer if (current_owned) freeExpression(self.alloc, current);

        current_owned = false;
        current = try self.parseArithmeticExpressionRestAlloc(current, 0);
        current_owned = true;

        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, switch (current.kind) {
                .add => "add",
                .sub => "sub",
                .mul => "mul",
                .div => "div",
                else => "expr",
            });
        var output_owned = true;
        errdefer if (output_owned) self.alloc.free(output);

        current_owned = false;
        output_owned = false;
        return .{
            .output = output,
            .expression = current,
        };
    }

    const ArithmeticOperator = struct {
        token: TokenKind,
        kind: db_mod.types.RelationalRowsExpressionKind,
        precedence: u8,
    };

    fn peekArithmeticOperator(self: *@This()) ?ArithmeticOperator {
        if (self.peekKind(.plus)) return .{ .token = .plus, .kind = .add, .precedence = 1 };
        if (self.peekKind(.minus)) return .{ .token = .minus, .kind = .sub, .precedence = 1 };
        if (self.peekKind(.star)) return .{ .token = .star, .kind = .mul, .precedence = 2 };
        if (self.peekKind(.slash)) return .{ .token = .slash, .kind = .div, .precedence = 2 };
        return null;
    }

    fn parseArithmeticExpressionRestAlloc(
        self: *@This(),
        lhs: db_mod.types.RelationalRowsExpression,
        min_precedence: u8,
    ) anyerror!db_mod.types.RelationalRowsExpression {
        var current = lhs;
        var current_owned = true;
        errdefer if (current_owned) freeExpression(self.alloc, current);

        while (self.peekArithmeticOperator()) |op| {
            if (op.precedence < min_precedence) break;
            _ = self.match(op.token) orelse unreachable;
            var rhs = try self.parseRowExpressionOperandAlloc();
            var rhs_owned = true;
            errdefer if (rhs_owned) freeExpression(self.alloc, rhs);
            try self.validateNumericRowExpression(rhs);

            while (self.peekArithmeticOperator()) |next_op| {
                if (next_op.precedence <= op.precedence) break;
                rhs_owned = false;
                rhs = try self.parseArithmeticExpressionRestAlloc(rhs, next_op.precedence);
                rhs_owned = true;
            }

            const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
            var operands_owned = true;
            errdefer if (operands_owned) self.alloc.free(operands);
            operands[0] = current;
            operands[1] = rhs;
            current_owned = false;
            rhs_owned = false;
            operands_owned = false;
            current = .{
                .kind = op.kind,
                .operands = operands,
            };
            current_owned = true;
        }

        current_owned = false;
        return current;
    }

    fn validateNumericRowExpression(self: *@This(), expression: db_mod.types.RelationalRowsExpression) !void {
        switch (expression.kind) {
            .field => {
                const column = relationalColumnForField(self.schema, expression.field, null) orelse return error.InvalidSqlCatalog;
                if (column.field_type != .numeric) return error.InvalidSqlCatalog;
            },
            .value => {
                var parsed = std.json.parseFromSlice(std.json.Value, self.alloc, expression.value_json, .{}) catch return error.UnsupportedSqlShape;
                defer parsed.deinit();
                switch (parsed.value) {
                    .null, .integer, .float, .number_string => {},
                    else => return error.UnsupportedSqlShape,
                }
            },
            .nullif => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .add => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .mul => {
                if (expression.operands.len < 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .sub => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .div => {
                if (expression.operands.len != 2) return error.UnsupportedSqlShape;
                for (expression.operands) |operand| try self.validateNumericRowExpression(operand);
            },
            .case => {
                if (expression.case_branches.len == 0 or expression.case_else.len != 1) return error.UnsupportedSqlShape;
                for (expression.case_branches) |branch| try self.validateNumericRowExpression(branch.then);
                try self.validateNumericRowExpression(expression.case_else[0]);
            },
            .cast => {
                if (expression.operands.len != 1 or expression.cast_type != .numeric) return error.UnsupportedSqlShape;
            },
            else => return error.UnsupportedSqlShape,
        }
    }

    fn parseLowerExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        try self.expectKeyword("lower");
        try self.expect(.lparen);
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
        try self.expect(.rparen);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "lower");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);

        const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var operands_transferred = false;
        errdefer if (!operands_transferred) self.alloc.free(operands);
        operands[0] = .{
            .kind = .field,
            .field = field,
        };

        field_transferred = true;
        output_transferred = true;
        operands_transferred = true;
        return .{
            .output = output,
            .expression = .{
                .kind = .lower,
                .operands = operands,
            },
        };
    }

    fn parseConcatExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        try self.expectKeyword("concat");
        try self.expect(.lparen);

        var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
        errdefer {
            for (operands.items) |operand| freeExpression(self.alloc, operand);
            operands.deinit(self.alloc);
        }
        while (true) {
            const operand = try self.parseRowExpressionOperandAlloc();
            var operand_transferred = false;
            errdefer if (!operand_transferred) freeExpression(self.alloc, operand);
            try operands.append(self.alloc, operand);
            operand_transferred = true;
            if (self.match(.comma) == null) break;
        }
        if (operands.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);

        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "concat");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);
        const owned_operands = try operands.toOwnedSlice(self.alloc);
        var operands_transferred = false;
        errdefer if (!operands_transferred) {
            for (owned_operands) |operand| freeExpression(self.alloc, operand);
            self.alloc.free(owned_operands);
        };

        output_transferred = true;
        operands_transferred = true;
        return .{
            .output = output,
            .expression = .{
                .kind = .concat,
                .operands = owned_operands,
            },
        };
    }

    fn parseNullifExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        const expression = try self.parseNullifRowExpressionAlloc();
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(self.alloc, expression);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "nullif");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);

        expression_transferred = true;
        output_transferred = true;
        return .{
            .output = output,
            .expression = expression,
        };
    }

    fn parseCaseExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        const expression = try self.parseCaseRowExpressionAlloc();
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(self.alloc, expression);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "case");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);

        expression_transferred = true;
        output_transferred = true;
        return .{
            .output = output,
            .expression = expression,
        };
    }

    fn parseCastExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        const expression = try self.parseCastRowExpressionAlloc();
        var expression_transferred = false;
        errdefer if (!expression_transferred) freeExpression(self.alloc, expression);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "cast");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);

        expression_transferred = true;
        output_transferred = true;
        return .{
            .output = output,
            .expression = expression,
        };
    }

    fn parseRowExpressionOperandAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpression {
        if (self.peekKeyword("cast")) {
            return try self.parseCastRowExpressionAlloc();
        }
        if (self.peekKeyword("case")) {
            return try self.parseCaseRowExpressionAlloc();
        }
        if (self.peekKeyword("lower")) {
            return try self.parseLowerRowExpressionAlloc();
        }
        if (self.peekKeyword("nullif")) {
            return try self.parseNullifRowExpressionAlloc();
        }
        if (self.peekKind(.identifier) and
            !self.peekKeyword("null") and
            !self.peekKeyword("true") and
            !self.peekKeyword("false"))
        {
            const field = try self.parseFieldExpressionOwned();
            errdefer self.alloc.free(field);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            return .{ .kind = .field, .field = field };
        }

        const value_json = try self.parseJsonValueAlloc();
        errdefer self.alloc.free(value_json);
        return .{ .kind = .value, .value_json = value_json };
    }

    fn parseCastRowExpressionAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpression {
        try self.expectKeyword("cast");
        try self.expect(.lparen);
        const operand = try self.parseRowExpressionOperandAlloc();
        var operand_transferred = false;
        errdefer if (!operand_transferred) freeExpression(self.alloc, operand);
        try self.expectKeyword("as");
        const cast_type = try self.parseExpressionCastType();
        try self.expect(.rparen);

        const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var operands_transferred = false;
        errdefer if (!operands_transferred) self.alloc.free(operands);
        operands[0] = operand;

        operand_transferred = true;
        operands_transferred = true;
        return .{
            .kind = .cast,
            .operands = operands,
            .cast_type = cast_type,
        };
    }

    fn parseExpressionCastType(self: *@This()) !db_mod.types.RelationalRowsExpressionCastType {
        const token = self.match(.identifier) orelse return error.UnsupportedSqlShape;
        if (std.ascii.eqlIgnoreCase(token.text, "text")) return .text;
        if (std.ascii.eqlIgnoreCase(token.text, "numeric")) return .numeric;
        if (std.ascii.eqlIgnoreCase(token.text, "bool") or std.ascii.eqlIgnoreCase(token.text, "boolean")) return .bool;
        return error.UnsupportedSqlShape;
    }

    fn parseCaseRowExpressionAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpression {
        try self.expectKeyword("case");

        var branches = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpressionCaseBranch).empty;
        errdefer {
            for (branches.items) |branch| freeExpressionCaseBranch(self.alloc, branch);
            branches.deinit(self.alloc);
        }

        while (self.matchKeyword("when")) {
            const condition = try self.parseCaseExpressionConditionAlloc();
            var condition_transferred = false;
            errdefer if (!condition_transferred) freeExpressionCondition(self.alloc, condition);
            try self.expectKeyword("then");
            const then_expression = try self.parseRowExpressionOperandAlloc();
            var then_transferred = false;
            errdefer if (!then_transferred) freeExpression(self.alloc, then_expression);
            try branches.append(self.alloc, .{ .when = condition, .then = then_expression });
            condition_transferred = true;
            then_transferred = true;
        }
        if (branches.items.len == 0) return error.UnsupportedSqlShape;

        try self.expectKeyword("else");
        const else_expression = try self.parseRowExpressionOperandAlloc();
        var else_transferred = false;
        errdefer if (!else_transferred) freeExpression(self.alloc, else_expression);
        try self.expectKeyword("end");

        const owned_branches = try branches.toOwnedSlice(self.alloc);
        var branches_transferred = false;
        errdefer if (!branches_transferred) {
            for (owned_branches) |branch| freeExpressionCaseBranch(self.alloc, branch);
            self.alloc.free(owned_branches);
        };
        const fallback = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var fallback_transferred = false;
        errdefer if (!fallback_transferred) self.alloc.free(fallback);
        fallback[0] = else_expression;

        branches_transferred = true;
        fallback_transferred = true;
        else_transferred = true;
        return .{
            .kind = .case,
            .case_branches = owned_branches,
            .case_else = fallback,
        };
    }

    fn parseCaseExpressionConditionAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpressionCondition {
        const lhs = try self.parseRowExpressionOperandAlloc();
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(self.alloc, lhs);

        const op: runtime_schema.RelationalCheckOp = if (self.matchKeyword("is")) blk: {
            const not = self.matchKeyword("not");
            try self.expectKeyword("null");
            break :blk if (not) .is_not_null else .is_null;
        } else try self.parseComparisonOp();

        const rhs = switch (op) {
            .is_null, .is_not_null => &.{},
            else => blk: {
                const out = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
                var out_transferred = false;
                errdefer if (!out_transferred) self.alloc.free(out);
                out[0] = try self.parseRowExpressionOperandAlloc();
                out_transferred = true;
                break :blk out;
            },
        };

        lhs_transferred = true;
        return .{
            .lhs = lhs,
            .op = op,
            .rhs = rhs,
        };
    }

    fn parseLowerRowExpressionAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpression {
        try self.expectKeyword("lower");
        try self.expect(.lparen);
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, null) orelse return error.InvalidSqlCatalog;
        if (column.field_type != .keyword and column.field_type != .text and column.field_type != .link) return error.InvalidSqlCatalog;
        try self.expect(.rparen);

        const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var operands_transferred = false;
        errdefer if (!operands_transferred) self.alloc.free(operands);
        operands[0] = .{
            .kind = .field,
            .field = field,
        };
        field_transferred = true;
        operands_transferred = true;
        return .{
            .kind = .lower,
            .operands = operands,
        };
    }

    fn parseNullifRowExpressionAlloc(self: *@This()) anyerror!db_mod.types.RelationalRowsExpression {
        try self.expectKeyword("nullif");
        try self.expect(.lparen);
        const lhs = try self.parseRowExpressionOperandAlloc();
        var lhs_transferred = false;
        errdefer if (!lhs_transferred) freeExpression(self.alloc, lhs);
        try self.expect(.comma);
        const rhs = try self.parseRowExpressionOperandAlloc();
        var rhs_transferred = false;
        errdefer if (!rhs_transferred) freeExpression(self.alloc, rhs);
        try self.expect(.rparen);

        const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
        operands[0] = lhs;
        operands[1] = rhs;
        lhs_transferred = true;
        rhs_transferred = true;
        return .{
            .kind = .nullif,
            .operands = operands,
        };
    }

    fn parseArrayLengthProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsArrayLengthProjection {
        try self.expectKeyword("array_length");
        try self.expect(.lparen);
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, .array) orelse return error.InvalidSqlCatalog;
        _ = column;
        try self.expect(.comma);
        const dimension = try self.parseU32Value();
        if (dimension != 1) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "array_length");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);
        field_transferred = true;
        output_transferred = true;
        return .{ .output = output, .field = field };
    }

    fn parseArrayLengthExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        try self.expectKeyword("array_length");
        try self.expect(.lparen);
        const field = try self.parseIdentifierOwned();
        var field_transferred = false;
        errdefer if (!field_transferred) self.alloc.free(field);
        const column = relationalColumnForField(self.schema, field, .array) orelse return error.InvalidSqlCatalog;
        _ = column;
        try self.expect(.comma);
        const dimension = try self.parseU32Value();
        if (dimension != 1) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "array_length");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);
        const operands = try self.alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var operands_transferred = false;
        errdefer if (!operands_transferred) self.alloc.free(operands);
        operands[0] = .{ .kind = .field, .field = field };

        field_transferred = true;
        output_transferred = true;
        operands_transferred = true;
        return .{
            .output = output,
            .expression = .{
                .kind = .array_length,
                .operands = operands,
            },
        };
    }

    fn parseCoalesceProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsCoalesceProjection {
        try self.expectKeyword("coalesce");
        try self.expect(.lparen);
        var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsCoalesceOperand).empty;
        errdefer {
            for (operands.items) |operand| {
                switch (operand.kind) {
                    .field => if (operand.field.len > 0) self.alloc.free(operand.field),
                    .value => if (operand.value_json.len > 0) self.alloc.free(operand.value_json),
                }
            }
            operands.deinit(self.alloc);
        }
        while (true) {
            const operand = try self.parseCoalesceOperandAlloc();
            var operand_transferred = false;
            errdefer if (!operand_transferred) freeCoalesceOperand(self.alloc, operand);
            try operands.append(self.alloc, operand);
            operand_transferred = true;
            if (self.match(.comma) == null) break;
        }
        if (operands.items.len == 0) return error.UnsupportedSqlShape;
        try self.expect(.rparen);
        const output = if (self.matchKeyword("as"))
            try self.parseIdentifierOwned()
        else
            try self.alloc.dupe(u8, "coalesce");
        var output_transferred = false;
        errdefer if (!output_transferred) self.alloc.free(output);
        const owned_operands = try operands.toOwnedSlice(self.alloc);
        output_transferred = true;
        return .{ .output = output, .operands = owned_operands };
    }

    fn parseCoalesceExpressionProjectionAlloc(self: *@This()) !db_mod.types.RelationalRowsExpressionProjection {
        const projection = try self.parseCoalesceProjectionAlloc();
        defer freeCoalesceProjection(self.alloc, projection);
        return try expressionProjectionFromCoalesceAlloc(self.alloc, projection);
    }

    fn parseCoalesceOperandAlloc(self: *@This()) !db_mod.types.RelationalRowsCoalesceOperand {
        if (self.peekKind(.identifier) and
            !self.peekKeyword("null") and
            !self.peekKeyword("true") and
            !self.peekKeyword("false"))
        {
            const field = try self.parseFieldExpressionOwned();
            errdefer self.alloc.free(field);
            if (self.peekKind(.lparen)) return error.UnsupportedSqlShape;
            if (relationalColumnForField(self.schema, field, null) == null) return error.InvalidSqlCatalog;
            return .{ .kind = .field, .field = field };
        }

        const value_json = try self.parseJsonValueAlloc();
        errdefer self.alloc.free(value_json);
        return .{ .kind = .value, .value_json = value_json };
    }

    fn parseFieldExpressionOwned(self: *@This()) ![]const u8 {
        if (self.peekKeyword("lower") and self.pos + 1 < self.tokens.len and self.tokens[self.pos + 1].kind == .lparen) {
            self.pos += 1;
            try self.expect(.lparen);
            const source = try self.parseIdentifierOwned();
            defer self.alloc.free(source);
            if (relationalColumnForField(self.schema, source, null) == null) return error.InvalidSqlCatalog;
            try self.expect(.rparen);
            const generated = generatedLowerColumnForField(self.schema, source) orelse return error.UnsupportedSqlShape;
            return try self.alloc.dupe(u8, generated.name);
        }
        return try self.parseIdentifierOwned();
    }

    fn consumeCompatibleProjectionAlias(self: *@This(), field: []const u8) !void {
        const alias = (try self.parseOptionalProjectionAliasAlloc()) orelse return;
        defer self.alloc.free(alias);
        if (!std.mem.eql(u8, alias, field)) return error.UnsupportedSqlShape;
    }

    fn parseOptionalProjectionAliasAlloc(self: *@This()) !?[]const u8 {
        if (!self.matchKeyword("as")) return null;
        return try self.parseIdentifierOwned();
    }

    fn parseIdentifierOwned(self: *@This()) ![]const u8 {
        const token = self.match(.identifier) orelse return error.UnsupportedSqlShape;
        return try self.alloc.dupe(u8, token.text);
    }

    fn parseJsonPathOwned(self: *@This()) ![]const u8 {
        const token = self.match(.string) orelse return error.UnsupportedSqlShape;
        if (token.text.len == 0) return error.UnsupportedSqlShape;
        return try self.alloc.dupe(u8, token.text);
    }

    fn parsePostgresJsonPathAlloc(self: *@This()) ![]const []const u8 {
        const token = self.match(.string) orelse return error.UnsupportedSqlShape;
        if (token.text.len < 3 or token.text[0] != '{' or token.text[token.text.len - 1] != '}') return error.UnsupportedSqlShape;
        const inner = token.text[1 .. token.text.len - 1];
        var out = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (out.items) |segment| self.alloc.free(segment);
            out.deinit(self.alloc);
        }
        var parts = std.mem.splitScalar(u8, inner, ',');
        while (parts.next()) |part| {
            if (part.len == 0 or std.mem.indexOfScalar(u8, part, '.') != null) return error.UnsupportedSqlShape;
            const segment = try self.alloc.dupe(u8, part);
            var segment_transferred = false;
            errdefer if (!segment_transferred) self.alloc.free(segment);
            try out.append(self.alloc, segment);
            segment_transferred = true;
        }
        if (out.items.len == 0) return error.UnsupportedSqlShape;
        return try out.toOwnedSlice(self.alloc);
    }

    fn boundValueJsonAlloc(self: *@This(), token: Token) ![]const u8 {
        const value = try self.boundValue(token);
        return try value.jsonAlloc(self.alloc);
    }

    fn boundValue(self: *@This(), token: Token) !SqlValue {
        if (token.text.len < 2 or token.text[0] != '$') return error.UnsupportedSqlShape;
        var end: usize = 1;
        while (end < token.text.len and std.ascii.isDigit(token.text[end])) end += 1;
        if (end == 1) return error.UnsupportedSqlShape;
        const index = try std.fmt.parseInt(usize, token.text[1..end], 10);
        if (index == 0 or index > self.params.len) return error.MissingSqlParameter;
        return self.params[index - 1];
    }

    fn expectKeyword(self: *@This(), keyword: []const u8) !void {
        if (!self.matchKeyword(keyword)) return error.UnsupportedSqlShape;
    }

    fn expect(self: *@This(), kind: TokenKind) !void {
        if (self.match(kind) == null) return error.UnsupportedSqlShape;
    }

    fn matchKeyword(self: *@This(), keyword: []const u8) bool {
        if (self.pos >= self.tokens.len) return false;
        const token = self.tokens[self.pos];
        if (token.kind != .identifier) return false;
        if (!std.ascii.eqlIgnoreCase(token.text, keyword)) return false;
        self.pos += 1;
        return true;
    }

    fn peekKeyword(self: *@This(), keyword: []const u8) bool {
        if (self.pos >= self.tokens.len) return false;
        const token = self.tokens[self.pos];
        return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, keyword);
    }

    fn match(self: *@This(), kind: TokenKind) ?Token {
        if (self.pos >= self.tokens.len) return null;
        const token = self.tokens[self.pos];
        if (token.kind != kind) return null;
        self.pos += 1;
        return token;
    }

    fn peekKind(self: *@This(), kind: TokenKind) bool {
        return self.pos < self.tokens.len and self.tokens[self.pos].kind == kind;
    }

    fn atEnd(self: *@This()) bool {
        return self.pos >= self.tokens.len;
    }

    fn nextIsUnsupportedQueryKeyword(self: *@This()) bool {
        if (self.pos >= self.tokens.len or self.tokens[self.pos].kind != .identifier) return false;
        const token = self.tokens[self.pos].text;
        return std.ascii.eqlIgnoreCase(token, "join") or
            std.ascii.eqlIgnoreCase(token, "left") or
            std.ascii.eqlIgnoreCase(token, "inner") or
            std.ascii.eqlIgnoreCase(token, "group") or
            std.ascii.eqlIgnoreCase(token, "with") or
            std.ascii.eqlIgnoreCase(token, "over") or
            std.ascii.eqlIgnoreCase(token, "lateral");
    }

    fn whereHasTopLevelOr(self: *@This()) bool {
        var depth: usize = 0;
        var i = self.pos;
        while (i < self.tokens.len) : (i += 1) {
            const token = self.tokens[i];
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => if (depth > 0) {
                    depth -= 1;
                },
                .semicolon => if (depth == 0) return false,
                .identifier => if (depth == 0) {
                    if (std.ascii.eqlIgnoreCase(token.text, "or")) return true;
                    if (tokenStartsWhereTailClause(token.text)) return false;
                },
                else => {},
            }
        }
        return false;
    }
};

fn stripDdlPredicateOuterParens(raw_tokens: []const Token) []const Token {
    var tokens = raw_tokens;
    while (tokens.len >= 2 and tokens[0].kind == .lparen and tokens[tokens.len - 1].kind == .rparen) {
        var depth: usize = 0;
        var closes_at_end = false;
        for (tokens, 0..) |token, idx| {
            switch (token.kind) {
                .lparen => depth += 1,
                .rparen => {
                    if (depth == 0) return tokens;
                    depth -= 1;
                    if (depth == 0) {
                        closes_at_end = idx == tokens.len - 1;
                        break;
                    }
                },
                else => {},
            }
        }
        if (!closes_at_end) return tokens;
        tokens = tokens[1 .. tokens.len - 1];
    }
    return tokens;
}

fn parseDdlPredicateIdentifierOperand(tokens: []const Token, idx: *usize) !Token {
    var wrapped: usize = 0;
    while (idx.* < tokens.len and tokens[idx.*].kind == .lparen) {
        wrapped += 1;
        idx.* += 1;
    }
    if (idx.* >= tokens.len or tokens[idx.*].kind != .identifier) return error.UnsupportedSqlShape;
    const field_token = tokens[idx.*];
    idx.* += 1;
    while (wrapped > 0) {
        if (idx.* >= tokens.len or tokens[idx.*].kind != .rparen) return error.UnsupportedSqlShape;
        idx.* += 1;
        wrapped -= 1;
    }
    return field_token;
}

fn tokenStartsWhereTailClause(token: []const u8) bool {
    return std.ascii.eqlIgnoreCase(token, "order") or
        std.ascii.eqlIgnoreCase(token, "limit") or
        std.ascii.eqlIgnoreCase(token, "offset") or
        std.ascii.eqlIgnoreCase(token, "for") or
        std.ascii.eqlIgnoreCase(token, "group") or
        std.ascii.eqlIgnoreCase(token, "having") or
        std.ascii.eqlIgnoreCase(token, "join") or
        std.ascii.eqlIgnoreCase(token, "left") or
        std.ascii.eqlIgnoreCase(token, "inner") or
        std.ascii.eqlIgnoreCase(token, "with") or
        std.ascii.eqlIgnoreCase(token, "over") or
        std.ascii.eqlIgnoreCase(token, "lateral");
}

fn writeRelationalCheckAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const runtime_schema.RelationalCheck,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writeRelationalCheckAtomJson(writer, predicate);
        wrote_atom.* = true;
    }
}

fn writeRelationalCheckAtomJson(writer: *std.Io.Writer, predicate: runtime_schema.RelationalCheck) !void {
    try writer.print("{{\"field\":{f},\"op\":{f}", .{
        std.json.fmt(predicate.field, .{}),
        std.json.fmt(relationalCheckOpToken(predicate.op), .{}),
    });
    if (predicate.value_json) |value_json| {
        try writer.writeAll(",\"value\":");
        try writer.writeAll(value_json);
    }
    try writer.writeByte('}');
}

fn writeInPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsInPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(if (predicate.negated) "not_in" else "in", .{}),
        });
        try writer.writeAll(predicate.values_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

fn writeStructuredValuePredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    op_name: []const u8,
    predicates: anytype,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(op_name, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

fn writeJsonPathEqPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathEqPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_eq\",\"path\":{f},\"value\":", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        try writer.writeAll(predicate.value_json);
        try writer.writeByte('}');
        wrote_atom.* = true;
    }
}

fn writeJsonPathExistsPredicateAtomsJson(
    writer: *std.Io.Writer,
    wrote_atom: *bool,
    predicates: []const db_mod.types.RelationalRowsJsonPathExistsPredicate,
) !void {
    for (predicates) |predicate| {
        if (wrote_atom.*) try writer.writeByte(',');
        try writer.print("{{\"field\":{f},\"op\":\"json_path_exists\",\"path\":{f}}}", .{
            std.json.fmt(predicate.field, .{}),
            std.json.fmt(predicate.path, .{}),
        });
        wrote_atom.* = true;
    }
}

fn encodeSqlTxnIdHex(txn_id: db_mod.types.TxnId) [32]u8 {
    var out: [32]u8 = undefined;
    const hex = "0123456789abcdef";
    for (txn_id, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

fn findCteByName(ctes: []const db_mod.types.RelationalRowsCte, name: []const u8) ?db_mod.types.RelationalRowsCte {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

fn windowOutputContainsField(
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    field: []const u8,
) bool {
    for (fields) |candidate| {
        if (std.mem.eql(u8, candidate, field)) return true;
    }
    for (windows) |window| {
        if (std.mem.eql(u8, window.output, field)) return true;
    }
    return false;
}

fn tokenizeAlloc(alloc: std.mem.Allocator, sql: []const u8) !std.ArrayListUnmanaged(Token) {
    var tokens = std.ArrayListUnmanaged(Token).empty;
    errdefer freeTokens(alloc, &tokens);

    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (std.ascii.isWhitespace(ch)) {
            i += 1;
            continue;
        }
        if (std.ascii.isAlphabetic(ch) or ch == '_') {
            const start = i;
            i += 1;
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '.')) i += 1;
            const end = i;
            i = skipSqlCast(sql, i);
            try tokens.append(alloc, .{ .kind = .identifier, .text = sql[start..end] });
            continue;
        }
        if (ch == '"') {
            const start = i + 1;
            i += 1;
            while (i < sql.len and sql[i] != '"') i += 1;
            if (i >= sql.len) return error.UnsupportedSqlShape;
            try tokens.append(alloc, .{ .kind = .identifier, .text = sql[start..i] });
            i += 1;
            i = skipSqlCast(sql, i);
            continue;
        }
        if (ch == '\'') {
            var out = std.ArrayListUnmanaged(u8).empty;
            errdefer out.deinit(alloc);
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'') {
                    if (i + 1 < sql.len and sql[i + 1] == '\'') {
                        try out.append(alloc, '\'');
                        i += 2;
                        continue;
                    }
                    break;
                }
                try out.append(alloc, sql[i]);
                i += 1;
            }
            if (i >= sql.len) return error.UnsupportedSqlShape;
            const owned = try out.toOwnedSlice(alloc);
            errdefer alloc.free(owned);
            i += 1;
            i = skipSqlCast(sql, i);
            try tokens.append(alloc, .{ .kind = .string, .text = owned });
            continue;
        }
        if (std.ascii.isDigit(ch)) {
            const start = i;
            i += 1;
            while (i < sql.len and (std.ascii.isDigit(sql[i]) or sql[i] == '.')) i += 1;
            try tokens.append(alloc, .{ .kind = .number, .text = sql[start..i] });
            continue;
        }
        if (ch == '$') {
            const start = i;
            i += 1;
            while (i < sql.len and std.ascii.isDigit(sql[i])) i += 1;
            if (i == start + 1) return error.UnsupportedSqlShape;
            if (i + 1 < sql.len and sql[i] == ':' and sql[i + 1] == ':') {
                i += 2;
                while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '[' or sql[i] == ']')) i += 1;
            }
            try tokens.append(alloc, .{ .kind = .placeholder, .text = sql[start..i] });
            continue;
        }
        switch (ch) {
            ',' => {
                try tokens.append(alloc, .{ .kind = .comma, .text = sql[i .. i + 1] });
                i += 1;
            },
            '*' => {
                try tokens.append(alloc, .{ .kind = .star, .text = sql[i .. i + 1] });
                i += 1;
            },
            '+' => {
                try tokens.append(alloc, .{ .kind = .plus, .text = sql[i .. i + 1] });
                i += 1;
            },
            '/' => {
                try tokens.append(alloc, .{ .kind = .slash, .text = sql[i .. i + 1] });
                i += 1;
            },
            '(' => {
                try tokens.append(alloc, .{ .kind = .lparen, .text = sql[i .. i + 1] });
                i += 1;
            },
            ')' => {
                try tokens.append(alloc, .{ .kind = .rparen, .text = sql[i .. i + 1] });
                i += 1;
                i = skipSqlCast(sql, i);
            },
            '[' => {
                try tokens.append(alloc, .{ .kind = .lbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            ']' => {
                try tokens.append(alloc, .{ .kind = .rbracket, .text = sql[i .. i + 1] });
                i += 1;
            },
            '@' => {
                if (i + 1 >= sql.len or sql[i + 1] != '>') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .at_contains, .text = sql[i .. i + 2] });
                i += 2;
            },
            '|' => {
                if (i + 1 >= sql.len or sql[i + 1] != '|') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .pipe_concat, .text = sql[i .. i + 2] });
                i += 2;
            },
            '?' => {
                try tokens.append(alloc, .{ .kind = .question, .text = sql[i .. i + 1] });
                i += 1;
            },
            ';' => {
                try tokens.append(alloc, .{ .kind = .semicolon, .text = sql[i .. i + 1] });
                i += 1;
            },
            '=' => {
                try tokens.append(alloc, .{ .kind = .eq, .text = sql[i .. i + 1] });
                i += 1;
            },
            '!' => {
                if (i + 1 >= sql.len or sql[i + 1] != '=') return error.UnsupportedSqlShape;
                try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                i += 2;
            },
            '<' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .lte, .text = sql[i .. i + 2] });
                    i += 2;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .neq, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .lt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '>' => {
                if (i + 1 < sql.len and sql[i + 1] == '=') {
                    try tokens.append(alloc, .{ .kind = .gte, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .gt, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            '-' => {
                if (i + 1 < sql.len and sql[i + 1] == '>' and i + 2 < sql.len and sql[i + 2] == '>') {
                    try tokens.append(alloc, .{ .kind = .arrow_text, .text = sql[i .. i + 3] });
                    i += 3;
                } else if (i + 1 < sql.len and sql[i + 1] == '>') {
                    try tokens.append(alloc, .{ .kind = .arrow_json, .text = sql[i .. i + 2] });
                    i += 2;
                } else {
                    try tokens.append(alloc, .{ .kind = .minus, .text = sql[i .. i + 1] });
                    i += 1;
                }
            },
            else => return error.UnsupportedSqlShape,
        }
    }
    return tokens;
}

fn skipSqlCast(sql: []const u8, start: usize) usize {
    var i = start;
    if (i + 1 >= sql.len or sql[i] != ':' or sql[i + 1] != ':') return i;
    i += 2;
    while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '[' or sql[i] == ']')) i += 1;
    return i;
}

fn freeTokens(alloc: std.mem.Allocator, tokens: *std.ArrayListUnmanaged(Token)) void {
    for (tokens.items) |token| {
        if (token.kind == .string) alloc.free(token.text);
    }
    tokens.deinit(alloc);
}

fn relationalColumnForField(schema: runtime_schema.TableSchema, field: []const u8, expected_type: ?runtime_schema.AntflyType) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (!std.mem.eql(u8, column.name, field)) continue;
        if (expected_type) |field_type| {
            if (column.field_type != field_type) return null;
        }
        return column;
    }
    return null;
}

fn generatedLowerColumnForField(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        const generated = column.generated orelse continue;
        if (generated.op != .lower) continue;
        const generated_field = generated.field orelse continue;
        if (std.mem.eql(u8, generated_field, field)) return column;
    }
    return null;
}

fn relationalColumnForReturningField(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    if (relationalColumnForField(schema, field, null)) |column| return column;
    const dot_index = std.mem.indexOfScalar(u8, field, '.') orelse return null;
    if (dot_index == 0 or dot_index + 1 >= field.len) return null;
    return relationalColumnForField(schema, field[0..dot_index], .json);
}

fn findUniqueConstraintByColumns(schema: runtime_schema.TableSchema, columns: []const []const u8, require_partial: bool) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.expressions.len != 0) continue;
        if (require_partial and constraint.where.len == 0) continue;
        if (!require_partial and constraint.where.len != 0) continue;
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn findUniqueConstraintByLowerExpression(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.UniqueConstraint {
    for (schema.unique_constraints) |constraint| {
        if (constraint.columns.len != 0 or constraint.expressions.len != 1) continue;
        const expression = constraint.expressions[0];
        if (expression.op == .lower and std.mem.eql(u8, expression.field, field)) return constraint;
    }
    return null;
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn columnsMatchPrimaryKey(primary_key: runtime_schema.PrimaryKey, columns: []const []const u8) bool {
    return stringSlicesEqual(primary_key.columns, columns);
}

fn primaryKeyContains(primary_key: runtime_schema.PrimaryKey, field: []const u8) bool {
    for (primary_key.columns) |column| {
        if (std.mem.eql(u8, column, field)) return true;
    }
    return false;
}

fn aggregateOpForName(name: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "array_agg")) return .array_agg;
    return null;
}

fn aggregateOpName(op: db_mod.types.RelationalRowsAggregateOp) []const u8 {
    return switch (op) {
        .count => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .array_agg => "array_agg",
    };
}

fn validateAggregateGroupBy(group_fields: []const []const u8, group_by: []const []const u8) !void {
    if (!stringSlicesEqual(group_fields, group_by)) return error.UnsupportedSqlShape;
}

fn aggregateOutputContainsField(
    group_fields: []const []const u8,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) bool {
    for (group_fields) |group_field| {
        if (std.mem.eql(u8, group_field, field)) return true;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, field)) return true;
    }
    return false;
}

fn joinSideForQualifier(
    qualifier: []const u8,
    left_alias: []const u8,
    right_alias: []const u8,
) !db_mod.types.RelationalRowsJoinProjectionSide {
    if (std.mem.eql(u8, qualifier, left_alias)) return .left;
    if (std.mem.eql(u8, qualifier, right_alias)) return .right;
    return error.UnsupportedSqlShape;
}

fn joinProjectionContainsOutput(select: []const db_mod.types.RelationalRowsJoinProjection, field: []const u8) bool {
    for (select) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return true;
    }
    return false;
}

fn identifierHasQualifier(identifier: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, identifier, '.') orelse return false;
    return dot > 0 and dot + 1 < identifier.len;
}

fn isSupportedUpdatedAtTriggerFunction(name: []const u8) bool {
    const dot = std.mem.lastIndexOfScalar(u8, name, '.');
    const base = if (dot) |index| name[index + 1 ..] else name;
    return std.ascii.eqlIgnoreCase(base, "touch_updated_at") or
        std.ascii.eqlIgnoreCase(base, "set_updated_at") or
        std.ascii.eqlIgnoreCase(base, "update_updated_at") or
        std.ascii.eqlIgnoreCase(base, "antfly_on_update_now") or
        std.ascii.eqlIgnoreCase(base, "antfly_touch_updated_at");
}

fn findDdlColumn(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?runtime_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn ddlBaseTypeForName(name: []const u8) ?runtime_schema.AntflyType {
    if (std.ascii.eqlIgnoreCase(name, "uuid")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "text")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "varchar")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "citext")) return .keyword;
    if (std.ascii.eqlIgnoreCase(name, "integer")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "int")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "int4")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "bigint")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "int8")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "smallint")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "numeric")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "decimal")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "real")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "float4")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "float8")) return .numeric;
    if (std.ascii.eqlIgnoreCase(name, "boolean")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "bool")) return .boolean;
    if (std.ascii.eqlIgnoreCase(name, "date")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "timestamptz")) return .datetime;
    if (std.ascii.eqlIgnoreCase(name, "json")) return .json;
    if (std.ascii.eqlIgnoreCase(name, "jsonb")) return .json;
    if (std.ascii.eqlIgnoreCase(name, "bytea")) return .blob;
    return null;
}

fn relationalCheckOpToken(op: runtime_schema.RelationalCheckOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
        .gt => "gt",
        .gte => "gte",
        .lt => "lt",
        .lte => "lte",
    };
}

fn schemaJsonValueFromCreateTablePlanAlloc(alloc: std.mem.Allocator, plan: CreateTablePlan) !std.json.Value {
    var properties = std.json.ObjectMap.empty;
    for (plan.columns) |column| {
        try properties.put(alloc, try alloc.dupe(u8, column.name), try schemaJsonPropertyFromColumnAlloc(alloc, column));
    }

    var required = std.json.Array.init(alloc);
    for (plan.columns) |column| {
        if (!column.nullable) try required.append(.{ .string = try alloc.dupe(u8, column.name) });
    }

    var row_schema = std.json.ObjectMap.empty;
    try putJsonString(alloc, &row_schema, "type", "object");
    try row_schema.put(alloc, try alloc.dupe(u8, "properties"), .{ .object = properties });
    if (required.items.len > 0) try row_schema.put(alloc, try alloc.dupe(u8, "required"), .{ .array = required });
    try row_schema.put(alloc, try alloc.dupe(u8, "additionalProperties"), .{ .bool = false });

    var document_schema = std.json.ObjectMap.empty;
    try document_schema.put(alloc, try alloc.dupe(u8, "schema"), .{ .object = row_schema });

    var document_schemas = std.json.ObjectMap.empty;
    try document_schemas.put(alloc, try alloc.dupe(u8, "row"), .{ .object = document_schema });

    var root = std.json.ObjectMap.empty;
    try putJsonString(alloc, &root, "storage_mode", "relational");
    try putJsonString(alloc, &root, "default_type", "row");
    try root.put(alloc, try alloc.dupe(u8, "enforce_types"), .{ .bool = true });
    try root.put(alloc, try alloc.dupe(u8, "document_schemas"), .{ .object = document_schemas });
    if (plan.primary_key) |primary_key| try root.put(alloc, try alloc.dupe(u8, "primary_key"), try schemaJsonPrimaryKeyAlloc(alloc, primary_key));
    if (plan.unique_constraints.len > 0) try root.put(alloc, try alloc.dupe(u8, "unique_constraints"), try schemaJsonUniqueConstraintsAlloc(alloc, plan.unique_constraints));
    if (plan.foreign_keys.len > 0) try root.put(alloc, try alloc.dupe(u8, "foreign_keys"), try schemaJsonForeignKeysAlloc(alloc, plan.foreign_keys));
    if (plan.checks.len > 0) try root.put(alloc, try alloc.dupe(u8, "checks"), try schemaJsonRelationalChecksAlloc(alloc, plan.checks));
    return .{ .object = root };
}

fn applyCreateIndexPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: CreateIndexPlan,
) !void {
    const schema_parts = try relationalSchemaJsonParts(root);
    if (plan.unique) {
        const constraint: runtime_schema.UniqueConstraint = .{
            .name = plan.index_name,
            .columns = plan.columns,
            .expressions = plan.expressions,
            .where = plan.where,
            .validation_state = .unvalidated,
        };
        var constraints = try rootArrayFieldAlloc(alloc, root, "unique_constraints");
        try constraints.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
        return;
    }

    const index_generation = stableSecondaryIndexGeneration(plan);
    if (plan.expressions.len == 1 and plan.expressions[0].op == .lower and plan.columns.len == 0) {
        if (schema_parts.properties.get(plan.index_name) != null) return error.InvalidSqlCatalog;
        if (schema_parts.properties.get(plan.expressions[0].field) == null) return error.InvalidSqlCatalog;
        const generated: runtime_schema.RelationalGeneratedValue = .{ .op = .lower, .field = plan.expressions[0].field };
        const column: runtime_schema.RelationalColumn = .{
            .name = plan.index_name,
            .path = plan.index_name,
            .field_type = .keyword,
            .nullable = true,
            .indexed = true,
            .index_lifecycle = .building,
            .index_generation = index_generation,
            .generated = generated,
            .index_where = plan.where,
        };
        try schema_parts.properties.put(alloc, try alloc.dupe(u8, plan.index_name), try schemaJsonPropertyFromColumnAlloc(alloc, column));
        return;
    }

    if (plan.columns.len != 1 or plan.expressions.len != 0) return error.UnsupportedSqlShape;
    const property = schema_parts.properties.getPtr(plan.columns[0]) orelse return error.InvalidSqlCatalog;
    if (property.* != .object) return error.InvalidSqlCatalog;
    try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index"), .{ .bool = true });
    try putJsonString(alloc, &property.object, "x-antfly-index-lifecycle", "building");
    try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-generation"), .{ .integer = @intCast(index_generation) });
    if (plan.where.len > 0) try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-index-where"), try schemaJsonUniquePredicateDefinitionAlloc(alloc, plan.where));
}

fn applyAlterTablePlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: AlterTablePlan,
) !void {
    const schema_parts = try relationalSchemaJsonParts(root);
    for (plan.operations) |operation| {
        switch (operation) {
            .add_column => |column| {
                if (schema_parts.properties.get(column.name) != null) return error.InvalidSqlCatalog;
                try schema_parts.properties.put(alloc, try alloc.dupe(u8, column.name), try schemaJsonPropertyFromColumnAlloc(alloc, column));
                if (!column.nullable) {
                    var required = try rootArrayFieldAlloc(alloc, schema_parts.schema, "required");
                    try required.append(.{ .string = try alloc.dupe(u8, column.name) });
                }
            },
            .add_unique_constraint => |constraint| {
                var constraints = try rootArrayFieldAlloc(alloc, root, "unique_constraints");
                try constraints.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
            },
            .add_foreign_key => |foreign_key| {
                var foreign_keys = try rootArrayFieldAlloc(alloc, root, "foreign_keys");
                try foreign_keys.append(try schemaJsonForeignKeyAlloc(alloc, foreign_key));
            },
            .add_check => |check| {
                var checks = try rootArrayFieldAlloc(alloc, root, "checks");
                try checks.append(try schemaJsonRelationalCheckAlloc(alloc, check));
            },
            .validate_constraint => |constraint_name| try validateConstraintByNameInSchemaJson(alloc, root, constraint_name),
        }
    }
}

fn applyCreateUpdatePolicyPlanToSchemaJsonValue(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    plan: CreateUpdatePolicyPlan,
) !void {
    const schema_parts = try relationalSchemaJsonParts(root);
    const property = schema_parts.properties.getPtr(plan.column_name) orelse return error.InvalidSqlCatalog;
    if (property.* != .object) return error.InvalidSqlCatalog;
    try property.object.put(alloc, try alloc.dupe(u8, "x-antfly-on-update"), try schemaJsonDefaultValueAlloc(alloc, plan.on_update_value, true));
}

fn validateConstraintByNameInSchemaJson(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    constraint_name: []const u8,
) !void {
    if (try setNamedConstraintValidationStateInArray(alloc, root, "unique_constraints", constraint_name, "enforced")) return;
    if (try setNamedConstraintValidationStateInArray(alloc, root, "foreign_keys", constraint_name, "enforced")) return;
    if (try setNamedConstraintValidationStateInArray(alloc, root, "checks", constraint_name, "enforced")) return;
    return error.InvalidSqlCatalog;
}

fn setNamedConstraintValidationStateInArray(
    alloc: std.mem.Allocator,
    root: *std.json.ObjectMap,
    field: []const u8,
    constraint_name: []const u8,
    validation_state: []const u8,
) !bool {
    const value = root.getPtr(field) orelse return false;
    if (value.* != .array) return error.InvalidSqlCatalog;
    for (value.array.items) |*item| {
        if (item.* != .object) return error.InvalidSqlCatalog;
        const name = item.object.get("name") orelse return error.InvalidSqlCatalog;
        if (name != .string) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;
        try putJsonString(alloc, &item.object, "validation_state", validation_state);
        return true;
    }
    return false;
}

const RelationalSchemaJsonParts = struct {
    schema: *std.json.ObjectMap,
    properties: *std.json.ObjectMap,
};

fn relationalSchemaJsonParts(root: *std.json.ObjectMap) !RelationalSchemaJsonParts {
    const storage_mode = root.get("storage_mode") orelse return error.InvalidSqlCatalog;
    if (storage_mode != .string or !std.mem.eql(u8, storage_mode.string, "relational")) return error.InvalidSqlCatalog;
    const default_type = root.get("default_type") orelse return error.InvalidSqlCatalog;
    if (default_type != .string or default_type.string.len == 0) return error.InvalidSqlCatalog;
    const document_schemas = root.getPtr("document_schemas") orelse return error.InvalidSqlCatalog;
    if (document_schemas.* != .object) return error.InvalidSqlCatalog;
    const document_schema = document_schemas.object.getPtr(default_type.string) orelse return error.InvalidSqlCatalog;
    if (document_schema.* != .object) return error.InvalidSqlCatalog;
    const schema = document_schema.object.getPtr("schema") orelse return error.InvalidSqlCatalog;
    if (schema.* != .object) return error.InvalidSqlCatalog;
    const properties = schema.object.getPtr("properties") orelse return error.InvalidSqlCatalog;
    if (properties.* != .object) return error.InvalidSqlCatalog;
    return .{ .schema = &schema.object, .properties = &properties.object };
}

fn schemaJsonPropertyFromColumnAlloc(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "type", antflyTypeSchemaName(column.field_type));
    if (column.field_type == .array) {
        const item_type = column.array_item_type orelse return error.InvalidSqlCatalog;
        var item_object = std.json.ObjectMap.empty;
        try putJsonString(alloc, &item_object, "type", antflyTypeSchemaName(item_type));
        try object.put(alloc, try alloc.dupe(u8, "items"), .{ .object = item_object });
    }
    if (!column.indexed) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index"), .{ .bool = false });
    if (column.index_lifecycle != .ready) try putJsonString(alloc, &object, "x-antfly-index-lifecycle", relationalIndexLifecycleName(column.index_lifecycle));
    if (column.index_generation != 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-generation"), .{ .integer = @intCast(column.index_generation) });
    if (column.index_where.len > 0) try object.put(alloc, try alloc.dupe(u8, "x-antfly-index-where"), try schemaJsonUniquePredicateDefinitionAlloc(alloc, column.index_where));
    if (column.default_value) |value| {
        const key = if (value.kind == .literal) "default" else "x-antfly-default";
        try object.put(alloc, try alloc.dupe(u8, key), try schemaJsonDefaultValueAlloc(alloc, value, value.kind != .literal));
    }
    if (column.on_update_value) |value| try object.put(alloc, try alloc.dupe(u8, "x-antfly-on-update"), try schemaJsonDefaultValueAlloc(alloc, value, true));
    if (column.generated) |generated| try object.put(alloc, try alloc.dupe(u8, "generated"), try schemaJsonGeneratedValueAlloc(alloc, generated));
    return .{ .object = object };
}

fn schemaJsonDefaultValueAlloc(alloc: std.mem.Allocator, value: runtime_schema.RelationalDefaultValue, force_server_default: bool) !std.json.Value {
    if (!force_server_default and value.kind == .literal) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value.value_json, .{});
        defer parsed.deinit();
        return try json_helpers.cloneJsonValue(alloc, parsed.value);
    }
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "op", switch (value.kind) {
        .literal => return error.InvalidSqlCatalog,
        .now_ns => "now_ns",
        .uuid_v4 => "uuid_v4",
    });
    return .{ .object = object };
}

fn schemaJsonGeneratedValueAlloc(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "op", switch (generated.op) {
        .lower => "lower",
        .concat => "concat",
    });
    switch (generated.op) {
        .lower => try putJsonString(alloc, &object, "field", generated.field orelse return error.InvalidSqlCatalog),
        .concat => {
            try object.put(alloc, try alloc.dupe(u8, "fields"), try schemaJsonStringArrayAlloc(alloc, generated.fields));
            try putJsonString(alloc, &object, "separator", generated.separator);
        },
    }
    return .{ .object = object };
}

fn schemaJsonPrimaryKeyAlloc(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try object.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, primary_key.columns));
    return .{ .object = object };
}

fn schemaJsonUniqueConstraintsAlloc(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (constraints) |constraint| try array.append(try schemaJsonUniqueConstraintAlloc(alloc, constraint));
    return .{ .array = array };
}

fn schemaJsonUniqueConstraintAlloc(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", constraint.name);
    if (constraint.columns.len > 0) try object.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, constraint.columns));
    if (constraint.expressions.len > 0) try object.put(alloc, try alloc.dupe(u8, "expressions"), try schemaJsonUniqueExpressionsAlloc(alloc, constraint.expressions));
    if (constraint.where.len > 0) try object.put(alloc, try alloc.dupe(u8, "where"), try schemaJsonUniquePredicateDefinitionAlloc(alloc, constraint.where));
    if (constraint.validation_state != .enforced) try putJsonString(alloc, &object, "validation_state", uniqueConstraintValidationStateString(constraint.validation_state));
    return .{ .object = object };
}

fn uniqueConstraintValidationStateString(state: runtime_schema.UniqueConstraintValidationState) []const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating => "validating",
        .invalid => "invalid",
    };
}

fn schemaJsonUniqueExpressionsAlloc(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (expressions) |expression| {
        var object = std.json.ObjectMap.empty;
        try putJsonString(alloc, &object, "op", switch (expression.op) {
            .lower => "lower",
        });
        try putJsonString(alloc, &object, "field", expression.field);
        try array.append(.{ .object = object });
    }
    return .{ .array = array };
}

fn schemaJsonUniquePredicateDefinitionAlloc(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    var array = std.json.Array.init(alloc);
    for (predicates) |predicate| try array.append(try schemaJsonUniquePredicateAlloc(alloc, predicate));
    try object.put(alloc, try alloc.dupe(u8, "all"), .{ .array = array });
    return .{ .object = object };
}

fn schemaJsonUniquePredicateAlloc(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "field", predicate.field);
    try putJsonString(alloc, &object, "op", uniquePredicateOpToken(predicate.op));
    if (predicate.value_json) |value_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        try object.put(alloc, try alloc.dupe(u8, "value"), try json_helpers.cloneJsonValue(alloc, parsed.value));
    }
    return .{ .object = object };
}

fn schemaJsonForeignKeysAlloc(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (foreign_keys) |foreign_key| try array.append(try schemaJsonForeignKeyAlloc(alloc, foreign_key));
    return .{ .array = array };
}

fn schemaJsonForeignKeyAlloc(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) !std.json.Value {
    var reference = std.json.ObjectMap.empty;
    try putJsonString(alloc, &reference, "table", foreign_key.parent_table);
    try reference.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, foreign_key.parent_columns));

    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", foreign_key.name);
    try object.put(alloc, try alloc.dupe(u8, "columns"), try schemaJsonStringArrayAlloc(alloc, foreign_key.child_columns));
    try object.put(alloc, try alloc.dupe(u8, "references"), .{ .object = reference });
    try putJsonString(alloc, &object, "on_delete", foreignKeyActionName(foreign_key.on_delete));
    try putJsonString(alloc, &object, "on_update", foreignKeyActionName(foreign_key.on_update));
    try putJsonString(alloc, &object, "timing", foreignKeyTimingName(foreign_key.timing));
    try object.put(alloc, try alloc.dupe(u8, "deferrable"), .{ .bool = foreign_key.deferrable });
    try putJsonString(alloc, &object, "match", foreignKeyMatchName(foreign_key.match));
    try putJsonString(alloc, &object, "validation_state", foreignKeyValidationStateName(foreign_key.validation_state));
    return .{ .object = object };
}

fn schemaJsonRelationalChecksAlloc(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (checks) |check| try array.append(try schemaJsonRelationalCheckAlloc(alloc, check));
    return .{ .array = array };
}

fn schemaJsonRelationalCheckAlloc(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    try putJsonString(alloc, &object, "name", check.name);
    try putJsonString(alloc, &object, "field", check.field);
    try putJsonString(alloc, &object, "op", relationalCheckOpToken(check.op));
    if (check.validation_state != .enforced) try putJsonString(alloc, &object, "validation_state", relationalCheckValidationStateName(check.validation_state));
    if (check.value_json) |value_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, value_json, .{});
        defer parsed.deinit();
        try object.put(alloc, try alloc.dupe(u8, "value"), try json_helpers.cloneJsonValue(alloc, parsed.value));
    }
    return .{ .object = object };
}

fn relationalCheckValidationStateName(state: runtime_schema.RelationalCheckValidationState) []const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating => "validating",
        .invalid => "invalid",
    };
}

fn schemaJsonStringArrayAlloc(alloc: std.mem.Allocator, values: []const []const u8) !std.json.Value {
    var array = std.json.Array.init(alloc);
    for (values) |value| try array.append(.{ .string = try alloc.dupe(u8, value) });
    return .{ .array = array };
}

fn rootArrayFieldAlloc(alloc: std.mem.Allocator, object: *std.json.ObjectMap, field: []const u8) !*std.json.Array {
    const entry = try object.getOrPut(alloc, field);
    if (!entry.found_existing) {
        entry.key_ptr.* = try alloc.dupe(u8, field);
        entry.value_ptr.* = .{ .array = std.json.Array.init(alloc) };
    }
    if (entry.value_ptr.* != .array) return error.InvalidSqlCatalog;
    return &entry.value_ptr.array;
}

fn putJsonString(alloc: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
}

fn antflyTypeSchemaName(field_type: runtime_schema.AntflyType) []const u8 {
    return switch (field_type) {
        .text => "text",
        .keyword => "keyword",
        .numeric => "numeric",
        .embedding => "embedding",
        .boolean => "boolean",
        .datetime => "datetime",
        .geopoint => "geopoint",
        .geoshape => "geoshape",
        .blob => "blob",
        .html => "html",
        .search_as_you_type => "search_as_you_type",
        .json => "json",
        .array => "array",
        .link => "link",
    };
}

fn foreignKeyActionName(action: runtime_schema.ForeignKeyAction) []const u8 {
    return switch (action) {
        .restrict => "restrict",
        .set_null => "set_null",
        .cascade => "cascade",
        .no_action => "no_action",
    };
}

fn foreignKeyTimingName(timing: runtime_schema.ForeignKeyTiming) []const u8 {
    return switch (timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
}

fn foreignKeyMatchName(match: runtime_schema.ForeignKeyMatch) []const u8 {
    return switch (match) {
        .simple => "simple",
        .full => "full",
        .partial => "partial",
    };
}

fn foreignKeyValidationStateName(state: runtime_schema.ForeignKeyValidationState) []const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating => "validating",
        .invalid => "invalid",
    };
}

fn validateDdlAppliedSchemaJsonAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !void {
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, runtime);
    if (runtime.storage_mode != .relational) return error.InvalidSqlCatalog;
}

fn cloneStringSlice(alloc: std.mem.Allocator, values: []const []const u8) ![]const []const u8 {
    if (values.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |value| alloc.free(value);
        alloc.free(out);
    }
    for (values, 0..) |value, i| {
        out[i] = try alloc.dupe(u8, value);
        initialized += 1;
    }
    return out;
}

fn cloneRelationalRuntimeSchemaAlloc(alloc: std.mem.Allocator, current: runtime_schema.TableSchema) !runtime_schema.TableSchema {
    if (current.storage_mode != .relational) return error.InvalidSqlCatalog;
    if (current.dynamic_templates.len != 0 or current.full_text_documents.len != 0) return error.UnsupportedSqlShape;

    const default_type = try alloc.dupe(u8, current.default_type);
    const ttl_field = alloc.dupe(u8, current.ttl_field) catch |err| {
        alloc.free(default_type);
        return err;
    };
    var schema: runtime_schema.TableSchema = .{
        .version = current.version,
        .default_type = default_type,
        .ttl_duration_ns = current.ttl_duration_ns,
        .ttl_field = ttl_field,
        .enforce_types = current.enforce_types,
        .storage_mode = current.storage_mode,
    };
    errdefer runtime_schema.freeSchema(alloc, schema);
    schema.relational_columns = try cloneDdlRelationalColumns(alloc, current.relational_columns);
    schema.primary_key = try cloneDdlPrimaryKeyMaybe(alloc, current.primary_key);
    schema.foreign_keys = try cloneDdlForeignKeys(alloc, current.foreign_keys);
    schema.unique_constraints = try cloneDdlUniqueConstraints(alloc, current.unique_constraints);
    schema.checks = try cloneDdlRelationalChecks(alloc, current.checks);
    return schema;
}

fn cloneDdlRelationalColumn(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) !runtime_schema.RelationalColumn {
    const name = try alloc.dupe(u8, column.name);
    const path = alloc.dupe(u8, column.path) catch |err| {
        alloc.free(name);
        return err;
    };
    var out: runtime_schema.RelationalColumn = .{
        .name = name,
        .path = path,
        .field_type = column.field_type,
        .array_item_type = column.array_item_type,
        .nullable = column.nullable,
        .indexed = column.indexed,
        .index_lifecycle = column.index_lifecycle,
        .index_generation = column.index_generation,
    };
    errdefer freeDdlRelationalColumn(alloc, out);
    out.default_value = if (column.default_value) |value| try cloneDdlDefaultValue(alloc, value) else null;
    out.on_update_value = if (column.on_update_value) |value| try cloneDdlDefaultValue(alloc, value) else null;
    out.generated = if (column.generated) |generated| try cloneDdlGeneratedValue(alloc, generated) else null;
    out.index_where = try cloneDdlUniquePredicates(alloc, column.index_where);
    return out;
}

fn cloneDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) ![]const runtime_schema.RelationalColumn {
    if (columns.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, columns.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |column| freeDdlRelationalColumn(alloc, column);
        alloc.free(out);
    }
    for (columns, 0..) |column, i| {
        out[i] = try cloneDdlRelationalColumn(alloc, column);
        initialized += 1;
    }
    return out;
}

fn cloneDdlDefaultValue(alloc: std.mem.Allocator, value: runtime_schema.RelationalDefaultValue) !runtime_schema.RelationalDefaultValue {
    return .{
        .kind = value.kind,
        .value_json = try alloc.dupe(u8, value.value_json),
    };
}

fn cloneDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) !runtime_schema.RelationalGeneratedValue {
    var out: runtime_schema.RelationalGeneratedValue = .{
        .op = generated.op,
        .separator = try alloc.dupe(u8, generated.separator),
    };
    errdefer freeDdlGeneratedValue(alloc, out);
    out.field = if (generated.field) |field| try alloc.dupe(u8, field) else null;
    out.fields = try cloneStringSlice(alloc, generated.fields);
    return out;
}

fn cloneDdlPrimaryKeyMaybe(alloc: std.mem.Allocator, primary_key: ?runtime_schema.PrimaryKey) !?runtime_schema.PrimaryKey {
    return if (primary_key) |key| try cloneDdlPrimaryKey(alloc, key) else null;
}

fn cloneDdlPrimaryKey(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) !runtime_schema.PrimaryKey {
    return .{ .columns = try cloneStringSlice(alloc, primary_key.columns) };
}

fn cloneDdlUniqueExpression(alloc: std.mem.Allocator, expression: runtime_schema.UniqueExpression) !runtime_schema.UniqueExpression {
    return .{
        .op = expression.op,
        .field = try alloc.dupe(u8, expression.field),
    };
}

fn cloneDdlUniqueExpressions(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) ![]const runtime_schema.UniqueExpression {
    if (expressions.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.UniqueExpression, expressions.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |expression| alloc.free(expression.field);
        alloc.free(out);
    }
    for (expressions, 0..) |expression, i| {
        out[i] = try cloneDdlUniqueExpression(alloc, expression);
        initialized += 1;
    }
    return out;
}

fn cloneDdlUniquePredicate(alloc: std.mem.Allocator, predicate: runtime_schema.UniquePredicate) !runtime_schema.UniquePredicate {
    const field = try alloc.dupe(u8, predicate.field);
    const value_json = if (predicate.value_json) |value|
        alloc.dupe(u8, value) catch |err| {
            alloc.free(field);
            return err;
        }
    else
        null;
    return .{
        .field = field,
        .op = predicate.op,
        .value_json = value_json,
    };
}

fn cloneDdlUniquePredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) ![]const runtime_schema.UniquePredicate {
    if (predicates.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.UniquePredicate, predicates.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |predicate| {
            alloc.free(predicate.field);
            if (predicate.value_json) |value| alloc.free(value);
        }
        alloc.free(out);
    }
    for (predicates, 0..) |predicate, i| {
        out[i] = try cloneDdlUniquePredicate(alloc, predicate);
        initialized += 1;
    }
    return out;
}

fn cloneDdlUniqueConstraint(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) !runtime_schema.UniqueConstraint {
    var out: runtime_schema.UniqueConstraint = .{
        .name = try alloc.dupe(u8, constraint.name),
    };
    errdefer freeDdlUniqueConstraint(alloc, out);
    out.columns = try cloneStringSlice(alloc, constraint.columns);
    out.expressions = try cloneDdlUniqueExpressions(alloc, constraint.expressions);
    out.where = try cloneDdlUniquePredicates(alloc, constraint.where);
    out.validation_state = constraint.validation_state;
    return out;
}

fn cloneDdlUniqueConstraints(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) ![]const runtime_schema.UniqueConstraint {
    if (constraints.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.UniqueConstraint, constraints.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |constraint| freeDdlUniqueConstraint(alloc, constraint);
        alloc.free(out);
    }
    for (constraints, 0..) |constraint, i| {
        out[i] = try cloneDdlUniqueConstraint(alloc, constraint);
        initialized += 1;
    }
    return out;
}

fn cloneDdlForeignKey(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) !runtime_schema.ForeignKey {
    const name = try alloc.dupe(u8, foreign_key.name);
    const parent_table = alloc.dupe(u8, foreign_key.parent_table) catch |err| {
        alloc.free(name);
        return err;
    };
    var out: runtime_schema.ForeignKey = .{
        .name = name,
        .parent_table = parent_table,
        .on_delete = foreign_key.on_delete,
        .on_update = foreign_key.on_update,
        .timing = foreign_key.timing,
        .deferrable = foreign_key.deferrable,
        .match = foreign_key.match,
        .validation_state = foreign_key.validation_state,
    };
    errdefer freeDdlForeignKey(alloc, out);
    out.child_columns = try cloneStringSlice(alloc, foreign_key.child_columns);
    out.parent_columns = try cloneStringSlice(alloc, foreign_key.parent_columns);
    return out;
}

fn cloneDdlForeignKeys(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) ![]const runtime_schema.ForeignKey {
    if (foreign_keys.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.ForeignKey, foreign_keys.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |foreign_key| freeDdlForeignKey(alloc, foreign_key);
        alloc.free(out);
    }
    for (foreign_keys, 0..) |foreign_key, i| {
        out[i] = try cloneDdlForeignKey(alloc, foreign_key);
        initialized += 1;
    }
    return out;
}

fn cloneDdlRelationalCheck(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) !runtime_schema.RelationalCheck {
    const name = try alloc.dupe(u8, check.name);
    const field = alloc.dupe(u8, check.field) catch |err| {
        alloc.free(name);
        return err;
    };
    var out: runtime_schema.RelationalCheck = .{
        .name = name,
        .field = field,
        .op = check.op,
        .validation_state = check.validation_state,
    };
    errdefer freeDdlRelationalCheck(alloc, out);
    out.value_json = if (check.value_json) |value| try alloc.dupe(u8, value) else null;
    return out;
}

fn cloneDdlRelationalChecks(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) ![]const runtime_schema.RelationalCheck {
    if (checks.len == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalCheck, checks.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| freeDdlRelationalCheck(alloc, check);
        alloc.free(out);
    }
    for (checks, 0..) |check, i| {
        out[i] = try cloneDdlRelationalCheck(alloc, check);
        initialized += 1;
    }
    return out;
}

fn appendRelationalColumnAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    column: runtime_schema.RelationalColumn,
) !void {
    if (relationalColumnIndex(schema.relational_columns, column.name) != null) return error.InvalidSqlCatalog;
    const len = schema.relational_columns.len;
    const out = try alloc.alloc(runtime_schema.RelationalColumn, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.relational_columns);
    out[len] = try cloneDdlRelationalColumn(alloc, column);
    if (len > 0) alloc.free(schema.relational_columns);
    schema.relational_columns = out;
}

fn appendUniqueConstraintAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    constraint: runtime_schema.UniqueConstraint,
) !void {
    if (uniqueConstraintNameExists(schema.unique_constraints, constraint.name)) return error.InvalidSqlCatalog;
    const len = schema.unique_constraints.len;
    const out = try alloc.alloc(runtime_schema.UniqueConstraint, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.unique_constraints);
    out[len] = try cloneDdlUniqueConstraint(alloc, constraint);
    if (len > 0) alloc.free(schema.unique_constraints);
    schema.unique_constraints = out;
}

fn appendForeignKeyAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    foreign_key: runtime_schema.ForeignKey,
) !void {
    if (foreignKeyNameExists(schema.foreign_keys, foreign_key.name)) return error.InvalidSqlCatalog;
    const len = schema.foreign_keys.len;
    const out = try alloc.alloc(runtime_schema.ForeignKey, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.foreign_keys);
    out[len] = try cloneDdlForeignKey(alloc, foreign_key);
    if (len > 0) alloc.free(schema.foreign_keys);
    schema.foreign_keys = out;
}

fn appendRelationalCheckAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    check: runtime_schema.RelationalCheck,
) !void {
    if (relationalCheckNameExists(schema.checks, check.name)) return error.InvalidSqlCatalog;
    const len = schema.checks.len;
    const out = try alloc.alloc(runtime_schema.RelationalCheck, len + 1);
    errdefer alloc.free(out);
    @memcpy(out[0..len], schema.checks);
    out[len] = try cloneDdlRelationalCheck(alloc, check);
    if (len > 0) alloc.free(schema.checks);
    schema.checks = out;
}

fn validateConstraintByName(schema: *runtime_schema.TableSchema, constraint_name: []const u8) !void {
    {
        const constraints = @constCast(schema.unique_constraints);
        for (constraints) |*constraint| {
            if (!std.mem.eql(u8, constraint.name, constraint_name)) continue;
            constraint.validation_state = .enforced;
            return;
        }
    }
    {
        const foreign_keys = @constCast(schema.foreign_keys);
        for (foreign_keys) |*foreign_key| {
            if (!std.mem.eql(u8, foreign_key.name, constraint_name)) continue;
            foreign_key.validation_state = .enforced;
            return;
        }
    }
    {
        const checks = @constCast(schema.checks);
        for (checks) |*check| {
            if (!std.mem.eql(u8, check.name, constraint_name)) continue;
            check.validation_state = .enforced;
            return;
        }
    }
    return error.InvalidSqlCatalog;
}

fn markColumnIndexedAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    column_name: []const u8,
    predicates: []const runtime_schema.UniquePredicate,
    index_generation: u64,
) !void {
    const index = relationalColumnIndex(schema.relational_columns, column_name) orelse return error.InvalidSqlCatalog;
    const cloned_predicates = try cloneDdlUniquePredicates(alloc, predicates);
    errdefer freeDdlUniquePredicates(alloc, cloned_predicates);
    const columns = @constCast(schema.relational_columns);
    freeDdlUniquePredicates(alloc, columns[index].index_where);
    columns[index].indexed = true;
    columns[index].index_lifecycle = .building;
    columns[index].index_generation = index_generation;
    columns[index].index_where = cloned_predicates;
}

fn stableSecondaryIndexGeneration(plan: CreateIndexPlan) u64 {
    var hasher = std.hash.Wyhash.init(0x5149_2026_5345_4349);
    hashPlanField(&hasher, "secondary-index-v1");
    hashPlanField(&hasher, plan.index_name);
    hashPlanU64(&hasher, @intCast(plan.columns.len));
    for (plan.columns) |column| hashPlanField(&hasher, column);
    hashPlanU64(&hasher, @intCast(plan.expressions.len));
    for (plan.expressions) |expression| {
        hashPlanU64(&hasher, @intFromEnum(expression.op));
        hashPlanField(&hasher, expression.field);
    }
    hashPlanU64(&hasher, @intCast(plan.where.len));
    for (plan.where) |predicate| {
        hashPlanU64(&hasher, @intFromEnum(predicate.op));
        hashPlanField(&hasher, predicate.field);
        if (predicate.value_json) |value| {
            hasher.update(&.{1});
            hashPlanField(&hasher, value);
        } else {
            hasher.update(&.{0});
        }
    }
    const json_integer_max: u64 = @intCast(std.math.maxInt(i64));
    const generation = hasher.final() & json_integer_max;
    return if (generation == 0) 1 else generation;
}

fn hashPlanField(hasher: *std.hash.Wyhash, value: []const u8) void {
    hashPlanU64(hasher, @intCast(value.len));
    hasher.update(value);
}

fn hashPlanU64(hasher: *std.hash.Wyhash, value: u64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    hasher.update(&buf);
}

fn relationalIndexLifecycleName(lifecycle: runtime_schema.RelationalIndexLifecycle) []const u8 {
    return switch (lifecycle) {
        .ready => "ready",
        .building => "building",
        .invalid => "invalid",
        .dropping => "dropping",
    };
}

fn setColumnOnUpdatePolicyAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    column_name: []const u8,
    value: runtime_schema.RelationalDefaultValue,
) !void {
    const index = relationalColumnIndex(schema.relational_columns, column_name) orelse return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[index].field_type != .numeric and columns[index].field_type != .datetime) return error.InvalidSqlCatalog;
    const cloned_value = try cloneDdlDefaultValue(alloc, value);
    errdefer alloc.free(cloned_value.value_json);
    if (columns[index].on_update_value) |existing| alloc.free(existing.value_json);
    columns[index].on_update_value = cloned_value;
}

fn validateRelationalColumnCatalog(columns: []const runtime_schema.RelationalColumn) !void {
    for (columns, 0..) |column, i| {
        if (relationalColumnIndex(columns[0..i], column.name) != null) return error.InvalidSqlCatalog;
        if (!std.mem.eql(u8, column.name, column.path)) return error.InvalidSqlCatalog;
        if (column.generated) |_| try validateGeneratedColumnForColumns(columns, column);
        try validateUniquePredicatesForColumns(columns, column.index_where);
        if (column.on_update_value) |_| {
            if (column.field_type != .numeric and column.field_type != .datetime) return error.InvalidSqlCatalog;
        }
    }
}

fn validateUniqueConstraintCatalog(columns: []const runtime_schema.RelationalColumn, constraints: []const runtime_schema.UniqueConstraint) !void {
    for (constraints, 0..) |constraint, i| {
        if (uniqueConstraintNameExists(constraints[0..i], constraint.name)) return error.InvalidSqlCatalog;
        try validateUniqueConstraintForColumns(columns, constraint);
    }
}

fn validateForeignKeyCatalog(columns: []const runtime_schema.RelationalColumn, foreign_keys: []const runtime_schema.ForeignKey) !void {
    for (foreign_keys, 0..) |foreign_key, i| {
        if (foreignKeyNameExists(foreign_keys[0..i], foreign_key.name)) return error.InvalidSqlCatalog;
        try validateForeignKeyForColumns(columns, foreign_key);
    }
}

fn validateRelationalCheckCatalog(columns: []const runtime_schema.RelationalColumn, checks: []const runtime_schema.RelationalCheck) !void {
    for (checks, 0..) |check, i| {
        if (relationalCheckNameExists(checks[0..i], check.name)) return error.InvalidSqlCatalog;
        try validateCheckForColumns(columns, check);
    }
}

fn validatePrimaryKeyColumns(columns: []const runtime_schema.RelationalColumn, primary_key: runtime_schema.PrimaryKey) !void {
    if (primary_key.columns.len == 0) return error.InvalidSqlCatalog;
    for (primary_key.columns) |column| {
        const found = relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.nullable) return error.InvalidSqlCatalog;
    }
}

fn validateUniqueConstraintForColumns(columns: []const runtime_schema.RelationalColumn, constraint: runtime_schema.UniqueConstraint) !void {
    if (constraint.columns.len == 0 and constraint.expressions.len == 0) return error.InvalidSqlCatalog;
    for (constraint.columns) |column| {
        const found = relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower => {
                const found = relationalColumnForDdl(columns, expression.field) orelse return error.InvalidSqlCatalog;
                if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
            },
        }
    }
    try validateUniquePredicatesForColumns(columns, constraint.where);
}

fn validateForeignKeyForColumns(columns: []const runtime_schema.RelationalColumn, foreign_key: runtime_schema.ForeignKey) !void {
    if (foreign_key.child_columns.len == 0 or foreign_key.child_columns.len != foreign_key.parent_columns.len) return error.InvalidSqlCatalog;
    for (foreign_key.child_columns) |column| {
        const found = relationalColumnForDdl(columns, column) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

fn validateCheckForColumns(columns: []const runtime_schema.RelationalColumn, check: runtime_schema.RelationalCheck) !void {
    _ = relationalColumnForDdl(columns, check.field) orelse return error.InvalidSqlCatalog;
}

fn validateGeneratedColumnForColumns(columns: []const runtime_schema.RelationalColumn, column: runtime_schema.RelationalColumn) !void {
    const generated = column.generated orelse return;
    switch (generated.op) {
        .lower => {
            const field = generated.field orelse return error.InvalidSqlCatalog;
            if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
            const source = relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
            if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
        },
        .concat => {
            if (generated.fields.len == 0) return error.InvalidSqlCatalog;
            for (generated.fields) |field| {
                if (std.mem.eql(u8, field, column.name)) return error.InvalidSqlCatalog;
                const source = relationalColumnForDdl(columns, field) orelse return error.InvalidSqlCatalog;
                if (source.field_type == .json or source.field_type == .array) return error.InvalidSqlCatalog;
            }
        },
    }
}

fn validateUniquePredicatesForColumns(columns: []const runtime_schema.RelationalColumn, predicates: []const runtime_schema.UniquePredicate) !void {
    for (predicates) |predicate| {
        const found = relationalColumnForDdl(columns, predicate.field) orelse return error.InvalidSqlCatalog;
        if (found.field_type == .json or found.field_type == .array) return error.InvalidSqlCatalog;
    }
}

fn relationalColumnForDdl(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?runtime_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn relationalColumnIndex(columns: []const runtime_schema.RelationalColumn, name: []const u8) ?usize {
    for (columns, 0..) |column, i| {
        if (std.mem.eql(u8, column.name, name)) return i;
    }
    return null;
}

fn uniqueConstraintNameExists(constraints: []const runtime_schema.UniqueConstraint, name: []const u8) bool {
    for (constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return true;
    }
    return false;
}

fn foreignKeyNameExists(foreign_keys: []const runtime_schema.ForeignKey, name: []const u8) bool {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return true;
    }
    return false;
}

fn relationalCheckNameExists(checks: []const runtime_schema.RelationalCheck, name: []const u8) bool {
    for (checks) |check| {
        if (std.mem.eql(u8, check.name, name)) return true;
    }
    return false;
}

fn conflictActionToken(action: Parser.ConflictAction) []const u8 {
    return switch (action) {
        .nothing => "nothing",
        .update => "update",
    };
}

fn uniquePredicateOpToken(op: runtime_schema.UniquePredicateOp) []const u8 {
    return switch (op) {
        .is_null => "is_null",
        .is_not_null => "is_not_null",
        .eq => "eq",
        .ne => "ne",
    };
}

fn arrayTransformOpToken(op: db_mod.types.TransformOpType) []const u8 {
    return switch (op) {
        .push => "append",
        .pull => "remove",
        .add_to_set => "add_to_set",
        else => unreachable,
    };
}

fn validateUniqueWhereJsonMatches(alloc: std.mem.Allocator, where_json: []const u8, predicates: []const runtime_schema.UniquePredicate) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, where_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const all_value = parsed.value.object.get("all") orelse return error.UnsupportedSqlShape;
    if (all_value != .array or all_value.array.items.len != predicates.len) return error.UnsupportedSqlShape;
    for (all_value.array.items, predicates) |item, predicate| {
        if (item != .object) return error.UnsupportedSqlShape;
        const field_value = item.object.get("field") orelse return error.UnsupportedSqlShape;
        const op_value = item.object.get("op") orelse return error.UnsupportedSqlShape;
        if (field_value != .string or !std.mem.eql(u8, field_value.string, predicate.field)) return error.UnsupportedSqlShape;
        if (op_value != .string or !std.mem.eql(u8, op_value.string, uniquePredicateOpToken(predicate.op))) return error.UnsupportedSqlShape;
        const supplied_value = item.object.get("value");
        if (predicate.value_json) |expected_json| {
            const supplied = supplied_value orelse return error.UnsupportedSqlShape;
            const supplied_json = try std.json.Stringify.valueAlloc(alloc, supplied, .{});
            defer alloc.free(supplied_json);
            if (!std.mem.eql(u8, supplied_json, expected_json)) return error.UnsupportedSqlShape;
        } else if (supplied_value != null) {
            return error.UnsupportedSqlShape;
        }
    }
}

fn updateWillLookupExistingRow(schema: runtime_schema.TableSchema, returning: ReturningProjection) bool {
    if (returning.hasProjection() or schema.checks.len > 0) return true;
    for (schema.relational_columns) |column| {
        if (column.generated != null) return true;
    }
    return false;
}

fn validateJsonDocument(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    switch (parsed.value) {
        .object, .array => {},
        else => return error.UnsupportedSqlShape,
    }
}

fn validateJsonArray(alloc: std.mem.Allocator, value: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array) return error.UnsupportedSqlShape;
}

fn jsonValueIsValid(alloc: std.mem.Allocator, value: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return false;
    parsed.deinit();
    return true;
}

fn freeStringSlice(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn freeAlterTableOperation(alloc: std.mem.Allocator, operation: AlterTableOperation) void {
    switch (operation) {
        .add_column => |column| freeDdlRelationalColumn(alloc, column),
        .add_unique_constraint => |constraint| freeDdlUniqueConstraint(alloc, constraint),
        .add_foreign_key => |foreign_key| freeDdlForeignKey(alloc, foreign_key),
        .add_check => |check| freeDdlRelationalCheck(alloc, check),
        .validate_constraint => |constraint_name| alloc.free(constraint_name),
    }
}

fn freeDdlRelationalColumn(alloc: std.mem.Allocator, column: runtime_schema.RelationalColumn) void {
    alloc.free(column.name);
    alloc.free(column.path);
    if (column.default_value) |value| alloc.free(value.value_json);
    if (column.on_update_value) |value| alloc.free(value.value_json);
    if (column.generated) |generated| {
        freeDdlGeneratedValue(alloc, generated);
    }
    for (column.index_where) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value| alloc.free(value);
    }
    if (column.index_where.len > 0) alloc.free(column.index_where);
}

fn freeDdlRelationalColumns(alloc: std.mem.Allocator, columns: []const runtime_schema.RelationalColumn) void {
    for (columns) |column| freeDdlRelationalColumn(alloc, column);
    if (columns.len > 0) alloc.free(columns);
}

fn freeDdlGeneratedValue(alloc: std.mem.Allocator, generated: runtime_schema.RelationalGeneratedValue) void {
    if (generated.field) |field| alloc.free(field);
    freeStringSlice(alloc, generated.fields);
    alloc.free(generated.separator);
}

fn freeDdlPrimaryKey(alloc: std.mem.Allocator, primary_key: runtime_schema.PrimaryKey) void {
    freeStringSlice(alloc, primary_key.columns);
}

fn freeDdlUniqueConstraint(alloc: std.mem.Allocator, constraint: runtime_schema.UniqueConstraint) void {
    alloc.free(constraint.name);
    freeStringSlice(alloc, constraint.columns);
    freeDdlUniqueExpressions(alloc, constraint.expressions);
    freeDdlUniquePredicates(alloc, constraint.where);
}

fn freeDdlUniqueConstraints(alloc: std.mem.Allocator, constraints: []const runtime_schema.UniqueConstraint) void {
    for (constraints) |constraint| freeDdlUniqueConstraint(alloc, constraint);
    if (constraints.len > 0) alloc.free(constraints);
}

fn freeDdlUniqueExpressions(alloc: std.mem.Allocator, expressions: []const runtime_schema.UniqueExpression) void {
    for (expressions) |expression| alloc.free(expression.field);
    if (expressions.len > 0) alloc.free(expressions);
}

fn freeDdlUniquePredicates(alloc: std.mem.Allocator, predicates: []const runtime_schema.UniquePredicate) void {
    for (predicates) |predicate| {
        alloc.free(predicate.field);
        if (predicate.value_json) |value| alloc.free(value);
    }
    if (predicates.len > 0) alloc.free(predicates);
}

fn freeDdlForeignKey(alloc: std.mem.Allocator, foreign_key: runtime_schema.ForeignKey) void {
    alloc.free(foreign_key.name);
    freeStringSlice(alloc, foreign_key.child_columns);
    alloc.free(foreign_key.parent_table);
    freeStringSlice(alloc, foreign_key.parent_columns);
}

fn freeDdlForeignKeys(alloc: std.mem.Allocator, foreign_keys: []const runtime_schema.ForeignKey) void {
    for (foreign_keys) |foreign_key| freeDdlForeignKey(alloc, foreign_key);
    if (foreign_keys.len > 0) alloc.free(foreign_keys);
}

fn freeDdlRelationalCheck(alloc: std.mem.Allocator, check: runtime_schema.RelationalCheck) void {
    alloc.free(check.name);
    alloc.free(check.field);
    if (check.value_json) |value| alloc.free(value);
}

fn freeDdlRelationalChecks(alloc: std.mem.Allocator, checks: []const runtime_schema.RelationalCheck) void {
    for (checks) |check| freeDdlRelationalCheck(alloc, check);
    if (checks.len > 0) alloc.free(checks);
}

fn freeFieldJsonValues(alloc: std.mem.Allocator, values: []const Parser.FieldJsonValue) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeArrayTransformValues(alloc: std.mem.Allocator, values: []const Parser.ArrayTransformValue) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeFieldPredicates(alloc: std.mem.Allocator, values: []const Parser.FieldPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        if (value.value_json) |json| alloc.free(json);
    }
}

fn freeTableAlias(alloc: std.mem.Allocator, value: Parser.TableAlias) void {
    alloc.free(value.name);
    alloc.free(value.alias);
}

fn freeQualifiedField(alloc: std.mem.Allocator, value: Parser.QualifiedField) void {
    alloc.free(value.qualifier);
    alloc.free(value.field);
}

fn freeQualifiedProjections(alloc: std.mem.Allocator, values: []const Parser.QualifiedProjection) void {
    for (values) |value| {
        freeQualifiedField(alloc, value.source);
        alloc.free(value.output);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeLateralCorrelations(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsLateralCorrelation) void {
    for (values) |value| {
        alloc.free(value.left_field);
        alloc.free(value.right_field);
    }
}

fn freeLateralSubquery(alloc: std.mem.Allocator, value: Parser.LateralSubquery) void {
    if (value.table.name.len > 0 or value.table.alias.len > 0) freeTableAlias(alloc, value.table);
    freeRelationalChecks(alloc, value.predicates);
    if (value.predicates.len > 0) alloc.free(value.predicates);
    freeLateralCorrelations(alloc, value.correlations);
    if (value.correlations.len > 0) alloc.free(value.correlations);
    freeOrderBy(alloc, value.order_by);
    if (value.order_by.len > 0) alloc.free(value.order_by);
}

fn freeAggregateSpec(alloc: std.mem.Allocator, spec: db_mod.types.RelationalRowsAggregateSpec) void {
    alloc.free(spec.name);
    if (spec.field) |field| alloc.free(field);
    if (spec.expression) |expression| freeExpression(alloc, expression);
    freeOrderBy(alloc, spec.array_order_by);
    if (spec.array_order_by.len > 0) alloc.free(spec.array_order_by);
    freeRelationalChecks(alloc, spec.filter_predicates);
    if (spec.filter_predicates.len > 0) alloc.free(spec.filter_predicates);
    freeExpressionConditions(alloc, spec.filter_expressions);
    if (spec.filter_expressions.len > 0) alloc.free(spec.filter_expressions);
}

fn freeAggregateSpecs(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsAggregateSpec) void {
    for (values) |value| freeAggregateSpec(alloc, value);
}

fn freeJsonSetValues(alloc: std.mem.Allocator, values: []const Parser.JsonSetValue) void {
    for (values) |value| {
        alloc.free(value.field);
        freeStringSlice(alloc, value.path);
        alloc.free(value.value_json);
    }
}

fn freeArrayContains(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayContainsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeArrayEq(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayEqPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeInPredicates(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsInPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.values_json);
    }
}

fn freeConflictTarget(alloc: std.mem.Allocator, target: Parser.ConflictTarget) void {
    switch (target) {
        .primary => {},
        .unique => |unique| {
            alloc.free(unique.name);
            if (unique.where_json.len > 0) alloc.free(unique.where_json);
        },
    }
}

fn freeConflictClause(alloc: std.mem.Allocator, clause: Parser.ConflictClause) void {
    freeConflictTarget(alloc, clause.target);
    freeFieldJsonValues(alloc, clause.patch);
    if (clause.patch.len > 0) alloc.free(clause.patch);
    freeFieldJsonValues(alloc, clause.increment);
    if (clause.increment.len > 0) alloc.free(clause.increment);
    freeJsonSetValues(alloc, clause.json_set);
    if (clause.json_set.len > 0) alloc.free(clause.json_set);
    freeArrayTransformValues(alloc, clause.array_update);
    if (clause.array_update.len > 0) alloc.free(clause.array_update);
}

fn freeJoinOn(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJoinOn) void {
    for (values) |value| {
        alloc.free(value.left_field);
        alloc.free(value.right_field);
    }
}

fn freeJoinProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJoinProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
}

fn freeSelectItem(alloc: std.mem.Allocator, item: Parser.SelectItem) void {
    switch (item) {
        .field => |field| alloc.free(field),
        .json_extract => |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
            alloc.free(projection.path);
        },
        .array_length => |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        },
        .coalesce => |projection| freeCoalesceProjection(alloc, projection),
        .expression => |projection| freeExpressionProjection(alloc, projection),
        .field_alias => |projection| {
            alloc.free(projection.output);
            alloc.free(projection.field);
        },
    }
}

fn freeRelationalCheck(alloc: std.mem.Allocator, value: runtime_schema.RelationalCheck) void {
    alloc.free(value.field);
    if (value.value_json) |json| alloc.free(json);
}

fn freeRelationalChecks(alloc: std.mem.Allocator, values: []const runtime_schema.RelationalCheck) void {
    for (values) |value| freeRelationalCheck(alloc, value);
}

fn freePredicateGroups(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsPredicateGroup) void {
    for (values) |value| {
        freeRelationalChecks(alloc, value.predicates);
        if (value.predicates.len > 0) alloc.free(value.predicates);
    }
}

fn freeJsonContains(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonContainsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.value_json);
    }
}

fn freeJsonPathEq(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonPathEqPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.path);
        alloc.free(value.value_json);
    }
}

fn freeJsonPathExists(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonPathExistsPredicate) void {
    for (values) |value| {
        alloc.free(value.field);
        alloc.free(value.path);
    }
}

fn freeJsonExtract(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsJsonExtractProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
        alloc.free(value.path);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeArrayLengthProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsArrayLengthProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeCoalesceOperand(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsCoalesceOperand) void {
    switch (value.kind) {
        .field => if (value.field.len > 0) alloc.free(value.field),
        .value => if (value.value_json.len > 0) alloc.free(value.value_json),
    }
}

fn freeCoalesceProjection(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsCoalesceProjection) void {
    alloc.free(value.output);
    for (value.operands) |operand| freeCoalesceOperand(alloc, operand);
    if (value.operands.len > 0) alloc.free(value.operands);
}

fn freeCoalesceProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsCoalesceProjection) void {
    for (values) |value| freeCoalesceProjection(alloc, value);
    if (values.len > 0) alloc.free(values);
}

fn expressionProjectionFromCoalesceAlloc(
    alloc: std.mem.Allocator,
    projection: db_mod.types.RelationalRowsCoalesceProjection,
) !db_mod.types.RelationalRowsExpressionProjection {
    const output = try alloc.dupe(u8, projection.output);
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);

    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, projection.operands.len);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }
    for (projection.operands) |operand| {
        operands[initialized] = switch (operand.kind) {
            .field => .{
                .kind = .field,
                .field = try alloc.dupe(u8, operand.field),
            },
            .value => .{
                .kind = .value,
                .value_json = try alloc.dupe(u8, operand.value_json),
            },
        };
        initialized += 1;
    }

    output_transferred = true;
    return .{
        .output = output,
        .expression = .{
            .kind = .coalesce,
            .operands = operands,
        },
    };
}

fn freeExpression(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpression) void {
    if (value.field.len > 0) alloc.free(value.field);
    if (value.value_json.len > 0) alloc.free(value.value_json);
    if (value.json_path.len > 0) alloc.free(value.json_path);
    for (value.operands) |operand| freeExpression(alloc, operand);
    if (value.operands.len > 0) alloc.free(value.operands);
    for (value.case_branches) |branch| freeExpressionCaseBranch(alloc, branch);
    if (value.case_branches.len > 0) alloc.free(value.case_branches);
    for (value.case_else) |fallback| freeExpression(alloc, fallback);
    if (value.case_else.len > 0) alloc.free(value.case_else);
}

fn freeExpressionCaseBranch(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionCaseBranch) void {
    freeExpressionCondition(alloc, value.when);
    freeExpression(alloc, value.then);
}

fn freeExpressionCondition(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionCondition) void {
    freeExpression(alloc, value.lhs);
    for (value.rhs) |rhs| freeExpression(alloc, rhs);
    if (value.rhs.len > 0) alloc.free(value.rhs);
}

fn freeExpressionConditions(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionCondition) void {
    for (values) |value| freeExpressionCondition(alloc, value);
}

fn freeExpressionProjection(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsExpressionProjection) void {
    alloc.free(value.output);
    freeExpression(alloc, value.expression);
}

fn freeExpressionProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsExpressionProjection) void {
    for (values) |value| freeExpressionProjection(alloc, value);
    if (values.len > 0) alloc.free(values);
}

fn freeFieldAliasProjections(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsFieldAliasProjection) void {
    for (values) |value| {
        alloc.free(value.output);
        alloc.free(value.field);
    }
    if (values.len > 0) alloc.free(values);
}

fn freeWindowSpec(alloc: std.mem.Allocator, value: db_mod.types.RelationalRowsWindowSpec) void {
    alloc.free(value.output);
    freeStringSlice(alloc, value.partition_by);
    freeOrderBy(alloc, value.order_by);
    if (value.order_by.len > 0) alloc.free(value.order_by);
}

fn freeWindowSpecs(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsWindowSpec) void {
    for (values) |value| freeWindowSpec(alloc, value);
}

fn freeOrderBy(alloc: std.mem.Allocator, values: []const db_mod.types.RelationalRowsQueryOrder) void {
    for (values) |value| {
        if (value.field.len > 0) alloc.free(value.field);
        if (value.expression) |expression| freeExpression(alloc, expression);
    }
}

test "postgres sql adapter lowers create table ddl into typed schema plan" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanAlloc(
        alloc,
        \\CREATE TABLE IF NOT EXISTS usage_records (
        \\  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
        \\  tenant_id text NOT NULL,
        \\  amount numeric(18, 2) DEFAULT 0 CHECK (amount >= 0),
        \\  metadata jsonb,
        \\  tags text[],
        \\  created_at timestamptz DEFAULT now(),
        \\  email_key text GENERATED ALWAYS AS (lower(tenant_id)) STORED,
        \\  CONSTRAINT usage_records_tenant_key UNIQUE (tenant_id),
        \\  CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
        \\);
        ,
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .create_table => |plan| {
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqual(@as(usize, 7), plan.columns.len);
            try std.testing.expectEqualStrings("id", plan.columns[0].name);
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, plan.columns[0].field_type);
            try std.testing.expect(!plan.columns[0].nullable);
            try std.testing.expect(plan.columns[0].default_value != null);
            try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.uuid_v4, plan.columns[0].default_value.?.kind);
            try std.testing.expectEqualStrings("amount", plan.columns[2].name);
            try std.testing.expectEqual(runtime_schema.AntflyType.numeric, plan.columns[2].field_type);
            try std.testing.expect(plan.columns[2].default_value != null);
            try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, plan.columns[2].default_value.?.kind);
            try std.testing.expectEqualStrings("0", plan.columns[2].default_value.?.value_json);
            try std.testing.expectEqual(runtime_schema.AntflyType.json, plan.columns[3].field_type);
            try std.testing.expectEqual(runtime_schema.AntflyType.array, plan.columns[4].field_type);
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, plan.columns[4].array_item_type.?);
            try std.testing.expectEqual(runtime_schema.AntflyType.datetime, plan.columns[5].field_type);
            try std.testing.expect(plan.columns[5].default_value != null);
            try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.now_ns, plan.columns[5].default_value.?.kind);
            try std.testing.expectEqualStrings("email_key", plan.columns[6].name);
            try std.testing.expect(plan.columns[6].generated != null);
            try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, plan.columns[6].generated.?.op);
            try std.testing.expectEqualStrings("tenant_id", plan.columns[6].generated.?.field.?);
            try std.testing.expect(plan.primary_key != null);
            try std.testing.expectEqual(@as(usize, 1), plan.primary_key.?.columns.len);
            try std.testing.expectEqualStrings("id", plan.primary_key.?.columns[0]);
            try std.testing.expectEqual(@as(usize, 1), plan.unique_constraints.len);
            try std.testing.expectEqualStrings("usage_records_tenant_key", plan.unique_constraints[0].name);
            try std.testing.expectEqualStrings("tenant_id", plan.unique_constraints[0].columns[0]);
            try std.testing.expectEqual(@as(usize, 1), plan.foreign_keys.len);
            try std.testing.expectEqualStrings("usage_records_tenant_fkey", plan.foreign_keys[0].name);
            try std.testing.expectEqualStrings("tenant_id", plan.foreign_keys[0].child_columns[0]);
            try std.testing.expectEqualStrings("tenants", plan.foreign_keys[0].parent_table);
            try std.testing.expectEqual(runtime_schema.ForeignKeyAction.cascade, plan.foreign_keys[0].on_delete);
            try std.testing.expectEqual(@as(usize, 1), plan.checks.len);
            try std.testing.expectEqualStrings("amount", plan.checks[0].field);
            try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gte, plan.checks[0].op);
            try std.testing.expectEqualStrings("0", plan.checks[0].value_json.?);
        },
        .create_index => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers application inline foreign key ddl into typed schema plan" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanAlloc(
        alloc,
        \\CREATE TABLE cloud_groups (
        \\  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        \\  organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
        \\  name VARCHAR(100) NOT NULL,
        \\  slug VARCHAR(100) NOT NULL,
        \\  description TEXT,
        \\  created_by UUID REFERENCES users(id) ON DELETE SET NULL,
        \\  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        \\  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        \\  CONSTRAINT cloud_groups_slug_not_empty CHECK (slug <> ''),
        \\  CONSTRAINT cloud_groups_org_slug_unique UNIQUE (organization_id, slug)
        \\);
        ,
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .create_table => |plan| {
            try std.testing.expectEqualStrings("cloud_groups", plan.table_name);
            try std.testing.expectEqual(@as(usize, 8), plan.columns.len);
            try std.testing.expect(plan.primary_key != null);
            try std.testing.expectEqualStrings("id", plan.primary_key.?.columns[0]);
            try std.testing.expectEqual(@as(usize, 1), plan.unique_constraints.len);
            try std.testing.expectEqualStrings("cloud_groups_org_slug_unique", plan.unique_constraints[0].name);
            try std.testing.expectEqualStrings("organization_id", plan.unique_constraints[0].columns[0]);
            try std.testing.expectEqualStrings("slug", plan.unique_constraints[0].columns[1]);
            try std.testing.expectEqual(@as(usize, 2), plan.foreign_keys.len);
            try std.testing.expectEqualStrings("organizations_organization_id_fkey", plan.foreign_keys[0].name);
            try std.testing.expectEqualStrings("organization_id", plan.foreign_keys[0].child_columns[0]);
            try std.testing.expectEqualStrings("organizations", plan.foreign_keys[0].parent_table);
            try std.testing.expectEqualStrings("id", plan.foreign_keys[0].parent_columns[0]);
            try std.testing.expectEqual(runtime_schema.ForeignKeyAction.cascade, plan.foreign_keys[0].on_delete);
            try std.testing.expectEqualStrings("users_created_by_fkey", plan.foreign_keys[1].name);
            try std.testing.expectEqualStrings("created_by", plan.foreign_keys[1].child_columns[0]);
            try std.testing.expectEqualStrings("users", plan.foreign_keys[1].parent_table);
            try std.testing.expectEqualStrings("id", plan.foreign_keys[1].parent_columns[0]);
            try std.testing.expectEqual(runtime_schema.ForeignKeyAction.set_null, plan.foreign_keys[1].on_delete);
            try std.testing.expectEqual(@as(usize, 1), plan.checks.len);
            try std.testing.expectEqualStrings("cloud_groups_slug_not_empty", plan.checks[0].name);
        },
        .create_index => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers create index ddl into typed schema plan" {
    const alloc = std.testing.allocator;

    var ordinary = try lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX usage_records_status_idx ON usage_records (tenant_id, status);",
    );
    defer ordinary.deinit(alloc);
    switch (ordinary) {
        .create_index => |plan| {
            try std.testing.expect(!plan.unique);
            try std.testing.expectEqualStrings("usage_records_status_idx", plan.index_name);
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqual(@as(usize, 2), plan.columns.len);
            try std.testing.expectEqualStrings("tenant_id", plan.columns[0]);
            try std.testing.expectEqualStrings("status", plan.columns[1]);
            try std.testing.expectEqual(@as(usize, 0), plan.expressions.len);
            try std.testing.expectEqual(@as(usize, 0), plan.where.len);
        },
        .create_table => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    var ordered = try lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX idx_import_jobs_instance ON import_jobs USING btree (organization_id, cloud_instance_id, created_at DESC NULLS LAST);",
    );
    defer ordered.deinit(alloc);
    switch (ordered) {
        .create_index => |plan| {
            try std.testing.expect(!plan.unique);
            try std.testing.expectEqualStrings("idx_import_jobs_instance", plan.index_name);
            try std.testing.expectEqualStrings("import_jobs", plan.table_name);
            try std.testing.expectEqual(@as(usize, 3), plan.columns.len);
            try std.testing.expectEqualStrings("organization_id", plan.columns[0]);
            try std.testing.expectEqualStrings("cloud_instance_id", plan.columns[1]);
            try std.testing.expectEqualStrings("created_at", plan.columns[2]);
            try std.testing.expectEqual(@as(usize, 0), plan.expressions.len);
        },
        .create_table => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    var partial_expression = try lowerDdlPlanAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_lower_email_active_key ON users (tenant_id, lower(email)) WHERE (deleted_at IS NULL) AND ((status)::text = 'active'::text);",
    );
    defer partial_expression.deinit(alloc);
    switch (partial_expression) {
        .create_index => |plan| {
            try std.testing.expect(plan.unique);
            try std.testing.expectEqualStrings("users_lower_email_active_key", plan.index_name);
            try std.testing.expectEqualStrings("users", plan.table_name);
            try std.testing.expectEqual(@as(usize, 1), plan.columns.len);
            try std.testing.expectEqualStrings("tenant_id", plan.columns[0]);
            try std.testing.expectEqual(@as(usize, 1), plan.expressions.len);
            try std.testing.expectEqual(runtime_schema.UniqueExpressionOp.lower, plan.expressions[0].op);
            try std.testing.expectEqualStrings("email", plan.expressions[0].field);
            try std.testing.expectEqual(@as(usize, 2), plan.where.len);
            try std.testing.expectEqualStrings("deleted_at", plan.where[0].field);
            try std.testing.expectEqual(runtime_schema.UniquePredicateOp.is_null, plan.where[0].op);
            try std.testing.expectEqualStrings("status", plan.where[1].field);
            try std.testing.expectEqual(runtime_schema.UniquePredicateOp.eq, plan.where[1].op);
            try std.testing.expectEqualStrings("\"active\"", plan.where[1].value_json.?);
        },
        .create_table => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    var boolean_partial = try lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX idx_project_tables_default ON project_tables(project_id, is_default) WHERE (is_default = TRUE);",
    );
    defer boolean_partial.deinit(alloc);
    switch (boolean_partial) {
        .create_index => |plan| {
            try std.testing.expect(!plan.unique);
            try std.testing.expectEqualStrings("idx_project_tables_default", plan.index_name);
            try std.testing.expectEqualStrings("project_tables", plan.table_name);
            try std.testing.expectEqual(@as(usize, 2), plan.columns.len);
            try std.testing.expectEqualStrings("project_id", plan.columns[0]);
            try std.testing.expectEqualStrings("is_default", plan.columns[1]);
            try std.testing.expectEqual(@as(usize, 1), plan.where.len);
            try std.testing.expectEqualStrings("is_default", plan.where[0].field);
            try std.testing.expectEqual(runtime_schema.UniquePredicateOp.eq, plan.where[0].op);
            try std.testing.expectEqualStrings("true", plan.where[0].value_json.?);
        },
        .create_table => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    var casted_not_null = try lowerDdlPlanAlloc(
        alloc,
        "CREATE UNIQUE INDEX cloud_groups_external_key ON cloud_groups (organization_id, external_id) WHERE ((external_id)::text IS NOT NULL);",
    );
    defer casted_not_null.deinit(alloc);
    switch (casted_not_null) {
        .create_index => |plan| {
            try std.testing.expect(plan.unique);
            try std.testing.expectEqualStrings("cloud_groups_external_key", plan.index_name);
            try std.testing.expectEqual(@as(usize, 2), plan.columns.len);
            try std.testing.expectEqual(@as(usize, 1), plan.where.len);
            try std.testing.expectEqualStrings("external_id", plan.where[0].field);
            try std.testing.expectEqual(runtime_schema.UniquePredicateOp.is_not_null, plan.where[0].op);
        },
        .create_table => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX usage_records_metadata_gin ON usage_records USING gin (metadata)",
    ));
}

test "postgres sql adapter lowers alter table ddl into typed schema plan" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanAlloc(
        alloc,
        \\ALTER TABLE usage_records
        \\  ADD COLUMN metadata jsonb DEFAULT '{"source":"migration"}',
        \\  ADD COLUMN tenant_status_key text GENERATED ALWAYS AS (concat(tenant_id, ':', status)) STORED,
        \\  ADD CONSTRAINT usage_records_tenant_status_key UNIQUE (tenant_id, status),
        \\  ADD CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE SET NULL,
        \\  ADD CONSTRAINT usage_records_amount_check CHECK (amount >= 0);
        ,
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .alter_table => |plan| {
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqual(@as(usize, 5), plan.operations.len);
            switch (plan.operations[0]) {
                .add_column => |column| {
                    try std.testing.expectEqualStrings("metadata", column.name);
                    try std.testing.expectEqual(runtime_schema.AntflyType.json, column.field_type);
                    try std.testing.expect(column.default_value != null);
                    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.literal, column.default_value.?.kind);
                    try std.testing.expectEqualStrings("{\"source\":\"migration\"}", column.default_value.?.value_json);
                },
                else => return error.TestUnexpectedResult,
            }
            switch (plan.operations[1]) {
                .add_column => |column| {
                    try std.testing.expectEqualStrings("tenant_status_key", column.name);
                    try std.testing.expect(column.generated != null);
                    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, column.generated.?.op);
                    try std.testing.expectEqual(@as(usize, 2), column.generated.?.fields.len);
                    try std.testing.expectEqualStrings("tenant_id", column.generated.?.fields[0]);
                    try std.testing.expectEqualStrings("status", column.generated.?.fields[1]);
                    try std.testing.expectEqualStrings(":", column.generated.?.separator);
                },
                else => return error.TestUnexpectedResult,
            }
            switch (plan.operations[2]) {
                .add_unique_constraint => |constraint| {
                    try std.testing.expectEqualStrings("usage_records_tenant_status_key", constraint.name);
                    try std.testing.expectEqual(@as(usize, 2), constraint.columns.len);
                    try std.testing.expectEqualStrings("tenant_id", constraint.columns[0]);
                    try std.testing.expectEqualStrings("status", constraint.columns[1]);
                },
                else => return error.TestUnexpectedResult,
            }
            switch (plan.operations[3]) {
                .add_foreign_key => |foreign_key| {
                    try std.testing.expectEqualStrings("usage_records_tenant_fkey", foreign_key.name);
                    try std.testing.expectEqualStrings("tenant_id", foreign_key.child_columns[0]);
                    try std.testing.expectEqualStrings("tenants", foreign_key.parent_table);
                    try std.testing.expectEqualStrings("id", foreign_key.parent_columns[0]);
                    try std.testing.expectEqual(runtime_schema.ForeignKeyAction.set_null, foreign_key.on_delete);
                    try std.testing.expectEqual(runtime_schema.ForeignKeyValidationState.unvalidated, foreign_key.validation_state);
                },
                else => return error.TestUnexpectedResult,
            }
            switch (plan.operations[4]) {
                .add_check => |check| {
                    try std.testing.expectEqualStrings("usage_records_amount_check", check.name);
                    try std.testing.expectEqualStrings("amount", check.field);
                    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gte, check.op);
                    try std.testing.expectEqualStrings("0", check.value_json.?);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE usage_records DROP COLUMN metadata",
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE usage_records ADD COLUMN bad text GENERATED ALWAYS AS (upper(email)) STORED",
    ));
}

test "postgres sql adapter lowers alter table constraint validation ddl" {
    const alloc = std.testing.allocator;

    var add_check = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE usage_records ADD CONSTRAINT usage_records_amount_check CHECK (amount >= 0) NOT VALID;",
    );
    defer add_check.deinit(alloc);
    switch (add_check) {
        .alter_table => |plan| {
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqual(@as(usize, 1), plan.operations.len);
            switch (plan.operations[0]) {
                .add_check => |check| {
                    try std.testing.expectEqualStrings("usage_records_amount_check", check.name);
                    try std.testing.expectEqualStrings("amount", check.field);
                    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gte, check.op);
                    try std.testing.expectEqualStrings("0", check.value_json.?);
                    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.unvalidated, check.validation_state);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    var validate = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_amount_check;",
    );
    defer validate.deinit(alloc);
    switch (validate) {
        .alter_table => |plan| {
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqual(@as(usize, 1), plan.operations.len);
            switch (plan.operations[0]) {
                .validate_constraint => |constraint_name| try std.testing.expectEqualStrings("usage_records_amount_check", constraint_name),
                else => return error.TestUnexpectedResult,
            }
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers additive inline foreign key column ddl" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE usage_records ADD COLUMN IF NOT EXISTS cloud_instance_id UUID REFERENCES cloud_instances(id) ON DELETE CASCADE;",
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .alter_table => |plan| {
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqual(@as(usize, 2), plan.operations.len);
            switch (plan.operations[0]) {
                .add_column => |column| {
                    try std.testing.expectEqualStrings("cloud_instance_id", column.name);
                    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, column.field_type);
                },
                else => return error.TestUnexpectedResult,
            }
            switch (plan.operations[1]) {
                .add_foreign_key => |foreign_key| {
                    try std.testing.expectEqualStrings("cloud_instances_cloud_instance_id_fkey", foreign_key.name);
                    try std.testing.expectEqualStrings("cloud_instance_id", foreign_key.child_columns[0]);
                    try std.testing.expectEqualStrings("cloud_instances", foreign_key.parent_table);
                    try std.testing.expectEqualStrings("id", foreign_key.parent_columns[0]);
                    try std.testing.expectEqual(runtime_schema.ForeignKeyAction.cascade, foreign_key.on_delete);
                    try std.testing.expectEqual(runtime_schema.ForeignKeyValidationState.unvalidated, foreign_key.validation_state);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }

    var named = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE import_jobs ADD COLUMN cloud_instance_table_id uuid CONSTRAINT import_jobs_instance_table_fkey REFERENCES cloud_instance_tables(id) ON DELETE CASCADE;",
    );
    defer named.deinit(alloc);
    switch (named) {
        .alter_table => |plan| {
            try std.testing.expectEqual(@as(usize, 2), plan.operations.len);
            switch (plan.operations[1]) {
                .add_foreign_key => |foreign_key| {
                    try std.testing.expectEqualStrings("import_jobs_instance_table_fkey", foreign_key.name);
                    try std.testing.expectEqualStrings("cloud_instance_table_id", foreign_key.child_columns[0]);
                    try std.testing.expectEqualStrings("cloud_instance_tables", foreign_key.parent_table);
                    try std.testing.expectEqual(runtime_schema.ForeignKeyValidationState.unvalidated, foreign_key.validation_state);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .create_update_policy => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers updated-at trigger ddl into typed update policy" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records FOR EACH ROW EXECUTE FUNCTION public.touch_updated_at('updated_at_ns');",
    );
    defer lowered.deinit(alloc);

    switch (lowered) {
        .create_update_policy => |plan| {
            try std.testing.expectEqualStrings("update_timestamp", plan.trigger_name);
            try std.testing.expectEqualStrings("usage_records", plan.table_name);
            try std.testing.expectEqualStrings("updated_at_ns", plan.column_name);
            try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.now_ns, plan.on_update_value.kind);
            try std.testing.expectEqualStrings("", plan.on_update_value.value_json);
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
    }

    var default_column = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records EXECUTE PROCEDURE set_updated_at();",
    );
    defer default_column.deinit(alloc);
    switch (default_column) {
        .create_update_policy => |plan| {
            try std.testing.expectEqualStrings("updated_at", plan.column_name);
        },
        .create_table => return error.TestUnexpectedResult,
        .create_index => return error.TestUnexpectedResult,
        .alter_table => return error.TestUnexpectedResult,
    }

    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE INSERT ON usage_records EXECUTE FUNCTION touch_updated_at()",
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records EXECUTE FUNCTION arbitrary_trigger()",
    ));
}

test "postgres sql adapter applies create table ddl plan to owned runtime schema" {
    const alloc = std.testing.allocator;
    var lowered = try lowerDdlPlanAlloc(
        alloc,
        \\CREATE TABLE usage_records (
        \\  id uuid PRIMARY KEY,
        \\  tenant_id text NOT NULL,
        \\  status text DEFAULT 'open',
        \\  updated_at_ns bigint DEFAULT 0,
        \\  CONSTRAINT usage_records_tenant_key UNIQUE (tenant_id),
        \\  CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id),
        \\  CONSTRAINT usage_records_updated_check CHECK (updated_at_ns >= 0)
        \\);
        ,
    );
    defer lowered.deinit(alloc);

    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, lowered);
    defer runtime_schema.freeSchema(alloc, schema);

    try std.testing.expectEqual(runtime_schema.StorageMode.relational, schema.storage_mode);
    try std.testing.expect(schema.enforce_types);
    try std.testing.expectEqual(@as(usize, 4), schema.relational_columns.len);
    try std.testing.expect(schema.primary_key != null);
    try std.testing.expectEqualStrings("id", schema.primary_key.?.columns[0]);
    try std.testing.expectEqual(@as(usize, 1), schema.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_key", schema.unique_constraints[0].name);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.enforced, schema.unique_constraints[0].validation_state);
    try std.testing.expectEqual(@as(usize, 1), schema.foreign_keys.len);
    try std.testing.expectEqualStrings("usage_records_tenant_fkey", schema.foreign_keys[0].name);
    try std.testing.expectEqual(@as(usize, 1), schema.checks.len);
    try std.testing.expectEqualStrings("usage_records_updated_check", schema.checks[0].name);
}

test "postgres sql adapter applies additive alter table ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, tenant_id text NOT NULL, status text);",
    );
    defer create.deinit(alloc);
    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var alter = try lowerDdlPlanAlloc(
        alloc,
        \\ALTER TABLE usage_records
        \\  ADD COLUMN tenant_status_key text GENERATED ALWAYS AS (concat(tenant_id, ':', status)) STORED,
        \\  ADD CONSTRAINT usage_records_tenant_status_key UNIQUE (tenant_id, status),
        \\  ADD CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id),
        \\  ADD CONSTRAINT usage_records_status_check CHECK (status != 'deleted');
        ,
    );
    defer alter.deinit(alloc);

    const updated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, alter);
    defer runtime_schema.freeSchema(alloc, updated);

    try std.testing.expectEqual(@as(usize, 4), updated.relational_columns.len);
    const generated = relationalColumnForField(updated, "tenant_status_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.concat, generated.generated.?.op);
    try std.testing.expectEqual(@as(usize, 1), updated.unique_constraints.len);
    try std.testing.expectEqualStrings("usage_records_tenant_status_key", updated.unique_constraints[0].name);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, updated.unique_constraints[0].validation_state);
    try std.testing.expectEqual(@as(usize, 1), updated.foreign_keys.len);
    try std.testing.expectEqual(@as(usize, 1), updated.checks.len);

    var not_valid = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE usage_records ADD CONSTRAINT usage_records_status_not_deleted CHECK (status != 'deleted') NOT VALID;",
    );
    defer not_valid.deinit(alloc);
    const with_unvalidated_check = try applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, not_valid);
    defer runtime_schema.freeSchema(alloc, with_unvalidated_check);
    try std.testing.expectEqual(@as(usize, 2), with_unvalidated_check.checks.len);
    try std.testing.expectEqualStrings("usage_records_status_not_deleted", with_unvalidated_check.checks[1].name);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.unvalidated, with_unvalidated_check.checks[1].validation_state);

    var validate = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_status_not_deleted;",
    );
    defer validate.deinit(alloc);
    const validated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, with_unvalidated_check, validate);
    defer runtime_schema.freeSchema(alloc, validated);
    try std.testing.expectEqual(@as(usize, 2), validated.checks.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.enforced, validated.checks[1].validation_state);

    var duplicate = try lowerDdlPlanAlloc(alloc, "ALTER TABLE usage_records ADD COLUMN status text;");
    defer duplicate.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, applyDdlPlanToRuntimeSchemaAlloc(alloc, updated, duplicate));
}

test "postgres sql adapter applies create index ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TABLE users (id uuid PRIMARY KEY, tenant_id text NOT NULL, email text, status text, deleted_at timestamptz);",
    );
    defer create.deinit(alloc);
    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var partial_index = try lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX users_status_active_idx ON users (status DESC NULLS LAST) WHERE deleted_at IS NULL;",
    );
    defer partial_index.deinit(alloc);
    const indexed = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, partial_index);
    defer runtime_schema.freeSchema(alloc, indexed);
    const status = relationalColumnForField(indexed, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(status.indexed);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, status.index_lifecycle);
    try std.testing.expect(status.index_generation != 0);
    try std.testing.expectEqual(@as(usize, 1), status.index_where.len);
    try std.testing.expectEqualStrings("deleted_at", status.index_where[0].field);

    const indexed_again = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, partial_index);
    defer runtime_schema.freeSchema(alloc, indexed_again);
    const status_again = relationalColumnForField(indexed_again, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(status.index_generation, status_again.index_generation);

    var generated_index = try lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX users_lower_email_idx ON users (lower(email));",
    );
    defer generated_index.deinit(alloc);
    const generated_schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, indexed, generated_index);
    defer runtime_schema.freeSchema(alloc, generated_schema);
    const generated = relationalColumnForField(generated_schema, "users_lower_email_idx", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated.generated != null);
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, generated.index_lifecycle);
    try std.testing.expect(generated.index_generation != 0);
    try std.testing.expectEqual(runtime_schema.RelationalGeneratedOp.lower, generated.generated.?.op);
    try std.testing.expectEqualStrings("email", generated.generated.?.field.?);

    var unique_index = try lowerDdlPlanAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_lower_email_key ON users (tenant_id, lower(email)) WHERE deleted_at IS NULL;",
    );
    defer unique_index.deinit(alloc);
    const unique_schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, generated_schema, unique_index);
    defer runtime_schema.freeSchema(alloc, unique_schema);
    try std.testing.expectEqual(@as(usize, 1), unique_schema.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_lower_email_key", unique_schema.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 1), unique_schema.unique_constraints[0].expressions.len);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, unique_schema.unique_constraints[0].validation_state);

    var unsupported = try lowerDdlPlanAlloc(alloc, "CREATE INDEX users_tenant_status_idx ON users (tenant_id, status);");
    defer unsupported.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, unsupported));
}

test "postgres sql adapter applies updated-at trigger ddl plan to runtime schema" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TABLE usage_records (id uuid PRIMARY KEY, updated_at_ns bigint);",
    );
    defer create.deinit(alloc);
    const schema = try applyDdlPlanToRuntimeSchemaAlloc(alloc, .{}, create);
    defer runtime_schema.freeSchema(alloc, schema);

    var trigger = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
    );
    defer trigger.deinit(alloc);
    const updated = try applyDdlPlanToRuntimeSchemaAlloc(alloc, schema, trigger);
    defer runtime_schema.freeSchema(alloc, updated);

    const column = relationalColumnForField(updated, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(column.on_update_value != null);
    try std.testing.expectEqual(runtime_schema.RelationalDefaultKind.now_ns, column.on_update_value.?.kind);
}

test "postgres sql adapter compiles create table ddl plan to public schema json" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanAlloc(
        alloc,
        \\CREATE TABLE users (
        \\  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
        \\  tenant_id text NOT NULL,
        \\  email text,
        \\  attrs jsonb,
        \\  tags text[],
        \\  updated_at_ns bigint DEFAULT 0,
        \\  CONSTRAINT users_tenant_email_key UNIQUE (tenant_id, email),
        \\  CONSTRAINT users_updated_check CHECK (updated_at_ns >= 0)
        \\);
        ,
    );
    defer create.deinit(alloc);

    var applied = try applyDdlPlanToSchemaJsonAlloc(alloc, "", create);
    defer applied.deinit(alloc);
    try std.testing.expect(!applied.requires_rebuild);
    try std.testing.expect(!applied.validation_required);

    var parsed = try schema_api.parseValidatedTableSchema(alloc, applied.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, runtime);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, runtime.storage_mode);
    try std.testing.expectEqual(@as(usize, 6), runtime.relational_columns.len);
    try std.testing.expect(runtime.primary_key != null);
    try std.testing.expectEqualStrings("id", runtime.primary_key.?.columns[0]);
    try std.testing.expectEqual(@as(usize, 1), runtime.unique_constraints.len);
    try std.testing.expectEqualStrings("users_tenant_email_key", runtime.unique_constraints[0].name);
    try std.testing.expectEqual(@as(usize, 1), runtime.checks.len);
}

test "postgres sql adapter applies incremental ddl plans to public schema json" {
    const alloc = std.testing.allocator;
    var create = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TABLE users (id uuid PRIMARY KEY, tenant_id text NOT NULL, account_id text, email text, status text, deleted_at timestamptz, updated_at_ns bigint);",
    );
    defer create.deinit(alloc);
    var created = try applyDdlPlanToSchemaJsonAlloc(alloc, "", create);
    defer created.deinit(alloc);

    var status_index = try lowerDdlPlanAlloc(
        alloc,
        "CREATE INDEX users_status_idx ON users (status);",
    );
    defer status_index.deinit(alloc);
    var status_indexed = try applyDdlPlanToSchemaJsonAlloc(alloc, created.schema_json, status_index);
    defer status_indexed.deinit(alloc);
    try std.testing.expect(status_indexed.requires_rebuild);
    try std.testing.expect(!status_indexed.validation_required);

    var index = try lowerDdlPlanAlloc(
        alloc,
        "CREATE UNIQUE INDEX users_tenant_lower_email_key ON users (tenant_id, lower(email)) WHERE deleted_at IS NULL;",
    );
    defer index.deinit(alloc);
    var indexed = try applyDdlPlanToSchemaJsonAlloc(alloc, status_indexed.schema_json, index);
    defer indexed.deinit(alloc);
    try std.testing.expect(indexed.requires_rebuild);
    try std.testing.expect(indexed.validation_required);

    var alter = try lowerDdlPlanAlloc(
        alloc,
        \\ALTER TABLE users
        \\  ADD COLUMN tenant_status_key text GENERATED ALWAYS AS (concat(tenant_id, ':', status)) STORED,
        \\  ADD CONSTRAINT users_account_fkey FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE RESTRICT,
        \\  ADD CONSTRAINT users_status_check CHECK (status != 'deleted');
        ,
    );
    defer alter.deinit(alloc);
    var altered = try applyDdlPlanToSchemaJsonAlloc(alloc, indexed.schema_json, alter);
    defer altered.deinit(alloc);
    try std.testing.expect(altered.requires_rebuild);
    try std.testing.expect(altered.validation_required);

    var not_valid = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE users ADD CONSTRAINT users_status_known_check CHECK (status != 'unknown') NOT VALID;",
    );
    defer not_valid.deinit(alloc);
    var with_unvalidated_check = try applyDdlPlanToSchemaJsonAlloc(alloc, altered.schema_json, not_valid);
    defer with_unvalidated_check.deinit(alloc);
    try std.testing.expect(with_unvalidated_check.validation_required);

    var validate = try lowerDdlPlanAlloc(
        alloc,
        "ALTER TABLE ONLY users VALIDATE CONSTRAINT users_status_known_check;",
    );
    defer validate.deinit(alloc);
    var validated = try applyDdlPlanToSchemaJsonAlloc(alloc, with_unvalidated_check.schema_json, validate);
    defer validated.deinit(alloc);
    try std.testing.expect(validated.validation_required);

    var trigger = try lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER users_updated_at BEFORE UPDATE ON users EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
    );
    defer trigger.deinit(alloc);
    var updated = try applyDdlPlanToSchemaJsonAlloc(alloc, validated.schema_json, trigger);
    defer updated.deinit(alloc);
    try std.testing.expect(!updated.requires_rebuild);
    try std.testing.expect(!updated.validation_required);

    var parsed = try schema_api.parseValidatedTableSchema(alloc, updated.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, runtime);
    try std.testing.expectEqual(@as(usize, 8), runtime.relational_columns.len);
    try std.testing.expectEqual(@as(usize, 1), runtime.unique_constraints.len);
    try std.testing.expectEqual(runtime_schema.UniqueConstraintValidationState.unvalidated, runtime.unique_constraints[0].validation_state);
    try std.testing.expectEqual(@as(usize, 1), runtime.foreign_keys.len);
    try std.testing.expectEqualStrings("users_account_fkey", runtime.foreign_keys[0].name);
    try std.testing.expectEqual(runtime_schema.ForeignKeyValidationState.unvalidated, runtime.foreign_keys[0].validation_state);
    try std.testing.expectEqual(@as(usize, 2), runtime.checks.len);
    try std.testing.expectEqualStrings("users_status_known_check", runtime.checks[1].name);
    try std.testing.expectEqual(runtime_schema.RelationalCheckValidationState.enforced, runtime.checks[1].validation_state);
    const status = relationalColumnForField(runtime, "status", null) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema.RelationalIndexLifecycle.building, status.index_lifecycle);
    try std.testing.expect(status.index_generation != 0);
    const generated = relationalColumnForField(runtime, "tenant_status_key", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(generated.generated != null);
    const updated_at = relationalColumnForField(runtime, "updated_at_ns", null) orelse return error.TestUnexpectedResult;
    try std.testing.expect(updated_at.on_update_value != null);
}

test "postgres sql adapter rejects unsupported ddl shapes explicitly" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "CREATE TABLE audit_log (id uuid, payload jsonb)",
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(
        alloc,
        "CREATE TRIGGER audit_row AFTER UPDATE ON usage_records EXECUTE FUNCTION audit_changes()",
    ));
}

test "postgres sql adapter lowers application queue select into row claim query" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"billing_cycle_start":{"type":"datetime"},"metric_type":{"type":"keyword"},"bucket_start":{"type":"datetime"},"created_at":{"type":"datetime"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id, metric_type FROM usage_records WHERE status = $1 ORDER BY billing_cycle_start ASC, metric_type ASC, bucket_start ASC, id ASC LIMIT $2 FOR UPDATE SKIP LOCKED",
        schema,
        &.{ .{ .string = "unrated" }, .{ .integer = 100 } },
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.predicates[0].op);
    try std.testing.expectEqualStrings("\"unrated\"", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 4), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("billing_cycle_start", lowered.query.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 100), lowered.query.limit.?);
    try std.testing.expect(lowered.query.row_claim != null);
    try std.testing.expect(lowered.query.row_claim.?.skip_locked);
}

test "postgres sql adapter lowers json text extraction predicate" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"},"created_at":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE metadata->>'source' = 'autoscale_delta' ORDER BY created_at DESC LIMIT 1",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_path_eq.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_path_eq[0].field);
    try std.testing.expectEqualStrings("source", lowered.query.json_path_eq[0].path);
    try std.testing.expectEqualStrings("\"autoscale_delta\"", lowered.query.json_path_eq[0].value_json);
    try std.testing.expectEqualStrings("created_at", lowered.query.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.query.order_by[0].direction);
}

test "postgres sql adapter lowers jsonb containment existence and extraction projection" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"},"created_at":{"type":"datetime"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT metadata->>'source' AS source FROM usage_records WHERE metadata @> $1::jsonb AND metadata ? 'flags' ORDER BY created_at DESC LIMIT 5",
        schema,
        &.{.{ .json = "{\"billing\":{\"plan\":\"pro\"}}" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.json_extract.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("source", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.json_extract, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqualStrings("source", lowered.query.expressions[0].expression.json_path);
    try std.testing.expect(lowered.query.expressions[0].expression.json_as_text);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.expressions[0].expression.operands[0].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_contains.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_contains[0].field);
    try std.testing.expectEqualStrings("{\"billing\":{\"plan\":\"pro\"}}", lowered.query.json_contains[0].value_json);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_path_exists.len);
    try std.testing.expectEqualStrings("metadata", lowered.query.json_path_exists[0].field);
    try std.testing.expectEqualStrings("flags", lowered.query.json_path_exists[0].path);
    try std.testing.expectEqual(@as(u32, 5), lowered.query.limit.?);
}

test "postgres sql adapter accepts casted jsonb document literals" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE metadata @> '{\"source\":\"autoscale_delta\"}'::jsonb",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.json_contains.len);
    try std.testing.expectEqualStrings("{\"source\":\"autoscale_delta\"}", lowered.query.json_contains[0].value_json);
}

test "postgres sql adapter lowers array containment and equality predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var contains = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE tags @> $1::text[]",
        schema,
        &.{.{ .json = "[\"hot\",\"new\"]" }},
    );
    defer contains.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), contains.query.array_contains.len);
    try std.testing.expectEqualStrings("tags", contains.query.array_contains[0].field);
    try std.testing.expectEqualStrings("[\"hot\",\"new\"]", contains.query.array_contains[0].value_json);

    var eq = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE tags = $1::text[]",
        schema,
        &.{.{ .json = "[\"hot\"]" }},
    );
    defer eq.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), eq.query.array_eq.len);
    try std.testing.expectEqualStrings("tags", eq.query.array_eq[0].field);
    try std.testing.expectEqualStrings("[\"hot\"]", eq.query.array_eq[0].value_json);
}

test "postgres sql adapter lowers array_length projection" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id, array_length(tags, 1) AS tag_count FROM usage_records ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.array_length.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("tag_count", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.array_length, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqualStrings("tags", lowered.query.expressions[0].expression.operands[0].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("id", lowered.query.order_by[0].field);
}

test "postgres sql adapter lowers coalesce projection" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"display_name":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id, COALESCE(display_name, email, 'unknown') AS name_or_email FROM users ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expect(!lowered.query.select_all);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.query.select[0]);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.coalesce.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("name_or_email", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.coalesce, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 3), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, lowered.query.expressions[0].expression.operands[0].kind);
    try std.testing.expectEqualStrings("display_name", lowered.query.expressions[0].expression.operands[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.value, lowered.query.expressions[0].expression.operands[2].kind);
    try std.testing.expectEqualStrings("\"unknown\"", lowered.query.expressions[0].expression.operands[2].value_json);

    try std.testing.expectError(error.InvalidSqlCatalog, lowerSelectAlloc(
        alloc,
        "SELECT COALESCE(missing, 'unknown') AS display FROM users",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers scalar any predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = ANY($1::text[])",
        schema,
        &.{.{ .json = "[\"active\",\"pending\"]" }},
    );
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.in_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"active\",\"pending\"]", lowered.query.in_predicates[0].values_json);
    try std.testing.expect(!lowered.query.in_predicates[0].negated);

    var negated = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE NOT (status = ANY($1::text[]))",
        schema,
        &.{.{ .json = "[\"disabled\"]" }},
    );
    defer negated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), negated.query.in_predicates.len);
    try std.testing.expectEqualStrings("status", negated.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"disabled\"]", negated.query.in_predicates[0].values_json);
    try std.testing.expect(negated.query.in_predicates[0].negated);
}

test "postgres sql adapter lowers scalar in predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"priority":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var literal = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN ('active', 'pending')",
        schema,
        &.{},
    );
    defer literal.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), literal.query.in_predicates.len);
    try std.testing.expectEqualStrings("status", literal.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[\"active\",\"pending\"]", literal.query.in_predicates[0].values_json);
    try std.testing.expect(!literal.query.in_predicates[0].negated);

    var array_param = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN ($1::text[])",
        schema,
        &.{.{ .json = "[\"active\",\"pending\"]" }},
    );
    defer array_param.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), array_param.query.in_predicates.len);
    try std.testing.expectEqualStrings("[\"active\",\"pending\"]", array_param.query.in_predicates[0].values_json);

    var negated = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE priority NOT IN (1, 2, $1)",
        schema,
        &.{.{ .integer = 3 }},
    );
    defer negated.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), negated.query.in_predicates.len);
    try std.testing.expectEqualStrings("priority", negated.query.in_predicates[0].field);
    try std.testing.expectEqualStrings("[1,2,3]", negated.query.in_predicates[0].values_json);
    try std.testing.expect(negated.query.in_predicates[0].negated);
}

test "postgres sql adapter lowers null-safe distinct predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"encryption_key_version":{"type":"numeric"},"encrypted_secret":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM secrets WHERE \"encryption_key_version\" IS NOT DISTINCT FROM $1 AND \"encrypted_secret\" IS NOT DISTINCT FROM $2",
        schema,
        &.{ .{ .integer = 7 }, .null },
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("encryption_key_version", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.predicates[0].op);
    try std.testing.expectEqualStrings("7", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqualStrings("encrypted_secret", lowered.query.predicates[1].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, lowered.query.predicates[1].op);
    try std.testing.expect(lowered.query.predicates[1].value_json == null);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM secrets WHERE encrypted_secret IS DISTINCT FROM $1",
        schema,
        &.{.{ .string = "old" }},
    ));
}

test "postgres sql adapter lowers scalar or predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status = 'closed' OR status = 'open' AND amount > 20 ORDER BY created_at DESC",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.predicates.len);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.or_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.or_predicates[0].predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.or_predicates[0].predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.or_predicates[0].predicates[0].op);
    try std.testing.expectEqualStrings("\"closed\"", lowered.query.or_predicates[0].predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.or_predicates[1].predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.or_predicates[1].predicates[0].field);
    try std.testing.expectEqualStrings("amount", lowered.query.or_predicates[1].predicates[1].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.query.or_predicates[1].predicates[1].op);
    try std.testing.expectEqualStrings("20", lowered.query.or_predicates[1].predicates[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE status IN ('closed') OR status = 'open'",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers scalar not predicate groups" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE NOT (status = 'closed' AND amount > 20) AND created_at >= 10",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("created_at", lowered.query.predicates[0].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.not_predicates.len);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.not_predicates[0].predicates.len);
    try std.testing.expectEqualStrings("status", lowered.query.not_predicates[0].predicates[0].field);
    try std.testing.expectEqualStrings("amount", lowered.query.not_predicates[0].predicates[1].field);
}

test "postgres sql adapter lowers null-test order expressions" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"expires_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM jobs ORDER BY (expires_at IS NULL), expires_at ASC, id ASC LIMIT 5",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("expires_at", lowered.query.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderNullTest.is_null, lowered.query.order_by[0].null_test.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.asc, lowered.query.order_by[0].direction);
    try std.testing.expectEqualStrings("expires_at", lowered.query.order_by[1].field);
    try std.testing.expect(lowered.query.order_by[1].null_test == null);
    try std.testing.expectEqualStrings("id", lowered.query.order_by[2].field);
    try std.testing.expectEqual(@as(u32, 5), lowered.query.limit.?);
}

test "postgres sql adapter lowers now in scalar predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"created_at_ns":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM usage_records WHERE created_at_ns <= NOW() ORDER BY id",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("created_at_ns", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.lte, lowered.query.predicates[0].op);
    const now_value = try std.fmt.parseInt(u64, lowered.query.predicates[0].value_json.?, 10);
    try std.testing.expect(now_value > 0);
}

test "postgres sql adapter lowers grouped aggregate queries" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(*) AS order_count, SUM(amount) AS amount_sum, AVG(amount) AS amount_avg FROM usage_records WHERE status = $1 GROUP BY customer HAVING amount_sum > 10 ORDER BY amount_sum DESC LIMIT 10",
        schema,
        &.{.{ .string = "open" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.group_by.len);
    try std.testing.expectEqualStrings("customer", lowered.aggregate.group_by[0]);
    try std.testing.expectEqual(@as(usize, 3), lowered.aggregate.aggregations.len);
    try std.testing.expectEqualStrings("order_count", lowered.aggregate.aggregations[0].name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].field == null);
    try std.testing.expectEqualStrings("amount_sum", lowered.aggregate.aggregations[1].name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.avg, lowered.aggregate.aggregations[2].op);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.source.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.source.predicates[0].field);
    try std.testing.expectEqualStrings("\"open\"", lowered.aggregate.source.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.having_predicates.len);
    try std.testing.expectEqualStrings("amount_sum", lowered.aggregate.having_predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.aggregate.having_predicates[0].op);
    try std.testing.expectEqualStrings("10", lowered.aggregate.having_predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.order_by.len);
    try std.testing.expectEqualStrings("amount_sum", lowered.aggregate.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.aggregate.order_by[0].direction);
    try std.testing.expectEqual(@as(u32, 10), lowered.aggregate.limit.?);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(*) AS order_count FROM usage_records GROUP BY customer HAVING missing_alias > 0",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers filtered aggregate predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(*) FILTER (WHERE status = 'open') AS open_count, SUM(amount) FILTER (WHERE status = 'open' AND amount > 10) AS open_amount_sum, COUNT(*) FILTER (WHERE lower(status) = 'open') AS open_lower_count FROM usage_records GROUP BY customer ORDER BY open_amount_sum DESC LIMIT 10",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[0].filter_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[0].filter_predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.aggregate.aggregations[0].filter_predicates[0].op);
    try std.testing.expectEqualStrings("\"open\"", lowered.aggregate.aggregations[0].filter_predicates[0].value_json.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.aggregate.aggregations[1].filter_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[1].filter_predicates[0].field);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].filter_predicates[1].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.aggregate.aggregations[1].filter_predicates[1].op);
    try std.testing.expectEqualStrings("10", lowered.aggregate.aggregations[1].filter_predicates[1].value_json.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[2].op);
    try std.testing.expectEqual(@as(usize, 0), lowered.aggregate.aggregations[2].filter_predicates.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[2].filter_expressions.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.aggregate.aggregations[2].filter_expressions[0].lhs.kind);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.aggregate.aggregations[2].filter_expressions[0].op);
}

test "postgres sql adapter lowers distinct aggregate specs" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(DISTINCT status) AS status_count, SUM(DISTINCT amount) FILTER (WHERE status = 'open') AS open_amount_sum FROM usage_records GROUP BY customer",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].distinct);
    try std.testing.expectEqual(db_mod.types.default_relational_rows_aggregate_distinct_max_items, lowered.aggregate.aggregations[0].distinct_max_items);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[0].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expect(lowered.aggregate.aggregations[1].distinct);
    try std.testing.expectEqual(db_mod.types.default_relational_rows_aggregate_distinct_max_items, lowered.aggregate.aggregations[1].distinct_max_items);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[1].filter_predicates.len);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[1].filter_predicates[0].field);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateAlloc(
        alloc,
        "SELECT COUNT(DISTINCT *) AS row_count FROM usage_records",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers aggregate expression inputs" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"},"discount":{"type":"numeric"}},"required":["id","status","customer","amount","discount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, COUNT(DISTINCT lower(status)) AS status_count, SUM(amount - discount) AS net_amount FROM usage_records GROUP BY customer",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].distinct);
    try std.testing.expect(lowered.aggregate.aggregations[0].field == null);
    try std.testing.expect(lowered.aggregate.aggregations[0].expression != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.aggregate.aggregations[0].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.aggregate.aggregations[1].op);
    try std.testing.expect(lowered.aggregate.aggregations[1].field == null);
    try std.testing.expect(lowered.aggregate.aggregations[1].expression != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, lowered.aggregate.aggregations[1].expression.?.kind);
}

test "postgres sql adapter lowers bounded array aggregate specs" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"customer":{"type":"keyword"},"amount":{"type":"numeric"},"metadata":{"type":"json"}},"required":["id","status","customer","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT customer, ARRAY_AGG(DISTINCT status ORDER BY amount DESC) FILTER (WHERE amount > 10) AS statuses FROM usage_records GROUP BY customer",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.array_agg, lowered.aggregate.aggregations[0].op);
    try std.testing.expect(lowered.aggregate.aggregations[0].distinct);
    try std.testing.expectEqual(db_mod.types.default_relational_rows_aggregate_distinct_max_items, lowered.aggregate.aggregations[0].distinct_max_items);
    try std.testing.expectEqualStrings("status", lowered.aggregate.aggregations[0].field.?);
    try std.testing.expectEqual(default_array_agg_max_items, lowered.aggregate.aggregations[0].array_max_items);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[0].array_order_by.len);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[0].array_order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.aggregate.aggregations[0].array_order_by[0].direction);
    try std.testing.expectEqual(@as(usize, 1), lowered.aggregate.aggregations[0].filter_predicates.len);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[0].filter_predicates[0].field);

    try std.testing.expectError(error.InvalidSqlCatalog, lowerAggregateAlloc(
        alloc,
        "SELECT customer, ARRAY_AGG(metadata) AS metadata_values FROM usage_records GROUP BY customer",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers global aggregate queries" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","amount"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerAggregateAlloc(
        alloc,
        "SELECT COUNT(*) AS row_count, MIN(amount) AS min_amount, MAX(amount) AS max_amount FROM usage_records",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.aggregate.group_by.len);
    try std.testing.expectEqual(@as(usize, 3), lowered.aggregate.aggregations.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.count, lowered.aggregate.aggregations[0].op);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.min, lowered.aggregate.aggregations[1].op);
    try std.testing.expectEqualStrings("amount", lowered.aggregate.aggregations[1].field.?);
    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.max, lowered.aggregate.aggregations[2].op);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerAggregateAlloc(
        alloc,
        "SELECT id, COUNT(*) AS row_count FROM usage_records",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers equality join queries" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerJoinAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name, o.amount AS amount FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' ORDER BY amount DESC LIMIT 5",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
    try std.testing.expectEqualStrings("usage_records", lowered.right_table_name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinType.left, lowered.join.join_type);
    try std.testing.expectEqual(@as(usize, 2), lowered.join.on.len);
    try std.testing.expectEqualStrings("tenant", lowered.join.on[0].left_field);
    try std.testing.expectEqualStrings("tenant", lowered.join.on[0].right_field);
    try std.testing.expectEqualStrings("customer_id", lowered.join.on[1].left_field);
    try std.testing.expectEqualStrings("id", lowered.join.on[1].right_field);
    try std.testing.expectEqual(@as(usize, 1), lowered.join.left.predicates.len);
    try std.testing.expectEqualStrings("kind", lowered.join.left.predicates[0].field);
    try std.testing.expectEqualStrings("\"order\"", lowered.join.left.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.join.right.predicates.len);
    try std.testing.expectEqualStrings("kind", lowered.join.right.predicates[0].field);
    try std.testing.expectEqualStrings("\"customer\"", lowered.join.right.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 3), lowered.join.select.len);
    try std.testing.expectEqualStrings("order_id", lowered.join.select[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, lowered.join.select[0].side);
    try std.testing.expectEqualStrings("id", lowered.join.select[0].field);
    try std.testing.expectEqualStrings("customer_name", lowered.join.select[1].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.right, lowered.join.select[1].side);
    try std.testing.expectEqualStrings("name", lowered.join.select[1].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.join.order_by.len);
    try std.testing.expectEqualStrings("amount", lowered.join.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.join.order_by[0].direction);
    try std.testing.expectEqual(@as(u32, 5), lowered.join.limit.?);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerJoinAlloc(
        alloc,
        "SELECT o.id AS order_id FROM usage_records AS o JOIN usage_records AS c ON o.tenant = o.id",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers bounded left join lateral queries" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerLateralPlanAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY created_at DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC LIMIT 10",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
    try std.testing.expectEqualStrings("usage_records", lowered.right_table_name);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.left.predicates.len);
    try std.testing.expectEqualStrings("kind", lowered.plan.lateral.left.predicates[0].field);
    try std.testing.expectEqualStrings("\"organization\"", lowered.plan.lateral.left.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.right.predicates.len);
    try std.testing.expectEqualStrings("kind", lowered.plan.lateral.right.predicates[0].field);
    try std.testing.expectEqualStrings("\"balance\"", lowered.plan.lateral.right.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.correlations.len);
    try std.testing.expectEqualStrings("id", lowered.plan.lateral.correlations[0].left_field);
    try std.testing.expectEqualStrings("organization_id", lowered.plan.lateral.correlations[0].right_field);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.right.order_by.len);
    try std.testing.expectEqualStrings("created_at", lowered.plan.lateral.right.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.plan.lateral.right.order_by[0].direction);
    try std.testing.expectEqual(@as(u32, 1), lowered.plan.lateral.right.limit.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.plan.lateral.select.len);
    try std.testing.expectEqualStrings("organization_id", lowered.plan.lateral.select[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, lowered.plan.lateral.select[0].side);
    try std.testing.expectEqualStrings("latest_amount", lowered.plan.lateral.select[1].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.right, lowered.plan.lateral.select[1].side);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.order_by.len);
    try std.testing.expectEqualStrings("latest_amount", lowered.plan.lateral.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 10), lowered.plan.lateral.limit.?);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerLateralPlanAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount FROM usage_records AS bal WHERE bal.organization_id = org.id) AS latest ON true",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers generated lower expressions for query pushdown" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"email_key":{"type":"keyword","generated":{"op":"lower","field":"email"}}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT lower(email) AS email_key FROM users WHERE lower(email) = $1 ORDER BY lower(email) ASC",
        schema,
        &.{.{ .string = "ada@example.test" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("email_key", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, lowered.query.expressions[0].expression.operands[0].kind);
    try std.testing.expectEqualStrings("email", lowered.query.expressions[0].expression.operands[0].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("email_key", lowered.query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, lowered.query.predicates[0].op);
    try std.testing.expectEqualStrings("\"ada@example.test\"", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("email_key", lowered.query.order_by[0].field);
}

test "postgres sql adapter lowers scalar expression order keys" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"amount":{"type":"numeric"},"discount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT id FROM users ORDER BY lower(email) ASC, amount - discount DESC LIMIT 10",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.query.order_by.len);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.order_by[0].field.len);
    try std.testing.expect(lowered.query.order_by[0].expression != null);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.query.order_by[0].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.asc, lowered.query.order_by[0].direction);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, lowered.query.order_by[1].expression.?.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.query.order_by[1].direction);
}

test "postgres sql adapter lowers concat projection into expression AST" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"first_name":{"type":"keyword"},"last_name":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT concat(first_name, ' ', last_name, ' <', lower(email), '>') AS display_label FROM users WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("display_label", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.concat, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 6), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, lowered.query.expressions[0].expression.operands[0].kind);
    try std.testing.expectEqualStrings("first_name", lowered.query.expressions[0].expression.operands[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.value, lowered.query.expressions[0].expression.operands[1].kind);
    try std.testing.expectEqualStrings("\" \"", lowered.query.expressions[0].expression.operands[1].value_json);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.query.expressions[0].expression.operands[4].kind);
    try std.testing.expectEqualStrings("email", lowered.query.expressions[0].expression.operands[4].operands[0].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("id", lowered.query.predicates[0].field);
}

test "postgres sql adapter lowers nullif projection into expression AST" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT nullif(lower(email), 'blocked@example.test') AS usable_email FROM users WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("usable_email", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.nullif, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, lowered.query.expressions[0].expression.operands[0].kind);
    try std.testing.expectEqualStrings("email", lowered.query.expressions[0].expression.operands[0].operands[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.value, lowered.query.expressions[0].expression.operands[1].kind);
    try std.testing.expectEqualStrings("\"blocked@example.test\"", lowered.query.expressions[0].expression.operands[1].value_json);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("id", lowered.query.predicates[0].field);
}

test "postgres sql adapter lowers arithmetic projection into expression AST" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"tax":{"type":"numeric"},"rate":{"type":"numeric"},"discount":{"type":"numeric"},"divisor":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT amount + tax * rate - discount / divisor AS net_amount FROM invoices WHERE id = $1",
        schema,
        &.{.{ .string = "inv1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("net_amount", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.sub, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(@as(usize, 2), lowered.query.expressions[0].expression.operands.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, lowered.query.expressions[0].expression.operands[0].kind);
    try std.testing.expectEqualStrings("amount", lowered.query.expressions[0].expression.operands[0].operands[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.mul, lowered.query.expressions[0].expression.operands[0].operands[1].kind);
    try std.testing.expectEqualStrings("tax", lowered.query.expressions[0].expression.operands[0].operands[1].operands[0].field);
    try std.testing.expectEqualStrings("rate", lowered.query.expressions[0].expression.operands[0].operands[1].operands[1].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.div, lowered.query.expressions[0].expression.operands[1].kind);
    try std.testing.expectEqualStrings("discount", lowered.query.expressions[0].expression.operands[1].operands[0].field);
    try std.testing.expectEqualStrings("divisor", lowered.query.expressions[0].expression.operands[1].operands[1].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("id", lowered.query.predicates[0].field);

    try std.testing.expectError(error.InvalidSqlCatalog, lowerSelectAlloc(
        alloc,
        "SELECT amount + id AS bad FROM invoices WHERE id = $1",
        schema,
        &.{.{ .string = "inv1" }},
    ));
}

test "postgres sql adapter lowers case projection into expression AST" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT CASE WHEN email IS NULL THEN 'missing' WHEN email = 'blocked@example.test' THEN 'blocked' ELSE lower(status) END AS email_bucket FROM usage_records WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("email_bucket", lowered.query.expressions[0].output);
    const expression = lowered.query.expressions[0].expression;
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, expression.kind);
    try std.testing.expectEqual(@as(usize, 2), expression.case_branches.len);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.is_null, expression.case_branches[0].when.op);
    try std.testing.expectEqual(@as(usize, 0), expression.case_branches[0].when.rhs.len);
    try std.testing.expectEqualStrings("\"missing\"", expression.case_branches[0].then.value_json);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.eq, expression.case_branches[1].when.op);
    try std.testing.expectEqualStrings("email", expression.case_branches[1].when.lhs.field);
    try std.testing.expectEqual(@as(usize, 1), expression.case_branches[1].when.rhs.len);
    try std.testing.expectEqualStrings("\"blocked@example.test\"", expression.case_branches[1].when.rhs[0].value_json);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.lower, expression.case_else[0].kind);
    try std.testing.expectEqualStrings("status", expression.case_else[0].operands[0].field);
}

test "postgres sql adapter lowers cast projection into expression AST" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"},"active":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT CAST(id AS text) AS id_text, CAST(amount AS text) AS amount_text, CAST(active AS bool) AS active_bool FROM usage_records WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 3), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("id_text", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.cast, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.text, lowered.query.expressions[0].expression.cast_type.?);
    try std.testing.expectEqualStrings("id", lowered.query.expressions[0].expression.operands[0].field);
    try std.testing.expectEqualStrings("amount_text", lowered.query.expressions[1].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.text, lowered.query.expressions[1].expression.cast_type.?);
    try std.testing.expectEqualStrings("active_bool", lowered.query.expressions[2].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.bool, lowered.query.expressions[2].expression.cast_type.?);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT CAST(id AS timestamp) AS bad FROM usage_records WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
    ));
}

test "postgres sql adapter ignores harmless identifier casts" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerSelectAlloc(
        alloc,
        "SELECT \"id\"::text AS id_text FROM users WHERE id::text = $1 ORDER BY \"status\"::text DESC",
        schema,
        &.{.{ .string = "u1" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 0), lowered.query.select.len);
    try std.testing.expectEqual(@as(usize, 0), lowered.query.field_aliases.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.expressions.len);
    try std.testing.expectEqualStrings("id_text", lowered.query.expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.field, lowered.query.expressions[0].expression.kind);
    try std.testing.expectEqualStrings("id", lowered.query.expressions[0].expression.field);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.predicates.len);
    try std.testing.expectEqualStrings("id", lowered.query.predicates[0].field);
    try std.testing.expectEqualStrings("\"u1\"", lowered.query.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.query.order_by.len);
    try std.testing.expectEqualStrings("status", lowered.query.order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.query.order_by[0].direction);
}

test "postgres sql adapter rejects lower predicate without generated column" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM users WHERE lower(email) = $1",
        schema,
        &.{.{ .string = "ada@example.test" }},
    ));
}

test "postgres sql adapter lowers insert values returning into row batch" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, metadata) VALUES ($1, $2, $3::jsonb) RETURNING id, status, metadata",
        schema,
        &.{
            .{ .string = "u1" },
            .{ .string = "pending" },
            .{ .json = "{\"source\":\"autoscale_delta\"}" },
        },
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);

    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("u1", returned.value.object.get("id").?.string);
    try std.testing.expectEqualStrings("pending", returned.value.object.get("status").?.string);
    try std.testing.expectEqualStrings("autoscale_delta", returned.value.object.get("metadata").?.object.get("source").?.string);
}

test "postgres sql adapter lowers insert jsonb literal" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', '{\"source\":\"literal\"}'::jsonb) RETURNING *",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("literal", returned.value.object.get("metadata").?.object.get("source").?.string);
}

test "postgres sql adapter lowers jsonb_build_object insert values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', jsonb_build_object('source', $1, 'count', 3, 'active', true)) RETURNING metadata",
        schema,
        &.{.{ .string = "builder" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    const metadata = returned.value.object.get("metadata").?.object;
    try std.testing.expectEqualStrings("builder", metadata.get("source").?.string);
    try std.testing.expectEqual(@as(i64, 3), metadata.get("count").?.integer);
    try std.testing.expect(metadata.get("active").?.bool);
}

test "postgres sql adapter lowers convert_from jsonb insert values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, metadata) VALUES ('u1', convert_from($1, 'UTF8')::jsonb) RETURNING metadata",
        schema,
        &.{.{ .string = "{\"source\":\"converted\",\"count\":4}" }},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"converted\",\"count\":4}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers now insert values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"created_at_ns":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, created_at_ns) VALUES ('u1', NOW()) RETURNING created_at_ns",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("created_at_ns").?) {
        .integer => |value| try std.testing.expect(value > 0),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers explicit default insert values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"},"created_at_ns":{"type":"numeric","x-antfly-default":{"op":"now_ns"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, status, created_at_ns) VALUES ('u1', DEFAULT, DEFAULT) RETURNING status, created_at_ns",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("active", returned.value.object.get("status").?.string);
    switch (returned.value.object.get("created_at_ns").?) {
        .integer => |value| try std.testing.expect(value > 0),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter rejects default without column default" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', DEFAULT)",
        schema,
        &.{},
    ));
}

const TestPrimaryResolver = struct {
    row_json: []const u8,
    version: u64,
    exists: bool = true,

    fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolve,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolve(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
    ) anyerror!?[]u8 {
        _ = table_name;
        _ = constraint_name;
        _ = encoded_value;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!self.exists) return null;
        return try alloc.dupe(u8, "test-existing-primary");
    }

    fn primaryExists(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!bool {
        _ = alloc;
        _ = table_name;
        _ = physical_key;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.exists;
    }

    fn lookupPrimary(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        physical_key: []const u8,
    ) anyerror!?relational_rows.ResolvedPrimaryRow {
        _ = table_name;
        if (physical_key.len == 0) return null;
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return .{
            .json = try alloc.dupe(u8, self.row_json),
            .version = self.version,
        };
    }
};

const AppCompatCorpusPlanFamily = enum {
    ddl,
    query,
    aggregate,
    join,
    lateral,
    window,
    insert,
    update,
    delete,
    update_source,
    delete_source,
    unsupported,
    unsupported_ddl,
    unsupported_update,
};

const AppCompatDdlTag = enum {
    create_table,
    create_index,
    alter_table,
    create_update_policy,
};

const AppCompatPlanSummary = struct {
    ddl_tag: ?AppCompatDdlTag = null,
    table_name: ?[]const u8 = null,
    ctes: ?usize = null,
    predicates: ?usize = null,
    json_path_eq: ?usize = null,
    select: ?usize = null,
    order_by: ?usize = null,
    limit: ?u32 = null,
    group_by: ?usize = null,
    aggregations: ?usize = null,
    having: ?usize = null,
    operations: ?usize = null,
    returning: ?usize = null,
    join_on: ?usize = null,
    join_select: ?usize = null,
    lateral_correlations: ?usize = null,
    windows: ?usize = null,
    row_claim_skip_locked: ?bool = null,
};

const AppCompatCorpusEntry = struct {
    name: []const u8,
    sql: []const u8,
    family: AppCompatCorpusPlanFamily,
    params: []const SqlValue = &.{},
    summary: AppCompatPlanSummary = .{},
    plan: []const u8 = "",
    apply_setup_sql: []const []const u8 = &.{},
    applied_plan: []const u8 = "",
};

fn expectOptionalUsize(expected: ?usize, actual: usize) !void {
    if (expected) |value| try std.testing.expectEqual(value, actual);
}

fn expectOptionalU32(expected: ?u32, actual: ?u32) !void {
    if (expected) |value| try std.testing.expectEqual(value, actual orelse return error.TestUnexpectedResult);
}

fn expectOptionalTableName(expected: ?[]const u8, actual: []const u8) !void {
    if (expected) |value| try std.testing.expectEqualStrings(value, actual);
}

fn expectAppCompatPlan(expected: []const u8, actual: []const u8) !void {
    if (expected.len == 0) return;
    try std.testing.expectEqualStrings(expected, actual);
}

fn appCompatLimitValue(limit: ?u32) i64 {
    return if (limit) |value| @intCast(value) else -1;
}

fn expressionOrderCount(order_by: []const db_mod.types.RelationalRowsQueryOrder) usize {
    var count: usize = 0;
    for (order_by) |order| {
        if (order.expression != null) count += 1;
    }
    return count;
}

fn aggregateFilterExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expressions.len;
    }
    return count;
}

fn aggregateInputExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.expression != null) count += 1;
    }
    return count;
}

fn queryFingerprintAlloc(alloc: std.mem.Allocator, family: []const u8, table_name: []const u8, query: db_mod.types.RelationalRowsQueryRequest, ctes: usize) ![]u8 {
    const claim = if (query.row_claim) |claim_value| if (claim_value.skip_locked) "skip_locked" else "locked" else "none";
    const order_expr = expressionOrderCount(query.order_by);
    if (query.limit) |limit| {
        return try std.fmt.allocPrint(
            alloc,
            "{s}:table={s}:ctes={d}:pred={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit={d}:claim={s}",
            .{
                family,
                table_name,
                ctes,
                query.predicates.len,
                query.json_path_eq.len,
                query.or_predicates.len,
                query.not_predicates.len,
                query.select.len,
                query.expressions.len,
                query.field_aliases.len,
                query.order_by.len,
                order_expr,
                limit,
                claim,
            },
        );
    }
    return try std.fmt.allocPrint(
        alloc,
        "{s}:table={s}:ctes={d}:pred={d}:json_eq={d}:or={d}:not={d}:select={d}:expr={d}:alias={d}:order={d}:order_expr={d}:limit=none:claim={s}",
        .{
            family,
            table_name,
            ctes,
            query.predicates.len,
            query.json_path_eq.len,
            query.or_predicates.len,
            query.not_predicates.len,
            query.select.len,
            query.expressions.len,
            query.field_aliases.len,
            query.order_by.len,
            order_expr,
            claim,
        },
    );
}

fn ddlFingerprintAlloc(alloc: std.mem.Allocator, lowered: LoweredDdlPlan) ![]u8 {
    return switch (lowered) {
        .create_table => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:create_table:table={s}:columns={d}:unique={d}:fk={d}:checks={d}",
            .{ plan.table_name, plan.columns.len, plan.unique_constraints.len, plan.foreign_keys.len, plan.checks.len },
        ),
        .create_index => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:create_index:table={s}:columns={d}:expr={d}:where={d}:unique={}",
            .{ plan.table_name, plan.columns.len, plan.expressions.len, plan.where.len, plan.unique },
        ),
        .alter_table => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:alter_table:table={s}:ops={d}",
            .{ plan.table_name, plan.operations.len },
        ),
        .create_update_policy => |plan| try std.fmt.allocPrint(
            alloc,
            "ddl:create_update_policy:table={s}:column={s}",
            .{ plan.table_name, plan.column_name },
        ),
    };
}

fn ddlAppliedFingerprintAlloc(alloc: std.mem.Allocator, applied: AppliedDdlSchemaJson) ![]u8 {
    var parsed = try schema_api.parseValidatedTableSchema(alloc, applied.schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var building_indexes: usize = 0;
    var update_policies: usize = 0;
    for (schema.relational_columns) |column| {
        if (column.index_lifecycle != .ready) building_indexes += 1;
        if (column.on_update_value != null) update_policies += 1;
    }

    var unvalidated_unique: usize = 0;
    for (schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) unvalidated_unique += 1;
    }
    var unvalidated_fk: usize = 0;
    for (schema.foreign_keys) |foreign_key| {
        if (foreign_key.validation_state != .enforced) unvalidated_fk += 1;
    }
    var unvalidated_check: usize = 0;
    for (schema.checks) |check| {
        if (check.validation_state != .enforced) unvalidated_check += 1;
    }

    return try std.fmt.allocPrint(
        alloc,
        "applied:rebuild={}:validation={}:building_indexes={d}:unvalidated_unique={d}:unvalidated_fk={d}:unvalidated_check={d}:update_policy={d}",
        .{
            applied.requires_rebuild,
            applied.validation_required,
            building_indexes,
            unvalidated_unique,
            unvalidated_fk,
            unvalidated_check,
            update_policies,
        },
    );
}

fn expectAppliedDdlCorpusPlan(
    alloc: std.mem.Allocator,
    base_schema_json: []const u8,
    entry: AppCompatCorpusEntry,
    lowered: LoweredDdlPlan,
) !void {
    if (entry.applied_plan.len == 0) return;

    var current_schema_json: []const u8 = base_schema_json;
    var owned_current_schema_json: ?[]u8 = null;
    defer if (owned_current_schema_json) |schema_json| alloc.free(schema_json);

    if (entry.apply_setup_sql.len > 0) current_schema_json = "";
    for (entry.apply_setup_sql) |setup_sql| {
        var setup_plan = try lowerDdlPlanAlloc(alloc, setup_sql);
        defer setup_plan.deinit(alloc);
        const setup_applied = try applyDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, setup_plan);
        if (owned_current_schema_json) |schema_json| alloc.free(schema_json);
        owned_current_schema_json = setup_applied.schema_json;
        current_schema_json = setup_applied.schema_json;
    }

    var applied = try applyDdlPlanToSchemaJsonAlloc(alloc, current_schema_json, lowered);
    defer applied.deinit(alloc);
    const fingerprint = try ddlAppliedFingerprintAlloc(alloc, applied);
    defer alloc.free(fingerprint);
    try expectAppCompatPlan(entry.applied_plan, fingerprint);
}

fn expectQuerySummary(summary: AppCompatPlanSummary, query: db_mod.types.RelationalRowsQueryRequest) !void {
    try expectOptionalUsize(summary.predicates, query.predicates.len);
    try expectOptionalUsize(summary.json_path_eq, query.json_path_eq.len);
    try expectOptionalUsize(summary.select, query.select.len);
    try expectOptionalUsize(summary.order_by, query.order_by.len);
    try expectOptionalU32(summary.limit, query.limit);
    if (summary.row_claim_skip_locked) |expected| {
        try std.testing.expect(query.row_claim != null);
        try std.testing.expectEqual(expected, query.row_claim.?.skip_locked);
    }
}

fn expectQuerySourceSummary(summary: AppCompatPlanSummary, query: db_mod.types.RelationalRowsQueryRequest) !void {
    try expectOptionalUsize(summary.predicates, query.predicates.len);
    try expectOptionalUsize(summary.json_path_eq, query.json_path_eq.len);
}

fn expectDdlSummary(summary: AppCompatPlanSummary, lowered: LoweredDdlPlan) !void {
    const expected = summary.ddl_tag orelse return;
    switch (lowered) {
        .create_table => |plan| {
            try std.testing.expectEqual(AppCompatDdlTag.create_table, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.select, plan.columns.len);
            try expectOptionalUsize(summary.operations, plan.unique_constraints.len + plan.foreign_keys.len + plan.checks.len);
        },
        .create_index => |plan| {
            try std.testing.expectEqual(AppCompatDdlTag.create_index, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.select, plan.columns.len + plan.expressions.len);
            try expectOptionalUsize(summary.predicates, plan.where.len);
        },
        .alter_table => |plan| {
            try std.testing.expectEqual(AppCompatDdlTag.alter_table, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
            try expectOptionalUsize(summary.operations, plan.operations.len);
        },
        .create_update_policy => |plan| {
            try std.testing.expectEqual(AppCompatDdlTag.create_update_policy, expected);
            try expectOptionalTableName(summary.table_name, plan.table_name);
        },
    }
}

fn expectAppCompatCorpusEntry(
    alloc: std.mem.Allocator,
    base_schema_json: []const u8,
    schema: runtime_schema.TableSchema,
    entry: AppCompatCorpusEntry,
    unique_resolver: relational_rows.UniqueSelectorResolver,
    row_claim: db_mod.types.RowClaimRequest,
) !void {
    switch (entry.family) {
        .ddl => {
            var lowered = try lowerDdlPlanAlloc(alloc, entry.sql);
            defer lowered.deinit(alloc);
            try expectDdlSummary(entry.summary, lowered);
            const fingerprint = try ddlFingerprintAlloc(alloc, lowered);
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
            try expectAppliedDdlCorpusPlan(alloc, base_schema_json, entry, lowered);
        },
        .query => {
            var lowered = try lowerQueryPlanAlloc(alloc, entry.sql, schema, entry.params);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectOptionalUsize(entry.summary.ctes, lowered.plan.ctes.len);
            try expectQuerySummary(entry.summary, lowered.plan.query);
            const fingerprint = try queryFingerprintAlloc(alloc, "query", lowered.table_name, lowered.plan.query, lowered.plan.ctes.len);
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .aggregate => {
            var lowered = try lowerAggregateAlloc(alloc, entry.sql, schema, entry.params);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectQuerySourceSummary(entry.summary, lowered.aggregate.source);
            try expectOptionalUsize(entry.summary.group_by, lowered.aggregate.group_by.len);
            try expectOptionalUsize(entry.summary.aggregations, lowered.aggregate.aggregations.len);
            try expectOptionalUsize(entry.summary.having, lowered.aggregate.having_predicates.len);
            try expectOptionalUsize(entry.summary.order_by, lowered.aggregate.order_by.len);
            try expectOptionalU32(entry.summary.limit, lowered.aggregate.limit);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "aggregate:table={s}:source_pred={d}:source_json_eq={d}:group={d}:aggs={d}:agg_expr={d}:filter_expr={d}:having={d}:order={d}:limit={d}",
                .{
                    lowered.table_name,
                    lowered.aggregate.source.predicates.len,
                    lowered.aggregate.source.json_path_eq.len,
                    lowered.aggregate.group_by.len,
                    lowered.aggregate.aggregations.len,
                    aggregateInputExpressionCount(lowered.aggregate.aggregations),
                    aggregateFilterExpressionCount(lowered.aggregate.aggregations),
                    lowered.aggregate.having_predicates.len,
                    lowered.aggregate.order_by.len,
                    appCompatLimitValue(lowered.aggregate.limit),
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .join => {
            var lowered = try lowerJoinAlloc(alloc, entry.sql, schema, entry.params);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.left_table_name);
            try expectOptionalUsize(entry.summary.predicates, lowered.join.left.predicates.len + lowered.join.right.predicates.len);
            try expectOptionalUsize(entry.summary.join_on, lowered.join.on.len);
            try expectOptionalUsize(entry.summary.join_select, lowered.join.select.len);
            try expectOptionalUsize(entry.summary.order_by, lowered.join.order_by.len);
            try expectOptionalU32(entry.summary.limit, lowered.join.limit);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "join:left={s}:right={s}:left_pred={d}:right_pred={d}:on={d}:select={d}:order={d}:limit={d}",
                .{
                    lowered.left_table_name,
                    lowered.right_table_name,
                    lowered.join.left.predicates.len,
                    lowered.join.right.predicates.len,
                    lowered.join.on.len,
                    lowered.join.select.len,
                    lowered.join.order_by.len,
                    appCompatLimitValue(lowered.join.limit),
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .lateral => {
            var lowered = try lowerLateralPlanAlloc(alloc, entry.sql, schema, entry.params);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.left_table_name);
            try expectOptionalUsize(entry.summary.predicates, lowered.plan.lateral.left.predicates.len + lowered.plan.lateral.right.predicates.len);
            try expectOptionalUsize(entry.summary.lateral_correlations, lowered.plan.lateral.correlations.len);
            try expectOptionalUsize(entry.summary.join_select, lowered.plan.lateral.select.len);
            try expectOptionalUsize(entry.summary.order_by, lowered.plan.lateral.order_by.len);
            try expectOptionalU32(entry.summary.limit, lowered.plan.lateral.limit);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "lateral:left={s}:right={s}:left_pred={d}:right_pred={d}:corr={d}:select={d}:order={d}:limit={d}",
                .{
                    lowered.left_table_name,
                    lowered.right_table_name,
                    lowered.plan.lateral.left.predicates.len,
                    lowered.plan.lateral.right.predicates.len,
                    lowered.plan.lateral.correlations.len,
                    lowered.plan.lateral.select.len,
                    lowered.plan.lateral.order_by.len,
                    appCompatLimitValue(lowered.plan.lateral.limit),
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .window => {
            var lowered = try lowerWindowPlanAlloc(alloc, entry.sql, schema, entry.params);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectOptionalUsize(entry.summary.ctes, lowered.plan.ctes.len);
            try expectQuerySourceSummary(entry.summary, lowered.plan.window.source);
            try expectOptionalUsize(entry.summary.windows, lowered.plan.window.windows.len);
            try expectOptionalUsize(entry.summary.select, lowered.plan.window.select.len);
            try expectOptionalUsize(entry.summary.order_by, lowered.plan.window.order_by.len);
            try expectOptionalU32(entry.summary.limit, lowered.plan.window.limit);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "window:table={s}:ctes={d}:source_pred={d}:windows={d}:select={d}:order={d}:limit={d}",
                .{
                    lowered.table_name,
                    lowered.plan.ctes.len,
                    lowered.plan.window.source.predicates.len,
                    lowered.plan.window.windows.len,
                    lowered.plan.window.select.len,
                    lowered.plan.window.order_by.len,
                    appCompatLimitValue(lowered.plan.window.limit),
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .insert => {
            var lowered = try lowerInsertWithResolverAlloc(alloc, entry.sql, schema, entry.params, unique_resolver);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectOptionalUsize(entry.summary.returning, lowered.batch.returning_rows.len);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "insert:table={s}:writes={d}:transforms={d}:deletes={d}:returning_rows={d}:returning_expr={d}",
                .{
                    lowered.table_name,
                    lowered.batch.writes.len,
                    lowered.batch.transforms.len,
                    lowered.batch.deletes.len,
                    lowered.batch.returning_rows.len,
                    lowered.returning_expression_count,
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .update => {
            var lowered = try lowerUpdateAlloc(alloc, entry.sql, schema, entry.params, unique_resolver);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectOptionalUsize(entry.summary.operations, if (lowered.batch.transforms.len == 0) 0 else lowered.batch.transforms[0].operations.len);
            try expectOptionalUsize(entry.summary.returning, lowered.batch.returning_rows.len);
            const operations = if (lowered.batch.transforms.len == 0) 0 else lowered.batch.transforms[0].operations.len;
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "update:table={s}:transforms={d}:ops={d}:returning_rows={d}:returning_expr={d}",
                .{ lowered.table_name, lowered.batch.transforms.len, operations, lowered.batch.returning_rows.len, lowered.returning_expression_count },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .delete => {
            var lowered = try lowerDeleteAlloc(alloc, entry.sql, schema, entry.params, unique_resolver);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectOptionalUsize(entry.summary.returning, lowered.batch.returning_rows.len);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "delete:table={s}:deletes={d}:returning_rows={d}:returning_expr={d}",
                .{ lowered.table_name, lowered.batch.deletes.len, lowered.batch.returning_rows.len, lowered.returning_expression_count },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .update_source => {
            var lowered = try lowerUpdateMutationSourceAlloc(alloc, entry.sql, schema, entry.params, row_claim);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectQuerySummary(entry.summary, lowered.mutation.req.source);
            try expectOptionalUsize(entry.summary.operations, lowered.mutation.req.operations.len);
            try expectOptionalUsize(entry.summary.returning, lowered.mutation.req.returning.len);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "update_source:table={s}:source_pred={d}:source_order={d}:source_limit={d}:claim={s}:ops={d}:returning={d}:returning_expr={d}",
                .{
                    lowered.table_name,
                    lowered.mutation.req.source.predicates.len,
                    lowered.mutation.req.source.order_by.len,
                    appCompatLimitValue(lowered.mutation.req.source.limit),
                    if (lowered.mutation.req.source.row_claim) |claim_value| if (claim_value.skip_locked) "skip_locked" else "locked" else "none",
                    lowered.mutation.req.operations.len,
                    lowered.mutation.req.returning.len,
                    lowered.mutation.req.returning_expressions.len,
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .delete_source => {
            var lowered = try lowerDeleteMutationSourceAlloc(alloc, entry.sql, schema, entry.params, row_claim);
            defer lowered.deinit(alloc);
            try expectOptionalTableName(entry.summary.table_name, lowered.table_name);
            try expectQuerySummary(entry.summary, lowered.mutation.req.source);
            try expectOptionalUsize(entry.summary.returning, lowered.mutation.req.returning.len);
            const fingerprint = try std.fmt.allocPrint(
                alloc,
                "delete_source:table={s}:source_pred={d}:source_order={d}:source_limit={d}:claim={s}:returning={d}:returning_expr={d}",
                .{
                    lowered.table_name,
                    lowered.mutation.req.source.predicates.len,
                    lowered.mutation.req.source.order_by.len,
                    appCompatLimitValue(lowered.mutation.req.source.limit),
                    if (lowered.mutation.req.source.row_claim) |claim_value| if (claim_value.skip_locked) "skip_locked" else "locked" else "none",
                    lowered.mutation.req.returning.len,
                    lowered.mutation.req.returning_expressions.len,
                },
            );
            defer alloc.free(fingerprint);
            try expectAppCompatPlan(entry.plan, fingerprint);
        },
        .unsupported => {
            try std.testing.expectError(error.UnsupportedSqlShape, lowerQueryPlanAlloc(alloc, entry.sql, schema, entry.params));
            try expectAppCompatPlan(entry.plan, "unsupported:query");
        },
        .unsupported_ddl => {
            try std.testing.expectError(error.UnsupportedSqlShape, lowerDdlPlanAlloc(alloc, entry.sql));
            try expectAppCompatPlan(entry.plan, "unsupported:ddl");
        },
        .unsupported_update => {
            try std.testing.expectError(error.UnsupportedSqlShape, lowerUpdateAlloc(alloc, entry.sql, schema, entry.params, unique_resolver));
            try expectAppCompatPlan(entry.plan, "unsupported:update");
        },
    }
}

test "postgres sql adapter classifies application compatibility corpus" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"organization_id":{"type":"keyword"},"cloud_instance_id":{"type":"keyword"},"user_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"kind":{"type":"keyword"},"status":{"type":"keyword","default":"active"},"metric_type":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"},"rating_status":{"type":"keyword"},"product_family":{"type":"keyword"},"amount":{"type":"numeric"},"quantity":{"type":"numeric"},"rated_quantity":{"type":"numeric"},"priority":{"type":"numeric"},"created_at":{"type":"numeric"},"updated_at_ns":{"type":"numeric"},"recorded_at":{"type":"numeric"},"expires_at":{"type":"numeric"},"billing_cycle_start":{"type":"numeric"},"bucket_start":{"type":"numeric"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var resolver_ctx = TestPrimaryResolver{
        .row_json = "{\"id\":\"u1\",\"tenant_id\":\"t1\",\"status\":\"queued\",\"quantity\":1,\"priority\":1,\"metadata\":{}}",
        .version = 7,
    };
    const txn_id = [_]u8{ 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f };
    const row_claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "compat-worker",
        .txn_id = txn_id,
    };

    const corpus = [_]AppCompatCorpusEntry{
        .{
            .name = "schema create table",
            .family = .ddl,
            .summary = .{ .ddl_tag = .create_table, .table_name = "usage_records", .select = 7, .operations = 3 },
            .plan = "ddl:create_table:table=usage_records:columns=7:unique=1:fk=1:checks=1",
            .sql =
            \\CREATE TABLE IF NOT EXISTS usage_records (
            \\  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
            \\  tenant_id text NOT NULL,
            \\  quantity numeric(18, 2) DEFAULT 0 CHECK (quantity >= 0),
            \\  metadata jsonb,
            \\  tags text[],
            \\  created_at timestamptz DEFAULT now(),
            \\  tenant_key text GENERATED ALWAYS AS (lower(tenant_id)) STORED,
            \\  CONSTRAINT usage_records_tenant_key UNIQUE (tenant_id),
            \\  CONSTRAINT usage_records_tenant_fkey FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
            \\);
            ,
        },
        .{
            .name = "schema partial index",
            .family = .ddl,
            .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records", .select = 1, .predicates = 1 },
            .plan = "ddl:create_index:table=usage_records:columns=1:expr=0:where=1:unique=false",
            .applied_plan = "applied:rebuild=true:validation=false:building_indexes=1:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0",
            .sql = "CREATE INDEX usage_records_status_idx ON usage_records (status) WHERE status = 'pending';",
        },
        .{
            .name = "schema unique expression index",
            .family = .ddl,
            .summary = .{ .ddl_tag = .create_index, .table_name = "usage_records", .select = 2, .predicates = 1 },
            .plan = "ddl:create_index:table=usage_records:columns=1:expr=1:where=1:unique=true",
            .applied_plan = "applied:rebuild=true:validation=true:building_indexes=0:unvalidated_unique=1:unvalidated_fk=0:unvalidated_check=0:update_policy=0",
            .sql = "CREATE UNIQUE INDEX usage_records_tenant_email_key ON usage_records (tenant_id, lower(email)) WHERE status = 'active';",
        },
        .{
            .name = "schema additive foreign key",
            .family = .ddl,
            .summary = .{ .ddl_tag = .alter_table, .table_name = "usage_records", .operations = 2 },
            .plan = "ddl:alter_table:table=usage_records:ops=2",
            .sql = "ALTER TABLE usage_records ADD COLUMN user_id text REFERENCES users(id);",
        },
        .{
            .name = "schema additive check validation work",
            .family = .ddl,
            .summary = .{ .ddl_tag = .alter_table, .table_name = "usage_records", .operations = 1 },
            .plan = "ddl:alter_table:table=usage_records:ops=1",
            .applied_plan = "applied:rebuild=true:validation=true:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=1:update_policy=0",
            .sql = "ALTER TABLE usage_records ADD CONSTRAINT usage_records_status_known_check CHECK (status != 'unknown') NOT VALID;",
        },
        .{
            .name = "schema validate check constraint",
            .family = .ddl,
            .summary = .{ .ddl_tag = .alter_table, .table_name = "usage_records", .operations = 1 },
            .plan = "ddl:alter_table:table=usage_records:ops=1",
            .apply_setup_sql = &.{
                "CREATE TABLE usage_records (id uuid PRIMARY KEY, status text);",
                "ALTER TABLE usage_records ADD CONSTRAINT usage_records_status_known_check CHECK (status != 'unknown') NOT VALID;",
            },
            .applied_plan = "applied:rebuild=true:validation=true:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=0",
            .sql = "ALTER TABLE ONLY usage_records VALIDATE CONSTRAINT usage_records_status_known_check;",
        },
        .{
            .name = "schema updated-at policy",
            .family = .ddl,
            .summary = .{ .ddl_tag = .create_update_policy, .table_name = "usage_records" },
            .plan = "ddl:create_update_policy:table=usage_records:column=updated_at_ns",
            .applied_plan = "applied:rebuild=false:validation=false:building_indexes=0:unvalidated_unique=0:unvalidated_fk=0:unvalidated_check=0:update_policy=1",
            .sql = "CREATE TRIGGER update_timestamp BEFORE UPDATE ON usage_records EXECUTE FUNCTION touch_updated_at('updated_at_ns');",
        },
        .{
            .name = "adapter-only extension syntax",
            .family = .unsupported_ddl,
            .plan = "unsupported:ddl",
            .sql = "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
        },
        .{
            .name = "single table json query",
            .family = .query,
            .summary = .{ .table_name = "usage_records", .predicates = 1, .json_path_eq = 1, .select = 1, .order_by = 1, .limit = 10 },
            .plan = "query:table=usage_records:ctes=0:pred=1:json_eq=1:or=0:not=0:select=1:expr=1:alias=0:order=1:order_expr=0:limit=10:claim=none",
            .sql = "SELECT id, metadata->>'source' AS source FROM usage_records WHERE organization_id = $1 AND metadata->>'source' = $2 ORDER BY created_at DESC LIMIT 10",
            .params = &.{ .{ .string = "org_1" }, .{ .string = "meter" } },
        },
        .{
            .name = "single table expression query",
            .family = .query,
            .summary = .{ .table_name = "usage_records", .predicates = 1, .select = 0, .order_by = 2, .limit = 20 },
            .plan = "query:table=usage_records:ctes=0:pred=1:json_eq=0:or=0:not=0:select=0:expr=5:alias=0:order=2:order_expr=1:limit=20:claim=none",
            .sql = "SELECT id AS usage_id, CASE WHEN status = 'blocked' THEN 'needs_review' ELSE lower(status) END AS status_label, CAST(amount AS text) AS amount_text, amount + quantity AS total_amount, array_length(tags, 1) AS tag_count FROM usage_records WHERE status = $1 ORDER BY lower(status) ASC, created_at DESC LIMIT 20",
            .params = &.{.{ .string = "ready" }},
        },
        .{
            .name = "grouped aggregate",
            .family = .aggregate,
            .summary = .{ .table_name = "usage_records", .predicates = 1, .group_by = 1, .aggregations = 3, .having = 1, .order_by = 1, .limit = 5 },
            .plan = "aggregate:table=usage_records:source_pred=1:source_json_eq=0:group=1:aggs=3:agg_expr=1:filter_expr=1:having=1:order=1:limit=5",
            .sql = "SELECT organization_id, COUNT(*) AS record_count, SUM(quantity) AS quantity_sum, COUNT(DISTINCT lower(status)) FILTER (WHERE lower(status) = 'active') AS active_status_count FROM usage_records WHERE metric_type = $1 GROUP BY organization_id HAVING quantity_sum > 0 ORDER BY quantity_sum DESC LIMIT 5",
            .params = &.{.{ .string = "tokens" }},
        },
        .{
            .name = "equality join",
            .family = .join,
            .summary = .{ .table_name = "usage_records", .predicates = 2, .join_on = 2, .join_select = 3, .order_by = 1, .limit = 5 },
            .plan = "join:left=usage_records:right=usage_records:left_pred=1:right_pred=1:on=2:select=3:order=1:limit=5",
            .sql = "SELECT o.id AS order_id, c.name AS customer_name, o.amount AS amount FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant_id = c.tenant_id AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' ORDER BY amount DESC LIMIT 5",
        },
        .{
            .name = "bounded lateral",
            .family = .lateral,
            .summary = .{ .table_name = "usage_records", .predicates = 2, .lateral_correlations = 1, .join_select = 2, .order_by = 1, .limit = 10 },
            .plan = "lateral:left=usage_records:right=usage_records:left_pred=1:right_pred=1:corr=1:select=2:order=1:limit=10",
            .sql = "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY created_at DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC LIMIT 10",
        },
        .{
            .name = "row number window",
            .family = .window,
            .summary = .{ .table_name = "usage_records", .ctes = 0, .predicates = 1, .select = 2, .windows = 1, .order_by = 1, .limit = 5 },
            .plan = "window:table=usage_records:ctes=0:source_pred=1:windows=1:select=2:order=1:limit=5",
            .sql = "SELECT tenant_id, id, row_number() OVER (PARTITION BY tenant_id ORDER BY amount DESC, id ASC) AS row_num FROM usage_records WHERE status = 'open' ORDER BY row_num ASC LIMIT 5",
        },
        .{
            .name = "conflict upsert",
            .family = .insert,
            .summary = .{ .table_name = "usage_records", .returning = 1 },
            .plan = "insert:table=usage_records:writes=0:transforms=1:deletes=0:returning_rows=1:returning_expr=1",
            .sql = "INSERT INTO usage_records (id, status, quantity) VALUES ('u1', 'open', 2) ON CONFLICT (id) DO UPDATE SET quantity = excluded.quantity RETURNING id, quantity, CAST(quantity AS text) AS quantity_text",
        },
        .{
            .name = "conflict default update",
            .family = .insert,
            .summary = .{ .table_name = "usage_records", .returning = 1 },
            .plan = "insert:table=usage_records:writes=0:transforms=1:deletes=0:returning_rows=1:returning_expr=0",
            .sql = "INSERT INTO usage_records (id, status) VALUES ('u1', 'open') ON CONFLICT (id) DO UPDATE SET status = DEFAULT RETURNING id, status",
        },
        .{
            .name = "point update",
            .family = .update,
            .summary = .{ .table_name = "usage_records", .operations = 1, .returning = 1 },
            .plan = "update:table=usage_records:transforms=1:ops=1:returning_rows=1:returning_expr=1",
            .sql = "UPDATE usage_records SET status = $1 WHERE id = $2 RETURNING id, status, LOWER(status) AS status_key",
            .params = &.{ .{ .string = "processing" }, .{ .string = "u1" } },
        },
        .{
            .name = "point delete",
            .family = .delete,
            .summary = .{ .table_name = "usage_records", .returning = 1 },
            .plan = "delete:table=usage_records:deletes=1:returning_rows=1:returning_expr=1",
            .sql = "DELETE FROM usage_records WHERE id = $1 RETURNING id, LOWER(status) AS status_key",
            .params = &.{.{ .string = "u1" }},
        },
        .{
            .name = "claimed queue update",
            .family = .update_source,
            .summary = .{ .table_name = "usage_records", .predicates = 1, .order_by = 2, .limit = 10, .operations = 2, .returning = 2, .row_claim_skip_locked = true },
            .plan = "update_source:table=usage_records:source_pred=1:source_order=2:source_limit=10:claim=skip_locked:ops=2:returning=2:returning_expr=0",
            .sql = "UPDATE usage_records SET status = $1, priority = priority + 1 WHERE status = $2 ORDER BY priority DESC, id ASC LIMIT 10 FOR UPDATE SKIP LOCKED RETURNING id, status",
            .params = &.{ .{ .string = "processing" }, .{ .string = "queued" } },
        },
        .{
            .name = "claimed queue update returning expressions",
            .family = .update_source,
            .summary = .{ .table_name = "usage_records", .predicates = 1, .order_by = 2, .limit = 10, .operations = 2, .returning = 2, .row_claim_skip_locked = true },
            .plan = "update_source:table=usage_records:source_pred=1:source_order=2:source_limit=10:claim=skip_locked:ops=2:returning=2:returning_expr=2",
            .sql = "UPDATE usage_records SET status = $1, priority = priority + 1 WHERE status = $2 ORDER BY priority DESC, id ASC LIMIT 10 FOR UPDATE SKIP LOCKED RETURNING id, status, priority + 1 AS next_priority, CAST(priority AS text) AS priority_text",
            .params = &.{ .{ .string = "processing" }, .{ .string = "queued" } },
        },
        .{
            .name = "claimed cleanup delete",
            .family = .delete_source,
            .summary = .{ .table_name = "usage_records", .predicates = 2, .order_by = 1, .limit = 10, .returning = 1, .row_claim_skip_locked = false },
            .plan = "delete_source:table=usage_records:source_pred=2:source_order=1:source_limit=10:claim=locked:returning=1:returning_expr=0",
            .sql = "DELETE FROM usage_records WHERE status = 'expired' AND expires_at < $1 ORDER BY expires_at ASC LIMIT 10 RETURNING id",
            .params = &.{.{ .integer = 1000 }},
        },
        .{
            .name = "unsupported ilike expression",
            .family = .unsupported,
            .plan = "unsupported:query",
            .sql = "SELECT id FROM usage_records WHERE name ILIKE $1",
            .params = &.{.{ .string = "a%" }},
        },
        .{
            .name = "unsupported update from",
            .family = .unsupported_update,
            .plan = "unsupported:update",
            .sql = "UPDATE usage_records SET quantity = source.quantity FROM source_records AS source WHERE usage_records.id = source.id",
        },
    };

    for (corpus) |entry| {
        errdefer std.debug.print("application compatibility corpus entry failed: {s}\n", .{entry.name});
        try expectAppCompatCorpusEntry(alloc, schema_json, schema, entry, resolver_ctx.resolver(), row_claim);
    }
}

test "postgres sql adapter lowers update patch with explicit version predicate" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"pending\",\"metadata\":{\"billing\":{\"plan\":\"free\"}}}", .version = 42 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET status = $1 WHERE id = $2",
        schema,
        &.{ .{ .string = "active" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 42), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers arithmetic updates into typed increments" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"amount\":5}", .version = 21 };

    var plus = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET amount = amount + $1 WHERE id = $2 RETURNING amount, amount * 2 AS doubled_amount",
        schema,
        &.{ .{ .integer = 2 }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer plus.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), plus.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), plus.batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, plus.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", plus.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("2", plus.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 21), plus.batch.predicates[0].expected_version);
    var plus_returned = try std.json.parseFromSlice(std.json.Value, alloc, plus.batch.returning_rows[0], .{});
    defer plus_returned.deinit();
    switch (plus_returned.value.object.get("amount").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 7), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 7), value),
        else => return error.TestUnexpectedResult,
    }
    switch (plus_returned.value.object.get("doubled_amount").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 14), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 14), value),
        else => return error.TestUnexpectedResult,
    }

    var minus = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET amount = amount - 1 WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer minus.deinit(alloc);

    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, minus.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", minus.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("-1", minus.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 21), minus.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers array updates into typed transforms" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"tags\":[\"old\"],\"status\":\"active\"}", .version = 22 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET tags = array_append(tags, 'new'), tags = array_remove(tags, 'old') WHERE id = $1 RETURNING tags",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 2), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqual(db_mod.types.TransformOpType.push, lowered.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("tags", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"new\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(db_mod.types.TransformOpType.pull, lowered.batch.transforms[0].operations[1].op);
    try std.testing.expectEqualStrings("\"old\"", lowered.batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqual(@as(u64, 22), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"tags\":[\"new\"]}", lowered.batch.returning_rows[0]);

    try std.testing.expectError(error.InvalidSqlCatalog, lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET status = array_append(status, 'bad') WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    ));
}

test "postgres sql adapter lowers now update values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"updated_at_ns":{"type":"numeric"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"name\":\"old\",\"updated_at_ns\":1}", .version = 22 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE users SET updated_at_ns = NOW() WHERE id = $1 RETURNING updated_at_ns",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("updated_at_ns", lowered.batch.transforms[0].operations[0].path);
    const planned_now = try std.fmt.parseInt(u64, lowered.batch.transforms[0].operations[0].value_json.?, 10);
    try std.testing.expect(planned_now > 0);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("updated_at_ns").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, @intCast(planned_now)), value),
        else => return error.TestUnexpectedResult,
    }
    try std.testing.expectEqual(@as(u64, 22), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter applies server update policies" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"updated_at_ns":{"type":"numeric","x-antfly-on-update":{"op":"now_ns"}}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"old\",\"updated_at_ns\":1}", .version = 24 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE users SET status = 'active' WHERE id = $1 RETURNING status, updated_at_ns",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("updated_at_ns", lowered.batch.transforms[0].operations[1].path);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    try std.testing.expectEqualStrings("active", returned.value.object.get("status").?.string);
    switch (returned.value.object.get("updated_at_ns").?) {
        .integer => |value| try std.testing.expect(value > 1),
        .float => |value| try std.testing.expect(value > 1),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers explicit default update values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"disabled\"}", .version = 23 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE users SET status = DEFAULT WHERE id = $1 RETURNING status",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"status\":\"active\"}", lowered.batch.returning_rows[0]);
    try std.testing.expectEqual(@as(u64, 23), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers jsonb_build_object update value" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"source\":\"old\"}}", .version = 6 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = jsonb_build_object('source', $1, 'nested', $2::jsonb) WHERE id = $3 RETURNING metadata",
        schema,
        &.{ .{ .string = "builder" }, .{ .json = "{\"plan\":\"pro\"}" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("metadata", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"source\":\"builder\",\"nested\":{\"plan\":\"pro\"}}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"builder\",\"nested\":{\"plan\":\"pro\"}}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers convert_from jsonb update value" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"source\":\"old\"}}", .version = 6 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = convert_from($1, 'UTF-8')::jsonb WHERE id = $2 RETURNING metadata",
        schema,
        &.{ .{ .string = "{\"source\":\"converted\",\"active\":true}" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("metadata", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"source\":\"converted\",\"active\":true}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"converted\",\"active\":true}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers update jsonb_set returning through row batch" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"billing\":{\"plan\":\"free\"}}}", .version = 9 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = jsonb_set(metadata, '{billing,plan}', $1, true) WHERE id = $2 RETURNING metadata.billing.plan",
        schema,
        &.{ .{ .string = "pro" }, .{ .string = "u1" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("metadata.billing.plan", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pro\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 9), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"metadata.billing.plan\":\"pro\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers update jsonb concat into json set operations" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"metadata\":{\"billing\":{\"plan\":\"free\"},\"source\":\"old\"}}", .version = 10 };

    var lowered = try lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET metadata = metadata || '{\"billing\":{\"plan\":\"pro\"},\"flags\":[\"rated\"]}'::jsonb WHERE id = $1 RETURNING metadata.billing.plan, metadata.flags",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 2), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("metadata.billing", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"plan\":\"pro\"}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("metadata.flags", lowered.batch.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("[\"rated\"]", lowered.batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.returning_rows.len);
    try std.testing.expectEqualStrings("{\"metadata.billing.plan\":\"pro\",\"metadata.flags\":[\"rated\"]}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers delete with explicit version predicate" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"disabled\"}", .version = 7 };

    var lowered = try lowerDeleteAlloc(
        alloc,
        "DELETE FROM usage_records WHERE id = $1",
        schema,
        &.{.{ .string = "u1" }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.deleted);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.deletes.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 7), lowered.batch.predicates[0].expected_version);
}

test "postgres sql adapter lowers claimed update mutation source" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"priority":{"type":"numeric"},"metadata":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    const txn_id = [_]u8{ 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f };
    const claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "worker-a",
        .txn_id = txn_id,
    };

    var lowered = try lowerUpdateMutationSourceAlloc(
        alloc,
        "UPDATE usage_records SET status = $1, priority = priority + 1, metadata = jsonb_set(metadata, '{billing,plan}', $2, true) WHERE status = $3 ORDER BY priority DESC, id ASC LIMIT $4 FOR UPDATE SKIP LOCKED RETURNING id, status, priority, priority + 1 AS next_priority, CASE WHEN priority > 5 THEN 'high' ELSE status END AS priority_label, CAST(priority AS text) AS priority_text",
        schema,
        &.{ .{ .string = "processing" }, .{ .string = "pro" }, .{ .string = "queued" }, .{ .integer = 5 } },
        claim,
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, lowered.mutation.req.kind);
    try std.testing.expectEqual(@as(usize, 1), lowered.mutation.req.source.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.mutation.req.source.predicates[0].field);
    try std.testing.expectEqualStrings("\"queued\"", lowered.mutation.req.source.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.mutation.req.source.order_by.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.mutation.req.source.order_by[0].direction);
    try std.testing.expectEqual(@as(u32, 5), lowered.mutation.req.source.limit.?);
    try std.testing.expect(lowered.mutation.req.source.row_claim != null);
    try std.testing.expect(lowered.mutation.req.source.row_claim.?.skip_locked);
    try std.testing.expectEqualStrings("worker-a", lowered.mutation.req.source.row_claim.?.owner_id);
    try std.testing.expectEqualSlices(u8, &txn_id, &lowered.mutation.req.source.row_claim.?.txn_id.?);
    try std.testing.expectEqual(@as(usize, 3), lowered.mutation.req.operations.len);
    try std.testing.expectEqualStrings("status", lowered.mutation.req.operations[0].path);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, lowered.mutation.req.operations[1].op);
    try std.testing.expectEqualStrings("metadata.billing.plan", lowered.mutation.req.operations[2].path);
    try std.testing.expectEqual(@as(usize, 3), lowered.mutation.req.returning.len);
    try std.testing.expectEqual(@as(usize, 3), lowered.mutation.req.returning_expressions.len);
    try std.testing.expectEqualStrings("next_priority", lowered.mutation.req.returning_expressions[0].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.add, lowered.mutation.req.returning_expressions[0].expression.kind);
    try std.testing.expectEqualStrings("priority", lowered.mutation.req.returning_expressions[0].expression.operands[0].field);
    try std.testing.expectEqualStrings("priority_label", lowered.mutation.req.returning_expressions[1].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.case, lowered.mutation.req.returning_expressions[1].expression.kind);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.mutation.req.returning_expressions[1].expression.case_branches[0].when.op);
    try std.testing.expectEqualStrings("priority_text", lowered.mutation.req.returning_expressions[2].output);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionKind.cast, lowered.mutation.req.returning_expressions[2].expression.kind);
    try std.testing.expectEqual(db_mod.types.RelationalRowsExpressionCastType.text, lowered.mutation.req.returning_expressions[2].expression.cast_type.?);

    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"queued\",\"priority\":1,\"metadata\":{}}", .version = 1 };
    try std.testing.expectError(error.UnsupportedSqlShape, lowerUpdateAlloc(
        alloc,
        "UPDATE usage_records SET status = $1 WHERE status = $2",
        schema,
        &.{ .{ .string = "processing" }, .{ .string = "queued" } },
        resolver_ctx.resolver(),
    ));
}

test "postgres sql adapter lowers claimed delete mutation source" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"expires_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    const txn_id = [_]u8{ 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f };
    const claim: db_mod.types.RowClaimRequest = .{
        .mode = .for_update,
        .owner_id = "cleanup",
        .txn_id = txn_id,
    };

    var lowered = try lowerDeleteMutationSourceAlloc(
        alloc,
        "DELETE FROM usage_records WHERE status = 'expired' AND expires_at < $1 ORDER BY expires_at ASC LIMIT 10 RETURNING id",
        schema,
        &.{.{ .integer = 1000 }},
        claim,
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.delete, lowered.mutation.req.kind);
    try std.testing.expectEqual(@as(usize, 2), lowered.mutation.req.source.predicates.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.mutation.req.source.order_by.len);
    try std.testing.expectEqual(@as(u32, 10), lowered.mutation.req.source.limit.?);
    try std.testing.expect(lowered.mutation.req.source.row_claim != null);
    try std.testing.expect(!lowered.mutation.req.source.row_claim.?.skip_locked);
    try std.testing.expectEqual(@as(usize, 0), lowered.mutation.req.operations.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.mutation.req.returning.len);
    try std.testing.expectEqualStrings("id", lowered.mutation.req.returning[0]);
}

test "postgres sql adapter lowers on conflict primary do nothing" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 12 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING RETURNING *",
        schema,
        &.{ .{ .string = "u1" }, .{ .string = "pending" } },
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(usize, 0), lowered.batch.writes.len);
    try std.testing.expectEqual(@as(usize, 0), lowered.batch.returning_rows.len);
}

test "postgres sql adapter lowers on conflict primary do update with excluded values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 12 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', 'pending') ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING status, CASE WHEN status = 'pending' THEN 'updated' ELSE status END AS status_label",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.transforms.len);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pending\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lowered.batch.predicates.len);
    try std.testing.expectEqual(@as(u64, 12), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), lowered.returning_expression_count);
    try std.testing.expectEqualStrings("{\"status\":\"pending\",\"status_label\":\"updated\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers excluded explicit default values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 14 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', DEFAULT) ON CONFLICT (id) DO UPDATE SET status = excluded.status RETURNING status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 14), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"status\":\"active\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers conflict update explicit default values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword","default":"active"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"status\":\"existing\"}", .version = 15 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', 'pending') ON CONFLICT (id) DO UPDATE SET status = DEFAULT RETURNING status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 15), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"status\":\"active\"}", lowered.batch.returning_rows[0]);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, status) VALUES ('u1', 'pending') ON CONFLICT (id) DO UPDATE SET id = DEFAULT RETURNING id",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
}

test "postgres sql adapter lowers cross-column excluded conflict values" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"next_status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"old\"}", .version = 14 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, next_status) VALUES ('u2', 'a@example.test', 'active') ON CONFLICT (email) DO UPDATE SET status = excluded.next_status RETURNING id, status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 0), lowered.batch.inserted);
    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"active\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 14), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"active\"}", lowered.batch.returning_rows[0]);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, amount) VALUES ('u2', 'a@example.test', 3) ON CONFLICT (email) DO UPDATE SET status = excluded.amount RETURNING status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
}

test "postgres sql adapter lowers on conflict unique do update" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"existing\"}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, status) VALUES ('u2', 'a@example.test', 'pending') ON CONFLICT (email) DO UPDATE SET status = excluded.status RETURNING id, status",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("status", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"pending\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 8), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"pending\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers on conflict arithmetic update" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"amount\":5}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, amount) VALUES ('u2', 'a@example.test', 1) ON CONFLICT (email) DO UPDATE SET amount = amount + 3 RETURNING amount",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(db_mod.types.TransformOpType.inc, lowered.batch.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("amount", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("3", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 8), lowered.batch.predicates[0].expected_version);
    var returned = try std.json.parseFromSlice(std.json.Value, alloc, lowered.batch.returning_rows[0], .{});
    defer returned.deinit();
    switch (returned.value.object.get("amount").?) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 8), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 8), value),
        else => return error.TestUnexpectedResult,
    }
}

test "postgres sql adapter lowers on conflict jsonb concat update" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"metadata\":{\"source\":\"old\"}}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, metadata) VALUES ('u2', 'a@example.test', '{\"source\":\"insert\"}'::jsonb) ON CONFLICT (email) DO UPDATE SET metadata = metadata || '{\"source\":\"conflict\",\"flags\":[\"seen\"]}'::jsonb RETURNING metadata.source, metadata.flags",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqual(@as(usize, 2), lowered.batch.transforms[0].operations.len);
    try std.testing.expectEqualStrings("metadata.source", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"conflict\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("metadata.flags", lowered.batch.transforms[0].operations[1].path);
    try std.testing.expectEqualStrings("[\"seen\"]", lowered.batch.transforms[0].operations[1].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata.source\":\"conflict\",\"metadata.flags\":[\"seen\"]}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers on conflict jsonb_build_object update" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_email_key","columns":["email"]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"metadata\":{\"source\":\"old\"}}", .version = 8 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, metadata) VALUES ('u2', 'a@example.test', '{\"source\":\"insert\"}'::jsonb) ON CONFLICT (email) DO UPDATE SET metadata = jsonb_build_object('source', 'conflict', 'count', $1) RETURNING metadata",
        schema,
        &.{.{ .integer = 2 }},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("metadata", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("{\"source\":\"conflict\",\"count\":2}", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqualStrings("{\"metadata\":{\"source\":\"conflict\",\"count\":2}}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers partial unique conflict target predicates" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"usage_records_active_email_key","columns":["email"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"status\":\"active\",\"name\":\"old\"}", .version = 11 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO usage_records (id, email, status, name) VALUES ('u2', 'a@example.test', 'active', 'new') ON CONFLICT (email) WHERE status = 'active' DO UPDATE SET name = excluded.name RETURNING id, name",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("name", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"new\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 11), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"name\":\"new\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers lower expression unique conflict target" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_lower_email_key","expressions":[{"op":"lower","field":"email"}]}]}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"email\":\"a@example.test\",\"name\":\"old\"}", .version = 13 };

    var lowered = try lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO users (id, email, name) VALUES ('u2', 'A@EXAMPLE.TEST', 'new') ON CONFLICT (lower(email)) DO UPDATE SET name = excluded.name RETURNING id, name",
        schema,
        &.{},
        resolver_ctx.resolver(),
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 1), lowered.batch.transformed);
    try std.testing.expectEqualStrings("name", lowered.batch.transforms[0].operations[0].path);
    try std.testing.expectEqualStrings("\"new\"", lowered.batch.transforms[0].operations[0].value_json.?);
    try std.testing.expectEqual(@as(u64, 13), lowered.batch.predicates[0].expected_version);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"name\":\"new\"}", lowered.batch.returning_rows[0]);
}

test "postgres sql adapter lowers non recursive cte query plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerQueryPlanAlloc(
        alloc,
        "WITH open_orders AS (SELECT id, status, amount, created_at FROM orders WHERE status = 'open'), expensive_open_orders AS (SELECT id, amount, created_at FROM open_orders WHERE amount > 10) SELECT id FROM expensive_open_orders ORDER BY created_at DESC LIMIT 2",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("orders", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.plan.ctes.len);
    try std.testing.expectEqualStrings("open_orders", lowered.plan.ctes[0].name);
    try std.testing.expectEqualStrings("", lowered.plan.ctes[0].query.source_cte);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes[0].query.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.plan.ctes[0].query.predicates[0].field);
    try std.testing.expectEqualStrings("\"open\"", lowered.plan.ctes[0].query.predicates[0].value_json.?);
    try std.testing.expectEqualStrings("expensive_open_orders", lowered.plan.ctes[1].name);
    try std.testing.expectEqualStrings("open_orders", lowered.plan.ctes[1].query.source_cte);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes[1].query.predicates.len);
    try std.testing.expectEqualStrings("amount", lowered.plan.ctes[1].query.predicates[0].field);
    try std.testing.expectEqual(runtime_schema.RelationalCheckOp.gt, lowered.plan.ctes[1].query.predicates[0].op);
    try std.testing.expectEqualStrings("10", lowered.plan.ctes[1].query.predicates[0].value_json.?);
    try std.testing.expectEqualStrings("expensive_open_orders", lowered.plan.query.source_cte);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.select.len);
    try std.testing.expectEqualStrings("id", lowered.plan.query.select[0]);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.order_by.len);
    try std.testing.expectEqualStrings("created_at", lowered.plan.query.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 2), lowered.plan.query.limit.?);

    var plain = try lowerQueryPlanAlloc(
        alloc,
        "SELECT id FROM orders WHERE status = 'open'",
        schema,
        &.{},
    );
    defer plain.deinit(alloc);
    try std.testing.expectEqualStrings("orders", plain.table_name);
    try std.testing.expectEqual(@as(usize, 0), plain.plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), plain.plan.query.predicates.len);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerQueryPlanAlloc(
        alloc,
        "WITH early AS (SELECT id FROM later), later AS (SELECT id FROM orders) SELECT id FROM early",
        schema,
        &.{},
    ));
}

test "postgres sql adapter lowers row_number window query plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant":{"type":"keyword"},"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant","id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);

    var lowered = try lowerWindowPlanAlloc(
        alloc,
        "SELECT tenant, id, row_number() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS row_num FROM usage_records WHERE status = 'open' ORDER BY row_num ASC LIMIT 5",
        schema,
        &.{},
    );
    defer lowered.deinit(alloc);

    try std.testing.expectEqualStrings("usage_records", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 0), lowered.plan.ctes.len);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.source.predicates.len);
    try std.testing.expectEqualStrings("status", lowered.plan.window.source.predicates[0].field);
    try std.testing.expectEqualStrings("\"open\"", lowered.plan.window.source.predicates[0].value_json.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.plan.window.select.len);
    try std.testing.expectEqualStrings("tenant", lowered.plan.window.select[0]);
    try std.testing.expectEqualStrings("id", lowered.plan.window.select[1]);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows.len);
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.row_number, lowered.plan.window.windows[0].function);
    try std.testing.expectEqualStrings("row_num", lowered.plan.window.windows[0].output);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows[0].partition_by.len);
    try std.testing.expectEqualStrings("tenant", lowered.plan.window.windows[0].partition_by[0]);
    try std.testing.expectEqual(@as(usize, 2), lowered.plan.window.windows[0].order_by.len);
    try std.testing.expectEqualStrings("amount", lowered.plan.window.windows[0].order_by[0].field);
    try std.testing.expectEqual(db_mod.types.RelationalRowsQueryOrderDirection.desc, lowered.plan.window.windows[0].order_by[0].direction);
    try std.testing.expectEqualStrings("id", lowered.plan.window.windows[0].order_by[1].field);
    try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.order_by.len);
    try std.testing.expectEqualStrings("row_num", lowered.plan.window.order_by[0].field);
    try std.testing.expectEqual(@as(u32, 5), lowered.plan.window.limit.?);

    var cte = try lowerWindowPlanAlloc(
        alloc,
        "WITH open_usage AS (SELECT tenant, id, status, amount, created_at FROM usage_records WHERE status = 'open') SELECT tenant, id, row_number() OVER (PARTITION BY tenant ORDER BY created_at ASC) AS rn FROM open_usage LIMIT 2",
        schema,
        &.{},
    );
    defer cte.deinit(alloc);
    try std.testing.expectEqualStrings("usage_records", cte.table_name);
    try std.testing.expectEqual(@as(usize, 1), cte.plan.ctes.len);
    try std.testing.expectEqualStrings("open_usage", cte.plan.window.source.source_cte);
    try std.testing.expectEqualStrings("rn", cte.plan.window.windows[0].output);

    try std.testing.expectError(error.UnsupportedSqlShape, lowerWindowPlanAlloc(
        alloc,
        "SELECT id, row_number() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rn FROM usage_records",
        schema,
        &.{},
    ));
}

test "postgres sql adapter rejects unsupported application shapes explicitly" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer runtime_schema.freeSchema(alloc, schema);
    var resolver_ctx = TestPrimaryResolver{ .row_json = "{\"id\":\"u1\",\"organization_id\":\"o1\"}", .version = 3 };

    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "WITH membership AS (SELECT id FROM users) SELECT id FROM membership",
        schema,
        &.{},
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT row_number() OVER (ORDER BY id) FROM users",
        schema,
        &.{},
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerSelectAlloc(
        alloc,
        "SELECT id FROM users LEFT JOIN organizations ON users.organization_id = organizations.id",
        schema,
        &.{},
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerUpdateAlloc(
        alloc,
        "UPDATE users SET id = 'u2' WHERE id = 'u1'",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerUpdateAlloc(
        alloc,
        "UPDATE users SET organization_id = 'o2' WHERE organization_id = 'o1'",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDeleteAlloc(
        alloc,
        "DELETE FROM users WHERE organization_id = 'o1'",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, lowerInsertWithResolverAlloc(
        alloc,
        "INSERT INTO users (id, organization_id) VALUES ('u1', 'o1') ON CONFLICT (upper(organization_id)) DO NOTHING",
        schema,
        &.{},
        resolver_ctx.resolver(),
    ));
}
