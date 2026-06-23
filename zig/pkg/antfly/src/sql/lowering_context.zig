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
const classifier = @import("classifier.zig");
const db_mod = @import("../storage/db/mod.zig");
const lower_expr = @import("lower_expr.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const parser_mod = @import("parser.zig");
const parser_context = @import("parser_context.zig");
const plan = @import("plan.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const relational_rows = @import("../api/relational_rows.zig");
const table_catalog = @import("../api/table_catalog.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const source_binding = @import("source_binding.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

pub const ReadPlanLoweringCallbacks = struct {
    lower_lateral_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredLateralPlan,
    lower_window: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredWindowPlan,
    lower_aggregate_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredAggregatePlan,
    lower_recursive_cte_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredRecursiveCtePlan,
    lower_join_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredJoin,
    lower_query_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredQueryPlan,
    lower_set_operation_optional_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        ?runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredSetOperationPlan,
};

pub const ReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: ReadPlanLoweringCallbacks,
    statement_kind: ?classifier.SqlReadStatementKind = null,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(self: *@This()) !plan.LoweredReadPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredReadPlan {
        const old_statement_kind = self.statement_kind;
        const old_parsed_sql = self.parsed_sql;
        self.statement_kind = parsed_sql.readStatementKind();
        self.parsed_sql = parsed_sql;
        defer self.statement_kind = old_statement_kind;
        defer self.parsed_sql = old_parsed_sql;
        return try plan.lowerReadPlanWithHooks(self.hooks());
    }

    fn hooks(self: *@This()) plan.ReadPlanLoweringHooks {
        return .{
            .ptr = self,
            .statement_kind = self.statement_kind,
            .lower_lateral = lowerLateralHook,
            .lower_window = lowerWindowHook,
            .lower_aggregate = lowerAggregateHook,
            .lower_recursive_cte = lowerRecursiveCteHook,
            .lower_join = lowerJoinHook,
            .lower_query = lowerQueryHook,
            .lower_set_operation = lowerSetOperationHook,
        };
    }

    fn joinedSourceSchema(self: *@This()) runtime_schema.TableSchema {
        return self.source_schema orelse self.schema;
    }

    fn lowerLateralHook(ptr: *anyopaque) anyerror!plan.LoweredLateralPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_lateral_with_schemas(self.alloc, self.parsed_sql.?, self.schema, self.joinedSourceSchema(), self.params);
    }

    fn lowerWindowHook(ptr: *anyopaque) anyerror!plan.LoweredWindowPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_window(self.alloc, self.parsed_sql.?, self.schema, self.params);
    }

    fn lowerAggregateHook(ptr: *anyopaque) anyerror!plan.LoweredAggregatePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_aggregate_plan(self.alloc, self.parsed_sql.?, self.schema, self.params);
    }

    fn lowerRecursiveCteHook(ptr: *anyopaque) anyerror!plan.LoweredRecursiveCtePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_recursive_cte_plan(self.alloc, self.parsed_sql.?, self.schema, self.params, self.function_bindings);
    }

    fn lowerJoinHook(ptr: *anyopaque) anyerror!plan.LoweredJoin {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_join_with_schemas(self.alloc, self.parsed_sql.?, self.schema, self.joinedSourceSchema(), self.params);
    }

    fn lowerQueryHook(ptr: *anyopaque) anyerror!plan.LoweredQueryPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_query_plan(self.alloc, self.parsed_sql.?, self.schema, self.params, self.function_bindings);
    }

    fn lowerSetOperationHook(ptr: *anyopaque) anyerror!plan.LoweredSetOperationPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_set_operation_optional_source_schema(self.alloc, self.parsed_sql.?, self.schema, self.source_schema, self.params, self.function_bindings);
    }
};

pub const CatalogReadPlanLoweringCallbacks = struct {
    lower_document_target: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        source_binding.DocumentBinding,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_with_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_without_source_schema: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const CatalogReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: CatalogReadPlanLoweringCallbacks,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(
        self: *@This(),
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredReadPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql, catalog);
    }

    pub fn lowerParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredReadPlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        var bound = try binder.bindReadPlanCatalogStatementAlloc(self.alloc, parsed_sql, catalog);
        defer bound.deinit(self.alloc);
        return try binder.lowerReadPlanWithBoundStatementAlloc(self.alloc, &bound, self.hooks());
    }

    fn hooks(self: *@This()) binder.ReadPlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_document_target = lowerDocumentTarget,
            .lower_with_source_schema = lowerWithSourceSchema,
            .lower_without_source_schema = lowerWithoutSourceSchema,
        };
    }

    fn lowerDocumentTarget(ptr: *anyopaque, document: source_binding.DocumentBinding) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_document_target(self.alloc, self.parsed_sql.?, document, self.params, self.function_bindings);
    }

    fn lowerWithSourceSchema(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_source_schema(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, self.function_bindings);
    }

    fn lowerWithoutSourceSchema(ptr: *anyopaque) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_without_source_schema(self.alloc, self.parsed_sql.?, self.schema, self.params, self.function_bindings);
    }
};

fn runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !runtime_schema.TableSchema {
    var parsed = try schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    return try schema_api.deriveRuntimeTableSchema(alloc, parsed);
}

fn lowerReadPlanWithCatalogForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    catalog: table_catalog.CatalogSource,
) !plan.LoweredReadPlan {
    var context = CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .sql = sql,
        .schema = schema,
        .params = params,
        .function_bindings = .{},
        .callbacks = .{
            .lower_document_target = lowerDocumentTargetParsedSqlForLoweringContextTestAlloc,
            .lower_with_source_schema = lowerReadPlanWithSourceSchemaParsedSqlForLoweringContextTestAlloc,
            .lower_without_source_schema = lowerReadPlanParsedSqlForLoweringContextTestAlloc,
        },
    };
    return try context.lower(catalog);
}

fn lowerDocumentTargetParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    document: source_binding.DocumentBinding,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    _ = params;
    _ = function_bindings;
    const document_plan = @import("document_plan.zig");
    return switch (parsed_sql.statement.readKind() orelse return error.UnsupportedSqlShape) {
        .aggregate => .{
            .document_aggregate = try document_plan.lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.indexes_json,
                document.capabilities.bounded_scan,
            ),
        },
        .query => .{
            .document_query = try document_plan.lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
                alloc,
                parsed_sql,
                document.schema,
                document.capabilities,
            ),
        },
        else => error.UnsupportedSqlShape,
    };
}

fn lowerReadPlanWithSourceSchemaParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, parsed_sql, schema, source_schema, params, function_bindings);
}

fn lowerReadPlanParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, parsed_sql, schema, null, params, function_bindings);
}

fn lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredReadPlan {
    var context = ReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .source_schema = source_schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_lateral_with_schemas = lowerLateralWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_window = lowerWindowParsedSqlForLoweringContextTestAlloc,
            .lower_aggregate_plan = lowerAggregateParsedSqlForLoweringContextTestAlloc,
            .lower_recursive_cte_plan = unsupportedRecursiveCteParsedSqlForLoweringContextTestAlloc,
            .lower_join_with_schemas = lowerJoinWithSchemasParsedSqlForLoweringContextTestAlloc,
            .lower_query_plan = lowerQueryParsedSqlForLoweringContextTestAlloc,
            .lower_set_operation_optional_source_schema = unsupportedSetOperationParsedSqlForLoweringContextTestAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

fn lowerReadPlanForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredReadPlan {
    var parsed_sql = try tokenized.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlForLoweringContextTestAlloc(alloc, &parsed_sql, schema, null, params, .{});
}

fn lowerQueryParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
) !plan.LoweredQueryPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
    };
    var lowered = lower_expr.parseQueryPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        params,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.queryPlanParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.simpleSelectSetTailHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsQueryPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerWindowParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredWindowPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
    };
    var lowered = plan.parseWindowPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.windowPlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsWindowPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerAggregateParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredAggregatePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
    };
    var lowered = plan.parseAggregatePlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.aggregatePlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsAggregatePlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerJoinWithSchemasParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredJoin {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
    };
    var lowered = plan.parseJoinPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.joinCteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.joinPlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsJoinPlanCteOutputAlloc(alloc, schema, lowered.asPlan()) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn lowerLateralWithSchemasParsedSqlForLoweringContextTestAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
) !plan.LoweredLateralPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = parser_mod.tokensStartWithKeywordTag(tokens, .with);

    var parser_state = parser_context.ParserState{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
    };
    var lowered = plan.parseLateralPlanAlloc(
        alloc,
        tokens,
        &parser_state.pos,
        parser_context.ParserState.ContextAccessors.cteSelectParserHooks(&parser_state),
        parser_context.ParserState.ContextAccessors.lateralPlanParserHooks(&parser_state),
    ) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    errdefer lowered.deinit(alloc);
    relational_rows.validateRowsLateralPlanCteOutputAlloc(alloc, schema, lowered.plan) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        error.InvalidSqlCatalog => if (cte_adapter_shape) return error.UnsupportedSqlShape else return err,
        else => return err,
    };
    return lowered;
}

fn unsupportedRecursiveCteParsedSqlForLoweringContextTestAlloc(
    _: std.mem.Allocator,
    _: *const tokenized.ParsedSql,
    _: runtime_schema.TableSchema,
    _: []const value_mod.SqlValue,
    _: lower_expr.SqlFunctionBindings,
) anyerror!plan.LoweredRecursiveCtePlan {
    return error.UnsupportedSqlShape;
}

fn unsupportedSetOperationParsedSqlForLoweringContextTestAlloc(
    _: std.mem.Allocator,
    _: *const tokenized.ParsedSql,
    _: runtime_schema.TableSchema,
    _: ?runtime_schema.TableSchema,
    _: []const value_mod.SqlValue,
    _: lower_expr.SqlFunctionBindings,
) anyerror!plan.LoweredSetOperationPlan {
    return error.UnsupportedSqlShape;
}

test "sql adapter lowering context classifies read sql into typed plan families" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"name":{"type":"keyword"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    var query = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT id, status FROM usage_records WHERE kind = 'order' ORDER BY created_at DESC LIMIT 5",
        schema,
        &.{},
    );
    defer query.deinit(alloc);
    switch (query) {
        .query => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.query.predicates.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var aggregate = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT status, SUM(amount) AS total FROM usage_records WHERE kind = 'order' GROUP BY status ORDER BY total DESC LIMIT 5",
        schema,
        &.{},
    );
    defer aggregate.deinit(alloc);
    switch (aggregate) {
        .aggregate => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 0), lowered.plan.ctes.len);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations.len);
            try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.sum, lowered.plan.aggregate.aggregations[0].op);
        },
        else => return error.TestUnexpectedResult,
    }

    var cte_aggregate = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "WITH open_usage AS (SELECT tenant, amount, status FROM usage_records WHERE status = 'open') SELECT tenant, SUM(amount) AS total FROM open_usage GROUP BY tenant ORDER BY total DESC LIMIT 5",
        schema,
        &.{},
    );
    defer cte_aggregate.deinit(alloc);
    switch (cte_aggregate) {
        .aggregate => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.ctes.len);
            try std.testing.expectEqualStrings("open_usage", lowered.plan.aggregate.source.source_cte);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.aggregate.aggregations.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var join = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM usage_records AS o LEFT JOIN usage_records AS c ON o.tenant = c.tenant AND o.customer_id = c.id WHERE o.kind = 'order' AND c.kind = 'customer' ORDER BY order_id ASC LIMIT 5",
        schema,
        &.{},
    );
    defer join.deinit(alloc);
    switch (join) {
        .join => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
            try std.testing.expectEqualStrings("usage_records", lowered.right_table_name);
            try std.testing.expectEqual(@as(usize, 2), lowered.join.on.len);
            try std.testing.expectEqual(@as(usize, 2), lowered.join.select.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var lateral = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM usage_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC LIMIT 10",
        schema,
        &.{},
    );
    defer lateral.deinit(alloc);
    switch (lateral) {
        .lateral => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.left_table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.lateral.correlations.len);
        },
        else => return error.TestUnexpectedResult,
    }

    var window = try lowerReadPlanForLoweringContextTestAlloc(
        alloc,
        "SELECT tenant, id, row_number() OVER (PARTITION BY tenant ORDER BY amount DESC, id ASC) AS row_num FROM usage_records WHERE status = 'open' ORDER BY row_num ASC LIMIT 5",
        schema,
        &.{},
    );
    defer window.deinit(alloc);
    switch (window) {
        .window => |lowered| {
            try std.testing.expectEqualStrings("usage_records", lowered.table_name);
            try std.testing.expectEqual(@as(usize, 1), lowered.plan.window.windows.len);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context lowers catalog-backed equality join read plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"name":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"enabled":{"type":"boolean"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    const customer_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"},"enabled":{"type":"boolean"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 11,
                        .name = "usage_records",
                        .placement_role = "data",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 12,
                        .name = "customers",
                        .placement_role = "data",
                        .schema_json = customer_schema_json,
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog_plan = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT o.id AS order_id, c.name AS customer_name FROM public.usage_records AS o LEFT JOIN public.customers AS c ON o.customer_id = c.id WHERE c.enabled IS TRUE ORDER BY 1 ASC LIMIT 2",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer catalog_plan.deinit(alloc);
    switch (catalog_plan) {
        .join => |catalog_join| {
            try std.testing.expectEqualStrings("usage_records", catalog_join.left_table_name);
            try std.testing.expectEqualStrings("customers", catalog_join.right_table_name);
            const catalog_join_plan = catalog_join.asPlan();
            try std.testing.expectEqualStrings("usage_records", catalog_join_plan.left_table);
            try std.testing.expectEqualStrings("customers", catalog_join_plan.right_table);
            try std.testing.expectEqual(@as(usize, 1), catalog_join.join.right.predicates.len);
            try std.testing.expectEqualStrings("enabled", catalog_join.join.right.predicates[0].field);
            try std.testing.expectEqualStrings("true", catalog_join.join.right.predicates[0].value_json.?);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "sql adapter lowering context lowers catalog-backed bounded left join lateral read plans" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"kind":{"type":"keyword"},"tenant":{"type":"keyword"},"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"scope":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"enabled":{"type":"boolean"}},"required":["kind","tenant","id"],"additionalProperties":false}}},"primary_key":{"columns":["kind","tenant","id"]}}
    ;
    const schema = try runtimeSchemaFromJsonForLoweringContextTestAlloc(alloc, schema_json);
    defer runtime_schema.freeSchema(alloc, schema);

    const balance_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"organization_id":{"type":"keyword"},"kind":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"}},"required":["id","organization_id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const Catalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 21,
                        .name = "usage_records",
                        .placement_role = "data",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 22,
                        .name = "balance_records",
                        .placement_role = "data",
                        .schema_json = balance_schema_json,
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog_plan = try lowerReadPlanWithCatalogForLoweringContextTestAlloc(
        alloc,
        "SELECT org.id AS organization_id, latest.amount AS latest_amount FROM public.usage_records AS org LEFT JOIN LATERAL (SELECT amount, created_at FROM public.balance_records AS bal WHERE bal.organization_id = org.id AND bal.kind = 'balance' ORDER BY 2 DESC LIMIT 1) AS latest ON true WHERE org.kind = 'organization' ORDER BY latest_amount DESC LIMIT 10",
        schema,
        &.{},
        Catalog.iface(),
    );
    defer catalog_plan.deinit(alloc);
    switch (catalog_plan) {
        .lateral => |catalog_lateral| {
            try std.testing.expectEqualStrings("usage_records", catalog_lateral.left_table_name);
            try std.testing.expectEqualStrings("balance_records", catalog_lateral.right_table_name);
            try std.testing.expectEqualStrings("usage_records", catalog_lateral.plan.left_table);
            try std.testing.expectEqualStrings("balance_records", catalog_lateral.plan.right_table);
            try std.testing.expectEqual(@as(usize, 1), catalog_lateral.plan.lateral.right.predicates.len);
            try std.testing.expectEqualStrings("kind", catalog_lateral.plan.lateral.right.predicates[0].field);
            try std.testing.expectEqualStrings("\"balance\"", catalog_lateral.plan.lateral.right.predicates[0].value_json.?);
            try std.testing.expectEqual(@as(usize, 1), catalog_lateral.plan.lateral.correlations.len);
            try std.testing.expectEqualStrings("organization_id", catalog_lateral.plan.lateral.correlations[0].right_field);
        },
        else => return error.TestUnexpectedResult,
    }
}

pub const CatalogWritePlanLoweringCallbacks = struct {
    lower_with_options: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
    ) anyerror!plan.LoweredWritePlan,
};

pub const CatalogWritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    callbacks: CatalogWritePlanLoweringCallbacks,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(
        self: *@This(),
        options: plan.LowerWritePlanOptions,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredWritePlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql, options, catalog);
    }

    pub fn lowerParsed(
        self: *@This(),
        parsed_sql: *const tokenized.ParsedSql,
        options: plan.LowerWritePlanOptions,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredWritePlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        var bound = try binder.bindWritePlanCatalogStatementAlloc(self.alloc, parsed_sql, options, catalog);
        defer bound.deinit(self.alloc);
        return try binder.lowerWritePlanWithBoundStatementAlloc(self.alloc, &bound, self.hooks());
    }

    fn hooks(self: *@This()) binder.WritePlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_with_options = lowerWithOptions,
        };
    }

    fn lowerWithOptions(ptr: *anyopaque, resolved_options: plan.LowerWritePlanOptions) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_options(self.alloc, self.parsed_sql.?, self.schema, self.params, resolved_options);
    }
};

pub const WritePlanLoweringCallbacks = struct {
    lower_recursive_insert_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredRecursiveInsertSource,
    lower_recursive_update_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredRecursiveJoinedMutationSource,
    lower_recursive_delete_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredRecursiveJoinedMutationSource,
    lower_recursive_merge_mutation_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredRecursiveMergeMutation,
    lower_insert_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsert,
    lower_insert_source_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsertSource,
    lower_insert_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsertSource,
    lower_update_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredJoinedMutationSource,
    classify_update_selector: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        ?relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.MutationSelectorKind,
    lower_update_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_update_source: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_delete_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredJoinedMutationSource,
    classify_delete_selector: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        ?relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.MutationSelectorKind,
    lower_delete_with_resolver: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_delete_source: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_truncate_source: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_merge_mutation_with_schemas: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredMergeMutationPlan,
};

pub const WritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8 = "",
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    callbacks: WritePlanLoweringCallbacks,
    parsed_sql: ?*const tokenized.ParsedSql = null,

    pub fn lower(self: *@This(), options: plan.LowerWritePlanOptions) !plan.LoweredWritePlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql, options);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql, options: plan.LowerWritePlanOptions) !plan.LoweredWritePlan {
        const old_parsed_sql = self.parsed_sql;
        self.parsed_sql = parsed_sql;
        defer self.parsed_sql = old_parsed_sql;
        return try plan.lowerWritePlanWithParsedSqlAlloc(parsed_sql, self.schema, options, self.hooks());
    }

    fn hooks(self: *@This()) plan.WritePlanLoweringHooks {
        return .{
            .ptr = self,
            .lower_recursive_insert_source = lowerRecursiveInsertSource,
            .lower_recursive_update_joined_source = lowerRecursiveUpdateJoinedSource,
            .lower_recursive_delete_joined_source = lowerRecursiveDeleteJoinedSource,
            .lower_recursive_merge_mutation = lowerRecursiveMergeMutation,
            .lower_insert = lowerInsert,
            .lower_insert_source = lowerInsertSource,
            .lower_insert_source_with_schema = lowerInsertSourceWithSchema,
            .lower_update_joined_source = lowerUpdateJoinedSource,
            .classify_update_selector = classifyUpdateSelector,
            .lower_update = lowerUpdate,
            .lower_update_source = lowerUpdateSource,
            .lower_delete_joined_source = lowerDeleteJoinedSource,
            .classify_delete_selector = classifyDeleteSelector,
            .lower_delete = lowerDelete,
            .lower_delete_source = lowerDeleteSource,
            .lower_truncate_source = lowerTruncateSource,
            .lower_merge_mutation = lowerMergeMutation,
        };
    }

    fn fromPtr(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }

    fn lowerRecursiveInsertSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredRecursiveInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_insert_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, resolver);
    }

    fn lowerRecursiveUpdateJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredRecursiveJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_update_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerRecursiveDeleteJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredRecursiveJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_delete_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerRecursiveMergeMutation(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredRecursiveMergeMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_merge_mutation_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params);
    }

    fn lowerInsert(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsert {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerInsertSource(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_source_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerInsertSourceWithSchema(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, resolver);
    }

    fn lowerUpdateJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn classifyUpdateSelector(ptr: *anyopaque, resolver: ?relational_rows.UniqueSelectorResolver) anyerror!plan.MutationSelectorKind {
        const self = fromPtr(ptr);
        return try self.callbacks.classify_update_selector(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerUpdate(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerUpdateSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_source(self.alloc, self.parsed_sql.?, self.schema, self.params, row_claim);
    }

    fn lowerDeleteJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_joined_source_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params, row_claim);
    }

    fn classifyDeleteSelector(ptr: *anyopaque, resolver: ?relational_rows.UniqueSelectorResolver) anyerror!plan.MutationSelectorKind {
        const self = fromPtr(ptr);
        return try self.callbacks.classify_delete_selector(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerDelete(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_with_resolver(self.alloc, self.parsed_sql.?, self.schema, self.params, resolver);
    }

    fn lowerDeleteSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_source(self.alloc, self.parsed_sql.?, self.schema, self.params, row_claim);
    }

    fn lowerTruncateSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_truncate_source(self.alloc, self.parsed_sql.?, self.schema, row_claim);
    }

    fn lowerMergeMutation(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredMergeMutationPlan {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_merge_mutation_with_schemas(self.alloc, self.parsed_sql.?, self.schema, source_schema, self.params);
    }
};

pub const ExplainPlanLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_write_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
        table_catalog.CatalogSource,
    ) anyerror!plan.LoweredWritePlan,
    lower_write_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
    ) anyerror!plan.LoweredWritePlan,
};

pub const ExplainPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    options: plan.LowerWritePlanOptions,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: ExplainPlanLoweringCallbacks,

    pub fn lower(self: *@This(), sql: []const u8) !plan.LoweredExplainPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredExplainPlan {
        return try plan.lowerExplainPlanWithParsedSqlAlloc(self.alloc, parsed_sql, self.hooks());
    }

    fn hooks(self: *@This()) plan.ExplainPlanLoweringHooks {
        return .{
            .ptr = self,
            .lower_read = lowerReadHook,
            .lower_write = lowerWriteHook,
        };
    }

    fn lowerReadHook(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_read_with_catalog(self.alloc, parsed_sql, self.schema, self.params, source_catalog, self.function_bindings);
        }
        return try self.callbacks.lower_read_without_catalog(self.alloc, parsed_sql, self.schema, self.params, self.function_bindings);
    }

    fn lowerWriteHook(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_write_with_catalog(self.alloc, parsed_sql, self.schema, self.params, self.options, source_catalog);
        }
        return try self.callbacks.lower_write_without_catalog(self.alloc, parsed_sql, self.schema, self.params, self.options);
    }
};

pub const RelationPopulationLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const RelationPopulationLoweringContext = struct {
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: RelationPopulationLoweringCallbacks,

    pub fn lower(self: *@This(), sql: []const u8) !plan.LoweredRelationPopulationPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, sql);
        defer parsed_sql.deinit(self.alloc);
        return try self.lowerParsed(&parsed_sql);
    }

    pub fn lowerParsed(self: *@This(), parsed_sql: *const tokenized.ParsedSql) !plan.LoweredRelationPopulationPlan {
        return try plan.lowerRelationPopulationPlanWithParsedSqlAlloc(self.alloc, parsed_sql, self.hooks());
    }

    fn hooks(self: *@This()) plan.RelationPopulationLoweringHooks {
        return .{
            .ptr = self,
            .lower_read = lowerRead,
        };
    }

    fn lowerRead(ptr: *anyopaque, parsed_sql: *const tokenized.ParsedSql) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_read_with_catalog(self.alloc, parsed_sql, self.schema, self.params, source_catalog, self.function_bindings);
        }
        return try self.callbacks.lower_read_without_catalog(self.alloc, parsed_sql, self.schema, self.params, self.function_bindings);
    }
};
