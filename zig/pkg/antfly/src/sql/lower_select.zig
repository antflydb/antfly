// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const binder = @import("binder.zig");
const catalog_resources = @import("catalog_resources.zig");
const document_plan = @import("document_plan.zig");
const generated_parser_mod = @import("generated_parser.zig");
const lower_dml = @import("lower_dml.zig");
const lower_expr = @import("lower_expr.zig");
const lowering_context = @import("lowering_context.zig");
const parser_context = @import("parser_context.zig");
const parser_mod = @import("parser.zig");
const plan = @import("plan.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const source_binding = @import("source_binding.zig");
const table_catalog = @import("../metadata/catalog/source.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

const sql_adapter = struct {
    const BoundSqlStatement = binder.BoundSqlStatement;
    const CatalogReadPlanLoweringContext = lowering_context.CatalogReadPlanLoweringContext;
    const DocumentBinding = source_binding.DocumentBinding;
    const LogicalSqlPlan = binder.LogicalSqlPlan;
    const ParsedSql = tokenized.ParsedSql;
    const ParserState = parser_context.ParserState;
    const ReadPlanLoweringContext = lowering_context.ReadPlanLoweringContext;
    const generated_parser = generated_parser_mod;
    const documentCapabilitiesForRuntimeSchema = source_binding.documentCapabilitiesForRuntimeSchema;
    const lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc = document_plan.lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc;
    const lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc = document_plan.lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc;
    const lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc = document_plan.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc;
    const lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc = document_plan.lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc;
    const parseAggregatePlanAlloc = plan.parseAggregatePlanAlloc;
    const parseJoinPlanAlloc = plan.parseJoinPlanAlloc;
    const parseLateralPlanAlloc = plan.parseLateralPlanAlloc;
    const parseQueryPlanAlloc = lower_expr.parseQueryPlanAlloc;
    const parseRecursiveCtePlanAlloc = plan.parseRecursiveCtePlanAlloc;
    const parseSetOperationPlanAlloc = plan.parseSetOperationPlanAlloc;
    const parseWindowPlanAlloc = plan.parseWindowPlanAlloc;
    const tokensStartWithKeywordTag = parser_mod.tokensStartWithKeywordTag;
};

pub const SqlValue = value_mod.SqlValue;
pub const ExtensionFunctionBinding = lower_expr.ExtensionFunctionBinding;
pub const SqlFunctionBindings = lower_expr.SqlFunctionBindings;

const LoweredAggregate = plan.LoweredAggregate;
const LoweredAggregatePlan = plan.LoweredAggregatePlan;
const LoweredExplainPlan = plan.LoweredExplainPlan;
const LoweredJoin = plan.LoweredJoin;
const LoweredLateralPlan = plan.LoweredLateralPlan;
const LoweredQueryPlan = plan.LoweredQueryPlan;
const LoweredReadPlan = plan.LoweredReadPlan;
const LoweredRecursiveCtePlan = plan.LoweredRecursiveCtePlan;
const LoweredRelationPopulationPlan = plan.LoweredRelationPopulationPlan;
const LoweredSelect = plan.LoweredSelect;
const LoweredSetOperationPlan = plan.LoweredSetOperationPlan;
const LoweredWindowPlan = plan.LoweredWindowPlan;
const LowerWritePlanOptions = plan.LowerWritePlanOptions;

fn generatedReadAstForParsedSql(
    parsed_sql: *const sql_adapter.ParsedSql,
    expected_kind: sql_adapter.generated_parser.GeneratedSqlReadKind,
) !?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generatedStatementKind() != .read) return null;
    _ = parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| blk: {
                    if (read.kind != expected_kind or read.cte_tokens != null or (expected_kind != .set_operation and read.set_operation_tokens != null)) return error.UnsupportedSqlShape;
                    try lowering_context.validateGeneratedReadAstForStatement(parsed_sql.items(), read);
                    break :blk read;
                },
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

fn generatedQueryPlanReadAstForParsedSql(
    parsed_sql: *const sql_adapter.ParsedSql,
) !?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generatedStatementKind() != .read) return null;
    _ = parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| blk: {
                    if ((read.kind != .query and read.kind != .set_operation) or read.cte_tokens != null) return error.UnsupportedSqlShape;
                    try lowering_context.validateGeneratedReadAstForStatement(parsed_sql.items(), read);
                    break :blk read;
                },
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

fn generatedCteReadAstForParsedSql(
    parsed_sql: *const sql_adapter.ParsedSql,
) !?*const sql_adapter.generated_parser.GeneratedSqlReadAst {
    if (parsed_sql.generatedStatementKind() != .read) return null;
    _ = parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape;
    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| {
            return switch (generated_ast.*) {
                .read => |read| blk: {
                    if (read.kind != .cte or read.cte_tokens == null) return error.UnsupportedSqlShape;
                    try lowering_context.validateGeneratedReadAstForStatement(parsed_sql.items(), read);
                    break :blk read;
                },
                else => error.UnsupportedSqlShape,
            };
        }
    }
    return error.UnsupportedSqlShape;
}

pub fn lowerSelectAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSelect {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerSelectParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

pub fn lowerSelectParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSelect {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedQueryPlanReadAstForParsedSql(parsed_sql);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseQueryPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        params,
        parser.generated_read_ast,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.queryPlanParserHooks(&parser),
        Parser.ContextAccessors.simpleSelectSetTailHooks(&parser),
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

    const table_name = lowered.table_name;
    const ctes = lowered.plan.ctes;
    const query = lowered.plan.query;
    lowered.table_name = "";
    lowered.plan.ctes = &.{};
    lowered.plan.query = .{};
    return .{
        .table_name = table_name,
        .ctes = ctes,
        .query = query,
    };
}

pub fn lowerQueryPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredQueryPlan {
    return try lowerQueryPlanWithExtensionFunctionsAlloc(alloc, sql, schema, params, &.{});
}

pub fn lowerQueryPlanWithExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredQueryPlan {
    return try lowerQueryPlanWithFunctionBindingsAlloc(alloc, sql, schema, params, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerQueryPlanWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredQueryPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, params, function_bindings);
}

pub fn lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredQueryPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedQueryPlanReadAstForParsedSql(parsed_sql);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseQueryPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        params,
        parser.generated_read_ast,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.queryPlanParserHooks(&parser),
        Parser.ContextAccessors.simpleSelectSetTailHooks(&parser),
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

pub fn lowerReadPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, .{});
}

pub fn lowerReadPlanWithExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredReadPlan {
    return try lowerReadPlanWithFunctionBindingsAlloc(alloc, sql, schema, params, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerReadPlanWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, function_bindings);
}

pub fn lowerReadPlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, null, params, function_bindings);
}

pub fn lowerReadPlanWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, &.{});
}

pub fn lowerReadPlanWithSchemasAndExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredReadPlan {
    return try lowerReadPlanWithSchemasAndFunctionBindingsAlloc(alloc, sql, schema, source_schema, params, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerReadPlanWithSchemasAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, function_bindings);
}

pub fn lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, source_schema, params, function_bindings);
}

pub fn lowerReadPlanWithOptionalSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params, function_bindings);
}

pub fn lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    if (schema.storage_mode == .document) {
        if (source_schema != null) return error.DocumentSqlUnsupportedJoin;
        const document_capabilities = sql_adapter.documentCapabilitiesForRuntimeSchema(schema);
        return try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
            alloc,
            parsed_sql,
            schema,
            null,
            .{},
            document_capabilities,
        );
    }
    var context = sql_adapter.ReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .source_schema = source_schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_lateral_with_schemas = lowerLateralPlanWithSchemasParsedSqlAlloc,
            .lower_window = lowerWindowPlanParsedSqlAlloc,
            .lower_aggregate_plan = lowerAggregatePlanParsedSqlAlloc,
            .lower_recursive_cte_plan = lowerRecursiveCtePlanParsedSqlAlloc,
            .lower_join_with_schemas = lowerJoinWithSchemasParsedSqlAlloc,
            .lower_query_plan = lowerQueryPlanWithFunctionBindingsParsedSqlAlloc,
            .lower_set_operation_optional_source_schema = lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

pub fn lowerRecursiveCtePlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveCtePlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRecursiveCtePlanParsedSqlAlloc(alloc, &parsed_sql, schema, params, function_bindings);
}

pub fn lowerRecursiveCtePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredRecursiveCtePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const generated_read_ast = try generatedCteReadAstForParsedSql(parsed_sql);
    if (generated_read_ast) |read_ast| {
        if (!read_ast.cte_recursive) return error.UnsupportedSqlShape;
        try lowering_context.validateGeneratedReadAstForStatement(tokens, read_ast);
    }

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .generated_read_ast = generated_read_ast,
    };
    return try sql_adapter.parseRecursiveCtePlanAlloc(alloc, tokens, &parser.pos, Parser.ContextAccessors.recursiveCteParserHooks(&parser));
}

pub fn lowerSetOperationPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, .{});
}

pub fn lowerSetOperationPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, null, params, .{});
}

pub fn lowerSetOperationPlanWithFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, null, params, function_bindings);
}

pub fn lowerSetOperationPlanWithFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, null, params, function_bindings);
}

pub fn lowerSetOperationPlanWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, .{});
}

pub fn lowerSetOperationPlanWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, source_schema, params, .{});
}

pub fn lowerSetOperationPlanWithSchemasAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaAlloc(alloc, sql, schema, source_schema, params, function_bindings);
}

pub fn lowerSetOperationPlanWithSchemasAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    return try lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, parsed_sql, schema, source_schema, params, function_bindings);
}

pub fn lowerSetOperationPlanWithOptionalSourceSchemaAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params, function_bindings);
}

pub fn lowerSetOperationPlanWithOptionalSourceSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredSetOperationPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema) |joined_source_schema| {
        if (joined_source_schema.storage_mode != .relational or joined_source_schema.primary_key == null) return error.InvalidSqlCatalog;
    }
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedReadAstForParsedSql(parsed_sql, .set_operation);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
        .function_bindings = function_bindings,
    };
    return try sql_adapter.parseSetOperationPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        source_schema orelse schema,
        source_schema != null,
        Parser.ContextAccessors.setOperationParserHooks(&parser),
    );
}

pub fn lowerReadPlanWithCatalogAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogAndExtensionFunctionsAlloc(alloc, sql, schema, params, catalog, &.{});
}

pub fn lowerReadPlanWithCatalogAndExtensionFunctionsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    extension_function_bindings: []const ExtensionFunctionBinding,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, catalog, .{ .extension_functions = extension_function_bindings });
}

pub fn lowerReadPlanWithCatalogAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogSessionAndFunctionBindingsAlloc(alloc, sql, schema, params, catalog, catalog_resources.SqlCatalogSession.default(), function_bindings);
}

pub fn lowerReadPlanWithCatalogSessionAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, params, catalog, session, function_bindings);
}

pub fn lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    return try lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(alloc, parsed_sql, schema, params, catalog, catalog_resources.SqlCatalogSession.default(), function_bindings);
}

pub fn lowerReadPlanWithCatalogSessionAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: table_catalog.CatalogSource,
    session: catalog_resources.SqlCatalogSession,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var context = sql_adapter.CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_document_target = lowerDocumentReadPlanFromBindingParsedSqlAlloc,
            .lower_with_source_schema = lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_without_source_schema = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerParsedWithSession(parsed_sql, catalog, session);
}

pub fn lowerExplainPlanWithOptionsCatalogAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    options: LowerWritePlanOptions,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredExplainPlan {
    var context = lowering_context.ExplainPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .options = options,
        .catalog = catalog,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_read_with_catalog = lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc,
            .lower_read_without_catalog = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
            .lower_write_with_catalog = lower_dml.lowerWritePlanWithCatalogParsedSqlAlloc,
            .lower_write_without_catalog = lower_dml.lowerWritePlanParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

pub fn lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredRelationPopulationPlan {
    var context = lowering_context.RelationPopulationLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .catalog = catalog,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_read_with_catalog = lowerReadPlanWithCatalogAndFunctionBindingsParsedSqlAlloc,
            .lower_read_without_catalog = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerParsed(parsed_sql);
}

pub fn lowerRelationPopulationPlanWithCatalogAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
    function_bindings: SqlFunctionBindings,
) !LoweredRelationPopulationPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerRelationPopulationPlanWithCatalogAndFunctionBindingsParsedSqlAlloc(
        alloc,
        &parsed_sql,
        schema,
        params,
        catalog,
        function_bindings,
    );
}

pub fn lowerRelationPopulationPlanWithCatalogAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    catalog: ?table_catalog.CatalogSource,
) !LoweredRelationPopulationPlan {
    return try lowerRelationPopulationPlanWithCatalogAndFunctionBindingsAlloc(alloc, sql, schema, params, catalog, .{});
}

pub fn lowerReadPlanWithBoundStatementAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    bound: *sql_adapter.BoundSqlStatement,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var context = sql_adapter.CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_document_target = lowerDocumentReadPlanFromBindingParsedSqlAlloc,
            .lower_with_source_schema = lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_without_source_schema = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerBoundParsed(parsed_sql, bound);
}

pub fn lowerReadPlanWithLogicalPlanAndFunctionBindingsAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    logical: *sql_adapter.LogicalSqlPlan,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    var context = sql_adapter.CatalogReadPlanLoweringContext{
        .alloc = alloc,
        .schema = schema,
        .params = params,
        .function_bindings = function_bindings,
        .callbacks = .{
            .lower_document_target = lowerDocumentReadPlanFromBindingParsedSqlAlloc,
            .lower_with_source_schema = lowerReadPlanWithSchemasAndFunctionBindingsParsedSqlAlloc,
            .lower_without_source_schema = lowerReadPlanWithFunctionBindingsParsedSqlAlloc,
        },
    };
    return try context.lowerLogicalParsed(parsed_sql, logical);
}

pub fn lowerDocumentReadPlanFromBindingParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    document: sql_adapter.DocumentBinding,
    params: []const SqlValue,
    function_bindings: SqlFunctionBindings,
) !LoweredReadPlan {
    _ = params;
    _ = function_bindings;
    return try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
        alloc,
        parsed_sql,
        document.schema,
        document.indexes_json,
        document.virtual_schema,
        document.capabilities,
    );
}

fn lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    indexes_json: ?[]const u8,
    virtual_schema: source_binding.DocumentSqlSchema,
    capabilities: source_binding.DocumentSqlCapabilities,
) !LoweredReadPlan {
    const read_kind = parsed_sql.readStatementKindIncludingGeneratedAst() orelse
        parsed_sql.generatedReadStatementKind() orelse
        parsed_sql.readStatementKind() orelse
        return error.UnsupportedSqlShape;
    return switch (read_kind) {
        .aggregate => .{
            .document_aggregate = try sql_adapter.lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(
                alloc,
                parsed_sql,
                schema,
                virtual_schema,
                indexes_json,
                capabilities,
            ),
        },
        .query => .{
            .document_query = try sql_adapter.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
                alloc,
                parsed_sql,
                schema,
                virtual_schema,
                capabilities,
            ),
        },
        .join, .lateral => {
            const document_query = sql_adapter.lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
                alloc,
                parsed_sql,
                schema,
                virtual_schema,
                capabilities,
            ) catch |err| switch (err) {
                error.UnsupportedSqlShape => return error.DocumentSqlUnsupportedJoin,
                else => return err,
            };
            return .{
                .document_query = document_query,
            };
        },
        .window => error.DocumentSqlWindowUnsupported,
        .set_operation, .recursive_cte => error.DocumentSqlUnsupportedJoin,
    };
}

pub fn lowerWindowPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredWindowPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerWindowPlanParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

pub fn lowerWindowPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredWindowPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedReadAstForParsedSql(parsed_sql, .window);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseWindowPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.windowPlanParserHooks(&parser),
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

pub fn lowerAggregateAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregate {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerAggregateParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

pub fn lowerAggregateParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregate {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedReadAstForParsedSql(parsed_sql, .aggregate);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    return Parser.ContextAccessors.parseAggregate(&parser) catch |err| switch (err) {
        error.InvalidRowsRequest => return error.UnsupportedSqlShape,
        else => return err,
    };
}

pub fn lowerAggregatePlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregatePlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerAggregatePlanParsedSqlAlloc(alloc, &parsed_sql, schema, params);
}

pub fn lowerAggregatePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredAggregatePlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedReadAstForParsedSql(parsed_sql, .aggregate);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseAggregatePlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.aggregatePlanParserHooks(&parser),
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

pub fn lowerJoinAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    return try lowerJoinWithSchemasAlloc(alloc, sql, schema, schema, params);
}

pub fn lowerJoinParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    return try lowerJoinWithSchemasParsedSqlAlloc(alloc, parsed_sql, schema, schema, params);
}

pub fn lowerJoinWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerJoinWithSchemasParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params);
}

pub fn lowerJoinWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredJoin {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedReadAstForParsedSql(parsed_sql, .join);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseJoinPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.joinCteSelectParserHooks(&parser),
        Parser.ContextAccessors.joinPlanParserHooks(&parser),
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

pub fn lowerLateralPlanAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    return try lowerLateralPlanWithSchemasAlloc(alloc, sql, schema, schema, params);
}

pub fn lowerLateralPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    return try lowerLateralPlanWithSchemasParsedSqlAlloc(alloc, parsed_sql, schema, schema, params);
}

pub fn lowerLateralPlanWithSchemasAlloc(
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(alloc, sql);
    defer parsed_sql.deinit(alloc);
    return try lowerLateralPlanWithSchemasParsedSqlAlloc(alloc, &parsed_sql, schema, source_schema, params);
}

pub fn lowerLateralPlanWithSchemasParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const sql_adapter.ParsedSql,
    schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    params: []const SqlValue,
) !LoweredLateralPlan {
    if (schema.storage_mode != .relational or schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational or source_schema.primary_key == null) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    const cte_adapter_shape = sql_adapter.tokensStartWithKeywordTag(tokens, .with);
    const generated_read_ast = if (cte_adapter_shape)
        try generatedCteReadAstForParsedSql(parsed_sql)
    else
        try generatedReadAstForParsedSql(parsed_sql, .lateral);

    var parser = Parser{
        .alloc = alloc,
        .tokens = tokens,
        .schema = schema,
        .joined_source_schema = source_schema,
        .params = params,
        .generated_read_ast = generated_read_ast,
    };
    var lowered = sql_adapter.parseLateralPlanAlloc(
        alloc,
        tokens,
        &parser.pos,
        Parser.ContextAccessors.cteSelectParserHooks(&parser),
        Parser.ContextAccessors.lateralPlanParserHooks(&parser),
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

const Parser = sql_adapter.ParserState;

test "recursive cte lowerer validates retained generated ast before token fallback" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
            .{ .name = "kind", .path = "kind", .field_type = .keyword },
            .{ .name = "customer_id", .path = "customer_id", .field_type = .keyword },
        },
        .primary_key = .{ .columns = &.{"id"} },
    };

    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "WITH RECURSIVE source_rows AS (SELECT id FROM usage_records WHERE kind = 'order' UNION ALL SELECT child.id FROM usage_records AS child JOIN source_rows AS parent ON child.customer_id = parent.id) SELECT id FROM source_rows",
    );
    defer parsed_sql.deinit(alloc);

    var lowered = try lowerRecursiveCtePlanParsedSqlAlloc(alloc, &parsed_sql, schema, &.{}, .{});
    defer lowered.deinit(alloc);

    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .read => |read| read.cte_recursive = false,
            else => return error.TestUnexpectedResult,
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerRecursiveCtePlanParsedSqlAlloc(alloc, &parsed_sql, schema, &.{}, .{}),
    );
}

test "read lowerers validate retained generated ast before typed planning" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
            .{ .name = "kind", .path = "kind", .field_type = .keyword },
        },
        .primary_key = .{ .columns = &.{"id"} },
    };

    var parsed_sql = try sql_adapter.ParsedSql.initAlloc(
        alloc,
        "WITH source_rows AS (SELECT id FROM usage_records WHERE kind = 'order') SELECT id FROM source_rows",
    );
    defer parsed_sql.deinit(alloc);

    var lowered = try lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, &.{}, .{});
    defer lowered.deinit(alloc);

    if (parsed_sql.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .read => |read| read.cte_final_kind = .join,
            else => return error.TestUnexpectedResult,
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;

    try std.testing.expectError(
        error.UnsupportedSqlShape,
        lowerQueryPlanWithFunctionBindingsParsedSqlAlloc(alloc, &parsed_sql, schema, &.{}, .{}),
    );
}
