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
const db_mod = @import("../../storage/db/mod.zig");
const lower_expr = @import("lower_expr.zig");
const plan = @import("plan.zig");
const relational_rows = @import("../relational_rows.zig");
const table_catalog = @import("../table_catalog.zig");
const runtime_schema = @import("../../storage/schema.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

pub const ReadPlanLoweringCallbacks = struct {
    lower_lateral_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredLateralPlan,
    lower_window: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredWindowPlan,
    lower_aggregate_plan: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredAggregatePlan,
    lower_recursive_cte_plan: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredRecursiveCtePlan,
    lower_join_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredJoin,
    lower_query_plan: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredQueryPlan,
    lower_set_operation_optional_source_schema: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        ?runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredSetOperationPlan,
};

pub const ReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    source_schema: ?runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: ReadPlanLoweringCallbacks,
    statement_kind: ?classifier.SqlReadStatementKind = null,

    pub fn lower(self: *@This()) !plan.LoweredReadPlan {
        var parsed_sql = try tokenized.ParsedSql.initAlloc(self.alloc, self.sql);
        defer parsed_sql.deinit(self.alloc);
        const old_statement_kind = self.statement_kind;
        self.statement_kind = parsed_sql.tokenized_sql.read_statement_kind;
        defer self.statement_kind = old_statement_kind;
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
        return try self.callbacks.lower_lateral_with_schemas(self.alloc, self.sql, self.schema, self.joinedSourceSchema(), self.params);
    }

    fn lowerWindowHook(ptr: *anyopaque) anyerror!plan.LoweredWindowPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_window(self.alloc, self.sql, self.schema, self.params);
    }

    fn lowerAggregateHook(ptr: *anyopaque) anyerror!plan.LoweredAggregatePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_aggregate_plan(self.alloc, self.sql, self.schema, self.params);
    }

    fn lowerRecursiveCteHook(ptr: *anyopaque) anyerror!plan.LoweredRecursiveCtePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_recursive_cte_plan(self.alloc, self.sql, self.schema, self.params, self.function_bindings);
    }

    fn lowerJoinHook(ptr: *anyopaque) anyerror!plan.LoweredJoin {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_join_with_schemas(self.alloc, self.sql, self.schema, self.joinedSourceSchema(), self.params);
    }

    fn lowerQueryHook(ptr: *anyopaque) anyerror!plan.LoweredQueryPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_query_plan(self.alloc, self.sql, self.schema, self.params, self.function_bindings);
    }

    fn lowerSetOperationHook(ptr: *anyopaque) anyerror!plan.LoweredSetOperationPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_set_operation_optional_source_schema(self.alloc, self.sql, self.schema, self.source_schema, self.params, self.function_bindings);
    }
};

pub const CatalogReadPlanLoweringCallbacks = struct {
    lower_with_source_schema: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_without_source_schema: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
};

pub const CatalogReadPlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    function_bindings: lower_expr.SqlFunctionBindings,
    callbacks: CatalogReadPlanLoweringCallbacks,

    pub fn lower(
        self: *@This(),
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredReadPlan {
        return try binder.lowerReadPlanWithCatalogSourceSchemaAlloc(self.alloc, self.sql, catalog, self.hooks());
    }

    fn hooks(self: *@This()) binder.ReadPlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_with_source_schema = lowerWithSourceSchema,
            .lower_without_source_schema = lowerWithoutSourceSchema,
        };
    }

    fn lowerWithSourceSchema(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_source_schema(self.alloc, self.sql, self.schema, source_schema, self.params, self.function_bindings);
    }

    fn lowerWithoutSourceSchema(ptr: *anyopaque) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_without_source_schema(self.alloc, self.sql, self.schema, self.params, self.function_bindings);
    }
};

pub const CatalogWritePlanLoweringCallbacks = struct {
    lower_with_options: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
    ) anyerror!plan.LoweredWritePlan,
};

pub const CatalogWritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    callbacks: CatalogWritePlanLoweringCallbacks,

    pub fn lower(
        self: *@This(),
        options: plan.LowerWritePlanOptions,
        catalog: table_catalog.CatalogSource,
    ) !plan.LoweredWritePlan {
        return try binder.lowerWritePlanWithCatalogOptionsAlloc(self.alloc, self.sql, options, catalog, self.hooks());
    }

    fn hooks(self: *@This()) binder.WritePlanCatalogLoweringHooks {
        return .{
            .ptr = self,
            .lower_with_options = lowerWithOptions,
        };
    }

    fn lowerWithOptions(ptr: *anyopaque, resolved_options: plan.LowerWritePlanOptions) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.callbacks.lower_with_options(self.alloc, self.sql, self.schema, self.params, resolved_options);
    }
};

pub const WritePlanLoweringCallbacks = struct {
    lower_recursive_insert_source_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredRecursiveInsertSource,
    lower_recursive_update_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredRecursiveJoinedMutationSource,
    lower_recursive_delete_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredRecursiveJoinedMutationSource,
    lower_recursive_merge_mutation_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredRecursiveMergeMutation,
    lower_insert_with_resolver: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsert,
    lower_insert_source_with_resolver: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsertSource,
    lower_insert_source_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsertSource,
    lower_update_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredJoinedMutationSource,
    lower_update_with_resolver: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_update_source: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_delete_joined_source_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredJoinedMutationSource,
    lower_delete_with_resolver: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_delete_source: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_truncate_source: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        db_mod.types.RowClaimRequest,
    ) anyerror!plan.LoweredMutationSource,
    lower_merge_mutation_with_schemas: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredMergeMutationPlan,
};

pub const WritePlanLoweringContext = struct {
    alloc: std.mem.Allocator,
    sql: []const u8,
    schema: runtime_schema.TableSchema,
    params: []const value_mod.SqlValue,
    callbacks: WritePlanLoweringCallbacks,

    pub fn lower(self: *@This(), options: plan.LowerWritePlanOptions) !plan.LoweredWritePlan {
        return try plan.lowerWritePlanWithHooksAlloc(self.alloc, self.sql, self.schema, options, self.hooks());
    }

    fn hooks(self: *@This()) plan.WritePlanLoweringHooks {
        return .{
            .ptr = self,
            .has_recursive_insert_source = hasRecursiveInsertSource,
            .lower_recursive_insert_source = lowerRecursiveInsertSource,
            .lower_recursive_update_joined_source = lowerRecursiveUpdateJoinedSource,
            .lower_recursive_delete_joined_source = lowerRecursiveDeleteJoinedSource,
            .lower_recursive_merge_mutation = lowerRecursiveMergeMutation,
            .lower_insert = lowerInsert,
            .lower_insert_source = lowerInsertSource,
            .lower_insert_source_with_schema = lowerInsertSourceWithSchema,
            .lower_update_joined_source = lowerUpdateJoinedSource,
            .lower_update = lowerUpdate,
            .lower_update_source = lowerUpdateSource,
            .lower_delete_joined_source = lowerDeleteJoinedSource,
            .lower_delete = lowerDelete,
            .lower_delete_source = lowerDeleteSource,
            .lower_truncate_source = lowerTruncateSource,
            .lower_merge_mutation = lowerMergeMutation,
        };
    }

    fn fromPtr(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }

    fn hasRecursiveInsertSource(ptr: *anyopaque, tokens: []const plan.Token) anyerror!bool {
        const self = fromPtr(ptr);
        const maybe_recursive_tables = try binder.recursiveInsertSourceTableNamesFromTokensAlloc(self.alloc, tokens);
        if (maybe_recursive_tables) |resolved_recursive_tables| {
            var recursive_tables = resolved_recursive_tables;
            recursive_tables.deinit(self.alloc);
            return true;
        }
        return false;
    }

    fn lowerRecursiveInsertSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredRecursiveInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_insert_source_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params, resolver);
    }

    fn lowerRecursiveUpdateJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredRecursiveJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_update_joined_source_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerRecursiveDeleteJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredRecursiveJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_delete_joined_source_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerRecursiveMergeMutation(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredRecursiveMergeMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_recursive_merge_mutation_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params);
    }

    fn lowerInsert(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsert {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_with_resolver(self.alloc, self.sql, self.schema, self.params, resolver);
    }

    fn lowerInsertSource(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_source_with_resolver(self.alloc, self.sql, self.schema, self.params, resolver);
    }

    fn lowerInsertSourceWithSchema(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredInsertSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_insert_source_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params, resolver);
    }

    fn lowerUpdateJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_joined_source_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerUpdate(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_with_resolver(self.alloc, self.sql, self.schema, self.params, resolver);
    }

    fn lowerUpdateSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_update_source(self.alloc, self.sql, self.schema, self.params, row_claim);
    }

    fn lowerDeleteJoinedSource(ptr: *anyopaque, source_schema: runtime_schema.TableSchema, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredJoinedMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_joined_source_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params, row_claim);
    }

    fn lowerDelete(ptr: *anyopaque, resolver: relational_rows.UniqueSelectorResolver) anyerror!plan.LoweredMutation {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_with_resolver(self.alloc, self.sql, self.schema, self.params, resolver);
    }

    fn lowerDeleteSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_delete_source(self.alloc, self.sql, self.schema, self.params, row_claim);
    }

    fn lowerTruncateSource(ptr: *anyopaque, row_claim: db_mod.types.RowClaimRequest) anyerror!plan.LoweredMutationSource {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_truncate_source(self.alloc, self.sql, self.schema, row_claim);
    }

    fn lowerMergeMutation(ptr: *anyopaque, source_schema: runtime_schema.TableSchema) anyerror!plan.LoweredMergeMutationPlan {
        const self = fromPtr(ptr);
        return try self.callbacks.lower_merge_mutation_with_schemas(self.alloc, self.sql, self.schema, source_schema, self.params);
    }
};

pub const ExplainPlanLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_write_with_catalog: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        plan.LowerWritePlanOptions,
        table_catalog.CatalogSource,
    ) anyerror!plan.LoweredWritePlan,
    lower_write_without_catalog: *const fn (
        std.mem.Allocator,
        []const u8,
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
        return try plan.lowerExplainPlanWithParsedSqlAlloc(&parsed_sql, self.hooks());
    }

    fn hooks(self: *@This()) plan.ExplainPlanLoweringHooks {
        return .{
            .ptr = self,
            .lower_read = lowerReadHook,
            .lower_write = lowerWriteHook,
        };
    }

    fn lowerReadHook(ptr: *anyopaque, inner_sql: []const u8) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_read_with_catalog(self.alloc, inner_sql, self.schema, self.params, source_catalog, self.function_bindings);
        }
        return try self.callbacks.lower_read_without_catalog(self.alloc, inner_sql, self.schema, self.params, self.function_bindings);
    }

    fn lowerWriteHook(ptr: *anyopaque, inner_sql: []const u8) anyerror!plan.LoweredWritePlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_write_with_catalog(self.alloc, inner_sql, self.schema, self.params, self.options, source_catalog);
        }
        return try self.callbacks.lower_write_without_catalog(self.alloc, inner_sql, self.schema, self.params, self.options);
    }
};

pub const RelationPopulationLoweringCallbacks = struct {
    lower_read_with_catalog: *const fn (
        std.mem.Allocator,
        []const u8,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        table_catalog.CatalogSource,
        lower_expr.SqlFunctionBindings,
    ) anyerror!plan.LoweredReadPlan,
    lower_read_without_catalog: *const fn (
        std.mem.Allocator,
        []const u8,
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
        return try plan.lowerRelationPopulationPlanWithHooksAlloc(self.alloc, sql, self.hooks());
    }

    fn hooks(self: *@This()) plan.RelationPopulationLoweringHooks {
        return .{
            .ptr = self,
            .lower_read = lowerRead,
        };
    }

    fn lowerRead(ptr: *anyopaque, source_sql: []const u8) anyerror!plan.LoweredReadPlan {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.catalog) |source_catalog| {
            return try self.callbacks.lower_read_with_catalog(self.alloc, source_sql, self.schema, self.params, source_catalog, self.function_bindings);
        }
        return try self.callbacks.lower_read_without_catalog(self.alloc, source_sql, self.schema, self.params, self.function_bindings);
    }
};
