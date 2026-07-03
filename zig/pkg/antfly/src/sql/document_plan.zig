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

const document_sql_corpus = @import("document_sql_corpus.zig");
const lowering_context = @import("lowering_context.zig");
const query_function = @import("query_function.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const source_binding = @import("source_binding.zig");
const sql_statement_kind = @import("statement_kind.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");

const Token = token_mod.Token;
const TokenKeyword = token_mod.TokenKeyword;

const ParsedDocumentWhere = struct {
    id_lookup_seen: bool = false,
    ids: std.ArrayListUnmanaged([]const u8) = .empty,
    filter_clauses: std.ArrayListUnmanaged([]const u8) = .empty,
    full_text_query: ?[]const u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ids.items) |id| alloc.free(@constCast(id));
        self.ids.deinit(alloc);
        for (self.filter_clauses.items) |clause| alloc.free(@constCast(clause));
        self.filter_clauses.deinit(alloc);
        if (self.full_text_query) |query| alloc.free(@constCast(query));
        self.* = undefined;
    }
};

const DocumentWhereClauseRange = struct {
    start: usize,
    end: usize,
};

fn validatedDocumentReadStatementKind(parsed_sql: *const tokenized.ParsedSql) !sql_statement_kind.SqlReadStatementKind {
    const generated_kind = parsed_sql.generatedReadStatementKind();
    const kind = parsed_sql.readStatementKindIncludingGeneratedAst() orelse
        generated_kind orelse
        parsed_sql.readStatementKind() orelse
        return error.UnsupportedSqlShape;
    if (parsed_sql.generatedStatementKind() == .read) {
        _ = generated_kind orelse return kind;
        const generated_statement = parsed_sql.generated_statement orelse return error.UnsupportedSqlShape;
        const generated_ast = generated_statement.ast orelse return error.UnsupportedSqlShape;
        switch (generated_ast) {
            .read => |read| try lowering_context.validateGeneratedReadAstForStatement(parsed_sql.items(), read),
            else => return error.UnsupportedSqlShape,
        }
    }
    return kind;
}

const DocumentProducerCapabilities = struct {
    indexed_scalar_filters: bool = true,
    indexed_scalar_filter_paths: []const []const u8 = &.{},
    indexed_array_element_paths: []const []const u8 = &.{},
    runtime_schema_scalar_filters: ?runtime_schema.TableSchema = null,
    full_text_filters: bool = true,
    full_text_indexes: []const source_binding.DocumentSqlFullTextIndex = &.{},
    semantic_filters: bool = false,
    semantic_index_names: []const []const u8 = &.{},
    vector_filters: bool = false,
    vector_index_names: []const []const u8 = &.{},
    hybrid_filters: bool = false,
    graph_filters: bool = false,
    graph_index_names: []const []const u8 = &.{},
    graph_metric_filters: bool = false,
    graph_metric_index_names: []const []const u8 = &.{},
    residual_candidate_limit: ?u32 = null,
};

const DocumentSourceRef = struct {
    table_name: []const u8,
    alias: ?[]const u8 = null,

    fn matchesQualifier(self: DocumentSourceRef, qualifier: []const u8) bool {
        if (std.ascii.eqlIgnoreCase(self.table_name, qualifier)) return true;
        if (self.alias) |alias| return std.ascii.eqlIgnoreCase(alias, qualifier);
        return false;
    }
};

const DocumentFromBinding = struct {
    source_ref: DocumentSourceRef,
    unnest: ?DocumentUnnest = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.unnest) |*unnest| unnest.deinit(alloc);
        self.* = undefined;
    }
};

pub const DocumentProjectionKind = enum {
    id,
    doc,
    field,
    unnest_value,
};

pub const DocumentProjection = struct {
    kind: DocumentProjectionKind,
    field: []const u8 = "",
    output: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        alloc.free(@constCast(self.output));
        self.* = undefined;
    }
};

pub const DocumentIndexQuery = struct {
    index_name: ?[]const u8 = null,
    native_query_json: ?[]const u8 = null,
    full_text_query: ?[]const u8 = null,
    filter_query_json: ?[]const u8 = null,
    residual_filter_json: ?[]const u8 = null,
    max_candidate_rows: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.index_name) |index_name| alloc.free(@constCast(index_name));
        if (self.native_query_json) |body| alloc.free(@constCast(body));
        if (self.full_text_query) |query| alloc.free(@constCast(query));
        if (self.filter_query_json) |query| alloc.free(@constCast(query));
        if (self.residual_filter_json) |query| alloc.free(@constCast(query));
        self.* = undefined;
    }
};

pub const DocumentAggregateOp = enum {
    count,
    sum,
    avg,
    min,
    max,
};

pub const DocumentAggregateInput = struct {
    field: []const u8,
    source_field: []const u8,
    field_type: runtime_schema.AntflyType,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        if (self.source_field.len > 0) alloc.free(@constCast(self.source_field));
        self.* = undefined;
    }
};

pub const DocumentAggregateGroupBy = struct {
    field: []const u8,
    source_field: []const u8,
    field_type: runtime_schema.AntflyType,
    output: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        if (self.source_field.len > 0) alloc.free(@constCast(self.source_field));
        if (self.output.len > 0) alloc.free(@constCast(self.output));
        self.* = undefined;
    }
};

pub const DocumentAggregateSpec = struct {
    op: DocumentAggregateOp,
    output: []const u8,
    input: ?DocumentAggregateInput = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.output.len > 0) alloc.free(@constCast(self.output));
        if (self.input) |*input| input.deinit(alloc);
        self.* = undefined;
    }
};

pub const DocumentAggregateOrderKey = enum {
    group,
    aggregate,
};

pub const DocumentAggregateOrderBy = struct {
    key: DocumentAggregateOrderKey,
    field_type: runtime_schema.AntflyType,
    direction: DocumentOrderDirection = .asc,
};

pub const DocumentAggregateHavingOp = enum {
    eq,
    neq,
    gt,
    gte,
    lt,
    lte,
};

pub const DocumentAggregateHavingPredicate = struct {
    key: DocumentAggregateOrderKey,
    field_type: runtime_schema.AntflyType,
    op: DocumentAggregateHavingOp,
    value_json: []const u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.value_json.len > 0) alloc.free(@constCast(self.value_json));
        self.* = undefined;
    }
};

pub const DocumentAlgebraicAggregatePlan = struct {
    table_name: []const u8,
    index_name: ?[]const u8 = null,
    materialization_name: ?[]const u8 = null,
    candidate_producer: ?DocumentProducer = null,
    filter_query_json: ?[]const u8 = null,
    group_by: ?DocumentAggregateGroupBy = null,
    aggregate: DocumentAggregateSpec,
    having: []DocumentAggregateHavingPredicate = &.{},
    order_by: ?DocumentAggregateOrderBy = null,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.index_name) |index_name| alloc.free(@constCast(index_name));
        if (self.materialization_name) |materialization_name| alloc.free(@constCast(materialization_name));
        if (self.candidate_producer) |*producer| producer.deinit(alloc);
        if (self.filter_query_json) |filter| alloc.free(@constCast(filter));
        if (self.group_by) |*group_by| group_by.deinit(alloc);
        self.aggregate.deinit(alloc);
        deinitDocumentAggregateHavingPredicates(alloc, self.having);
        self.* = undefined;
    }
};

fn deinitDocumentAggregateHavingPredicates(alloc: std.mem.Allocator, predicates: []DocumentAggregateHavingPredicate) void {
    for (predicates) |*predicate| predicate.deinit(alloc);
    if (predicates.len > 0) alloc.free(predicates);
}

pub const DocumentOrderDirection = enum {
    asc,
    desc,
};

pub const DocumentOrderBy = struct {
    field: []const u8,
    field_type: runtime_schema.AntflyType,
    direction: DocumentOrderDirection = .asc,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        self.* = undefined;
    }
};

pub const DocumentUnnest = struct {
    field: []const u8,
    alias: []const u8,
    item_type: runtime_schema.AntflyType,
    filter_value_json: ?[]const u8 = null,
    filter_values_json: ?[]const u8 = null,
    filter_range_json: ?[]const u8 = null,
    filter_not_value_json: ?[]const u8 = null,
    filter_not_query_json: ?[]const u8 = null,
    filter_pattern_json: ?[]const u8 = null,
    filter_pattern_query_json: ?[]const u8 = null,
    filter_pattern_case_insensitive: bool = false,
    filter_is_not_null: bool = false,
    filter_match_none: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        if (self.alias.len > 0) alloc.free(@constCast(self.alias));
        if (self.filter_value_json) |value| alloc.free(@constCast(value));
        if (self.filter_values_json) |values| alloc.free(@constCast(values));
        if (self.filter_range_json) |range| alloc.free(@constCast(range));
        if (self.filter_not_value_json) |value| alloc.free(@constCast(value));
        if (self.filter_not_query_json) |query| alloc.free(@constCast(query));
        if (self.filter_pattern_json) |pattern| alloc.free(@constCast(pattern));
        if (self.filter_pattern_query_json) |query| alloc.free(@constCast(query));
        self.* = undefined;
    }

    fn takeField(self: *@This()) []const u8 {
        const value = self.field;
        self.field = "";
        return value;
    }

    fn takeAlias(self: *@This()) []const u8 {
        const value = self.alias;
        self.alias = "";
        return value;
    }

    fn takeFilterValueJson(self: *@This()) ?[]const u8 {
        const value = self.filter_value_json;
        self.filter_value_json = null;
        return value;
    }

    fn takeFilterValuesJson(self: *@This()) ?[]const u8 {
        const values = self.filter_values_json;
        self.filter_values_json = null;
        return values;
    }

    fn takeFilterRangeJson(self: *@This()) ?[]const u8 {
        const range = self.filter_range_json;
        self.filter_range_json = null;
        return range;
    }

    fn takeFilterNotValueJson(self: *@This()) ?[]const u8 {
        const value = self.filter_not_value_json;
        self.filter_not_value_json = null;
        return value;
    }

    fn takeFilterNotQueryJson(self: *@This()) ?[]const u8 {
        const query = self.filter_not_query_json;
        self.filter_not_query_json = null;
        return query;
    }

    fn takeFilterPatternJson(self: *@This()) ?[]const u8 {
        const pattern = self.filter_pattern_json;
        self.filter_pattern_json = null;
        return pattern;
    }

    fn takeFilterPatternQueryJson(self: *@This()) ?[]const u8 {
        const query = self.filter_pattern_query_json;
        self.filter_pattern_query_json = null;
        return query;
    }

    fn takeFilterPatternCaseInsensitive(self: *@This()) bool {
        const value = self.filter_pattern_case_insensitive;
        self.filter_pattern_case_insensitive = false;
        return value;
    }

    fn takeFilterIsNotNull(self: *@This()) bool {
        const value = self.filter_is_not_null;
        self.filter_is_not_null = false;
        return value;
    }

    fn takeFilterMatchNone(self: *@This()) bool {
        const value = self.filter_match_none;
        self.filter_match_none = false;
        return value;
    }
};

pub const DocumentReadViewMapping = struct {
    name: []const u8,
    source_table: []const u8 = "",
    required_indexes: usize = 0,
    required_indexes_ready: bool = false,
    source_generation_fresh: bool = false,
    source_schema_fingerprint_fresh: bool = false,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.name.len > 0) alloc.free(@constCast(self.name));
        if (self.source_table.len > 0) alloc.free(@constCast(self.source_table));
        self.* = undefined;
    }
};

pub const DocumentIdLookup = struct {
    ids: []const []const u8,
    residual_filter_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ids) |id| alloc.free(@constCast(id));
        if (self.ids.len > 0) alloc.free(self.ids);
        if (self.residual_filter_json) |filter| alloc.free(@constCast(filter));
        self.* = undefined;
    }
};

pub const DocumentProducer = union(enum) {
    id_lookup: DocumentIdLookup,
    indexed_query: DocumentIndexQuery,
    bounded_scan: BoundedDocumentScan,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        switch (self.*) {
            .id_lookup => |*lookup| lookup.deinit(alloc),
            .indexed_query => |*query| query.deinit(alloc),
            .bounded_scan => |*scan| scan.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const BoundedDocumentScan = struct {
    max_rows: u32,
    max_bytes: ?u64 = null,
    residual_filter_json: ?[]const u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.residual_filter_json) |filter| alloc.free(@constCast(filter));
        self.* = undefined;
    }
};

pub fn lowerDocumentMutationProducerFromWhereAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    where_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
    table_name: []const u8,
    alias: []const u8,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !DocumentProducer {
    if (schema.storage_mode != .document) return error.InvalidSqlCatalog;
    if (where_index >= end_index or end_index > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[where_index].matchesKeywordTag(.where)) return error.UnsupportedSqlShape;
    const source_ref = DocumentSourceRef{
        .table_name = table_name,
        .alias = if (std.ascii.eqlIgnoreCase(table_name, alias)) null else alias,
    };
    const producer_capabilities = documentProducerCapabilitiesForRuntimeSchema(schema, bounded_scan_policy);
    return parseWhereProducerAlloc(
        alloc,
        tokens,
        where_index,
        end_index,
        schema,
        .{},
        source_ref,
        producer_capabilities,
        null,
    ) catch |err| switch (err) {
        error.DocumentSqlIndexUnavailable => if (bounded_scan_policy) |policy|
            try parseWhereBoundedScanProducerAlloc(
                alloc,
                tokens,
                where_index,
                end_index,
                schema,
                .{},
                source_ref,
                policy,
                true,
                null,
            )
        else
            return error.DocumentSqlBoundedScanMissingExactProducer,
        else => return err,
    };
}

pub fn lowerDocumentMutationBoundedScanProducerFromClauseAlloc(
    alloc: std.mem.Allocator,
    clause_tokens: []const Token,
    schema: runtime_schema.TableSchema,
    table_name: []const u8,
    alias: []const u8,
    bounded_scan_policy: source_binding.BoundedScanPolicy,
) !DocumentProducer {
    if (schema.storage_mode != .document) return error.InvalidSqlCatalog;
    if (clause_tokens.len == 0) return error.UnsupportedSqlShape;
    const source_ref = DocumentSourceRef{
        .table_name = table_name,
        .alias = if (std.ascii.eqlIgnoreCase(table_name, alias)) null else alias,
    };
    const residual_filter_json = (try parseScalarFilterClauseWithIndexRequirementAlloc(
        alloc,
        clause_tokens,
        schema,
        .{},
        source_ref,
        false,
    )) orelse return error.UnsupportedSqlShape;
    errdefer alloc.free(@constCast(residual_filter_json));
    var scan = try boundedDocumentScanFromPolicy(bounded_scan_policy);
    scan.residual_filter_json = residual_filter_json;
    return .{ .bounded_scan = scan };
}

pub const DocumentReadPlan = struct {
    table_name: []const u8,
    view_mapping: ?DocumentReadViewMapping = null,
    projection: []DocumentProjection,
    producer: DocumentProducer,
    order_by: ?DocumentOrderBy = null,
    unnest: ?DocumentUnnest = null,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.view_mapping) |*view_mapping| view_mapping.deinit(alloc);
        for (self.projection) |*projection| projection.deinit(alloc);
        if (self.projection.len > 0) alloc.free(self.projection);
        self.producer.deinit(alloc);
        if (self.order_by) |*order_by| order_by.deinit(alloc);
        if (self.unnest) |*unnest| unnest.deinit(alloc);
        self.* = undefined;
    }
};

fn combineDocumentFilterJsonAlloc(
    alloc: std.mem.Allocator,
    existing_filter_json: ?[]const u8,
    additional_filter_json: []const u8,
) ![]const u8 {
    const existing = existing_filter_json orelse return try alloc.dupe(u8, additional_filter_json);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"conjuncts\":[{s},{s}]}}",
        .{ existing, additional_filter_json },
    );
}

fn applyDocumentFilterConstraintAlloc(
    alloc: std.mem.Allocator,
    filter_json: *?[]const u8,
    additional_filter_json: []const u8,
) !void {
    const combined = try combineDocumentFilterJsonAlloc(alloc, filter_json.*, additional_filter_json);
    if (filter_json.*) |existing| alloc.free(@constCast(existing));
    filter_json.* = combined;
}

fn applyDocumentProducerFilterConstraintAlloc(
    alloc: std.mem.Allocator,
    producer: *DocumentProducer,
    filter_json: []const u8,
) !void {
    switch (producer.*) {
        .id_lookup => |*lookup| try applyDocumentFilterConstraintAlloc(alloc, &lookup.residual_filter_json, filter_json),
        .indexed_query => |*query| {
            try applyDocumentFilterConstraintAlloc(alloc, &query.residual_filter_json, filter_json);
            if (query.max_candidate_rows == null) {
                query.max_candidate_rows = source_binding.default_document_sql_bounded_scan_rows;
            }
        },
        .bounded_scan => |*scan| try applyDocumentFilterConstraintAlloc(alloc, &scan.residual_filter_json, filter_json),
    }
}

pub fn applyDocumentReadPlanFilterConstraintAlloc(
    alloc: std.mem.Allocator,
    plan: *DocumentReadPlan,
    filter_json: []const u8,
) !void {
    try applyDocumentProducerFilterConstraintAlloc(alloc, &plan.producer, filter_json);
}

pub fn applyDocumentAggregatePlanFilterConstraintAlloc(
    alloc: std.mem.Allocator,
    plan: *DocumentAlgebraicAggregatePlan,
    filter_json: []const u8,
) !void {
    try applyDocumentFilterConstraintAlloc(alloc, &plan.filter_query_json, filter_json);
    if (plan.candidate_producer) |*producer| {
        try applyDocumentProducerFilterConstraintAlloc(alloc, producer, filter_json);
    }
}

pub fn lowerDocumentReadPlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
) !DocumentReadPlan {
    return try lowerDocumentReadPlanInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        .{},
        null,
        documentProducerCapabilitiesForRuntimeSchema(schema, null),
    );
}

pub fn lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !DocumentReadPlan {
    return try lowerDocumentReadPlanInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        .{},
        bounded_scan_policy,
        documentProducerCapabilitiesForRuntimeSchema(schema, bounded_scan_policy),
    );
}

pub fn lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    capabilities: source_binding.DocumentSqlCapabilities,
) !DocumentReadPlan {
    return try lowerDocumentReadPlanInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        .{},
        capabilities.bounded_scan,
        .{
            .indexed_scalar_filters = capabilities.indexed_scalar_filters,
            .indexed_scalar_filter_paths = capabilities.indexed_scalar_filter_paths,
            .indexed_array_element_paths = capabilities.indexed_array_element_paths,
            .runtime_schema_scalar_filters = capabilities.runtime_schema_scalar_filters,
            .full_text_filters = capabilities.full_text_filters,
            .full_text_indexes = capabilities.full_text_indexes,
            .semantic_filters = capabilities.semantic_filters,
            .semantic_index_names = capabilities.semantic_index_names,
            .vector_filters = capabilities.vector_filters,
            .vector_index_names = capabilities.vector_index_names,
            .hybrid_filters = capabilities.hybrid_filters,
            .graph_filters = capabilities.graph_filters,
            .graph_index_names = capabilities.graph_index_names,
            .graph_metric_filters = capabilities.graph_metric_filters,
            .graph_metric_index_names = capabilities.graph_metric_index_names,
            .residual_candidate_limit = documentProducerResidualCandidateLimit(capabilities.bounded_scan),
        },
    );
}

pub fn lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    capabilities: source_binding.DocumentSqlCapabilities,
) !DocumentReadPlan {
    return try lowerDocumentReadPlanInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        virtual_schema,
        capabilities.bounded_scan,
        .{
            .indexed_scalar_filters = capabilities.indexed_scalar_filters,
            .indexed_scalar_filter_paths = capabilities.indexed_scalar_filter_paths,
            .indexed_array_element_paths = capabilities.indexed_array_element_paths,
            .runtime_schema_scalar_filters = capabilities.runtime_schema_scalar_filters,
            .full_text_filters = capabilities.full_text_filters,
            .full_text_indexes = capabilities.full_text_indexes,
            .semantic_filters = capabilities.semantic_filters,
            .semantic_index_names = capabilities.semantic_index_names,
            .vector_filters = capabilities.vector_filters,
            .vector_index_names = capabilities.vector_index_names,
            .hybrid_filters = capabilities.hybrid_filters,
            .graph_filters = capabilities.graph_filters,
            .graph_index_names = capabilities.graph_index_names,
            .graph_metric_filters = capabilities.graph_metric_filters,
            .graph_metric_index_names = capabilities.graph_metric_index_names,
            .residual_candidate_limit = documentProducerResidualCandidateLimit(capabilities.bounded_scan),
        },
    );
}

fn documentProducerResidualCandidateLimit(bounded_scan_policy: ?source_binding.BoundedScanPolicy) ?u32 {
    const policy = bounded_scan_policy orelse return null;
    const max_rows = policy.max_rows orelse return null;
    if (max_rows == 0) return null;
    return max_rows;
}

fn documentProducerCapabilitiesForRuntimeSchema(
    schema: runtime_schema.TableSchema,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) DocumentProducerCapabilities {
    const capabilities = source_binding.documentCapabilitiesForRuntimeSchema(schema);
    return .{
        .indexed_scalar_filters = false,
        .indexed_scalar_filter_paths = capabilities.indexed_scalar_filter_paths,
        .indexed_array_element_paths = capabilities.indexed_array_element_paths,
        .runtime_schema_scalar_filters = schema,
        .full_text_filters = capabilities.full_text_filters,
        .full_text_indexes = capabilities.full_text_indexes,
        .semantic_filters = capabilities.semantic_filters,
        .semantic_index_names = capabilities.semantic_index_names,
        .vector_filters = capabilities.vector_filters,
        .vector_index_names = capabilities.vector_index_names,
        .hybrid_filters = capabilities.hybrid_filters,
        .graph_filters = capabilities.graph_filters,
        .graph_index_names = capabilities.graph_index_names,
        .graph_metric_filters = capabilities.graph_metric_filters,
        .graph_metric_index_names = capabilities.graph_metric_index_names,
        .residual_candidate_limit = documentProducerResidualCandidateLimit(bounded_scan_policy),
    };
}

fn boundedDocumentScanFromPolicy(policy: source_binding.BoundedScanPolicy) !BoundedDocumentScan {
    const max_rows = policy.max_rows orelse return error.DocumentSqlBoundedScanPolicyRequired;
    if (max_rows == 0) return error.DocumentSqlBoundedScanPolicyRequired;
    if (policy.max_bytes) |max_bytes| {
        if (max_bytes == 0) return error.DocumentSqlBoundedScanPolicyRequired;
    }
    return .{
        .max_rows = max_rows,
        .max_bytes = policy.max_bytes,
    };
}

fn documentReadViewMappingFromVirtualSchemaAlloc(
    alloc: std.mem.Allocator,
    virtual_schema: source_binding.DocumentSqlSchema,
    table_name: []const u8,
) !?DocumentReadViewMapping {
    const summary = source_binding.documentSqlViewMappingSummaryForView(virtual_schema, table_name) orelse return null;
    const name = try alloc.dupe(u8, summary.name);
    errdefer alloc.free(name);
    const source_table = try alloc.dupe(u8, summary.source_table);
    errdefer alloc.free(source_table);
    return .{
        .name = name,
        .source_table = source_table,
        .required_indexes = summary.required_indexes,
        .required_indexes_ready = summary.required_indexes_ready,
        .source_generation_fresh = summary.source_generation_fresh,
        .source_schema_fingerprint_fresh = summary.source_schema_fingerprint_fresh,
    };
}

fn lowerDocumentReadPlanInternalParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
    producer_capabilities: DocumentProducerCapabilities,
) !DocumentReadPlan {
    if (schema.storage_mode != .document) return error.InvalidSqlCatalog;
    const tokens = parsed_sql.items();
    if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;

    const statement_end = documentSqlStatementEnd(tokens);
    const from_index = findTopLevelKeyword(tokens, .from) orelse return error.UnsupportedSqlShape;
    try rejectDocumentSelectProjectionModifier(tokens[1..from_index]);

    const where_index = findTopLevelKeyword(tokens, .where);
    const order_index = findTopLevelKeyword(tokens, .order);
    const limit_index = findTopLevelKeyword(tokens, .limit);
    const offset_index = documentStatementTailKeywordIndex(tokens, from_index, .offset);
    const fetch_index = documentStatementTailKeywordIndex(tokens, from_index, .fetch);
    const for_index = documentStatementTailKeywordIndex(tokens, from_index, .@"for");
    const window_index = documentStatementTailKeywordIndex(tokens, from_index, .window);
    const tail_start = minOptionalIndex(&.{ where_index, order_index, limit_index, offset_index, fetch_index, for_index, window_index }) orelse statement_end;
    try rejectUnsupportedDocumentStatementShape(tokens, from_index, tail_start, false, false);

    if (from_index + 1 >= statement_end or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    errdefer alloc.free(table_name);
    const view_mapping = try documentReadViewMappingFromVirtualSchemaAlloc(alloc, virtual_schema, table_name);
    errdefer if (view_mapping) |mapping| {
        var mutable = mapping;
        mutable.deinit(alloc);
    };

    var from_binding = try parseDocumentFromTailAlloc(alloc, tokens[from_index + 1].text, tokens[from_index + 2 .. tail_start], schema, virtual_schema);
    errdefer from_binding.deinit(alloc);
    switch (try validatedDocumentReadStatementKind(parsed_sql)) {
        .query => {},
        .join, .lateral => if (from_binding.unnest == null) return error.UnsupportedSqlShape,
        else => return error.UnsupportedSqlShape,
    }
    const source_ref = from_binding.source_ref;

    const projection = try parseProjectionAlloc(alloc, tokens[1..from_index], schema, virtual_schema, source_ref, from_binding.unnest);
    errdefer freeProjection(alloc, projection);

    const limit = if (limit_index) |idx| try parseLimit(tokens, idx) else null;
    const order_by = if (order_index) |idx|
        try parseOrderByAlloc(alloc, tokens, idx, limit_index orelse statement_end, schema, virtual_schema, source_ref, from_binding.unnest)
    else
        null;
    errdefer if (order_by) |*order| {
        var mutable = order.*;
        mutable.deinit(alloc);
    };
    var producer = if (where_index) |idx| blk: {
        const end_index = order_index orelse limit_index orelse statement_end;
        break :blk parseWhereProducerAlloc(alloc, tokens, idx, end_index, schema, virtual_schema, source_ref, producer_capabilities, if (from_binding.unnest) |*unnest| unnest else null) catch |err| switch (err) {
            error.DocumentSqlIndexUnavailable => if (whereRangeHasExactProducerPredicate(tokens, idx, end_index))
                return error.DocumentSqlBoundedScanMissingExactProducer
            else
                try parseWhereBoundedScanProducerAlloc(
                    alloc,
                    tokens,
                    idx,
                    end_index,
                    schema,
                    virtual_schema,
                    source_ref,
                    bounded_scan_policy,
                    limit != null or from_binding.unnest != null,
                    if (from_binding.unnest) |*unnest| unnest else null,
                ),
            else => return err,
        };
    } else blk: {
        if (order_by != null or from_binding.unnest != null) {
            const policy = bounded_scan_policy orelse return error.DocumentSqlBoundedScanPolicyRequired;
            break :blk DocumentProducer{ .bounded_scan = try boundedDocumentScanFromPolicy(policy) };
        }
        const bounded = limit orelse return error.DocumentSqlBoundedScanUnboundedSource;
        break :blk DocumentProducer{ .bounded_scan = .{ .max_rows = bounded } };
    };
    errdefer {
        var mutable = producer;
        mutable.deinit(alloc);
    }
    if (view_mapping != null) switch (producer) {
        .bounded_scan => return error.DocumentSqlBoundedScanMissingExactProducer,
        else => {},
    };
    if (limit == null and bounded_scan_policy != null) switch (producer) {
        .indexed_query, .bounded_scan => return error.DocumentSqlBoundedScanIncompleteTopK,
        .id_lookup => {},
    };
    if (order_by != null) switch (producer) {
        .indexed_query => |*query| {
            if (query.max_candidate_rows == null) {
                query.max_candidate_rows = documentProducerResidualCandidateLimit(bounded_scan_policy) orelse return error.DocumentSqlBoundedScanIncompleteTopK;
            }
        },
        else => {},
    };
    return .{
        .table_name = table_name,
        .view_mapping = view_mapping,
        .projection = projection,
        .producer = producer,
        .order_by = order_by,
        .unnest = if (from_binding.unnest) |*unnest| .{
            .field = unnest.takeField(),
            .alias = unnest.takeAlias(),
            .item_type = unnest.item_type,
            .filter_value_json = unnest.takeFilterValueJson(),
            .filter_values_json = unnest.takeFilterValuesJson(),
            .filter_range_json = unnest.takeFilterRangeJson(),
            .filter_not_value_json = unnest.takeFilterNotValueJson(),
            .filter_not_query_json = unnest.takeFilterNotQueryJson(),
            .filter_pattern_json = unnest.takeFilterPatternJson(),
            .filter_pattern_query_json = unnest.takeFilterPatternQueryJson(),
            .filter_pattern_case_insensitive = unnest.takeFilterPatternCaseInsensitive(),
            .filter_is_not_null = unnest.takeFilterIsNotNull(),
            .filter_match_none = unnest.takeFilterMatchNone(),
        } else null,
        .limit = limit,
    };
}

fn parseWhereBoundedScanProducerAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    where_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
    has_output_limit_or_aggregate_policy: bool,
    unnest: ?*DocumentUnnest,
) !DocumentProducer {
    if (!has_output_limit_or_aggregate_policy) return error.DocumentSqlBoundedScanIncompleteTopK;
    const policy = bounded_scan_policy orelse return error.DocumentSqlBoundedScanPolicyRequired;
    const residual_filter_json = parseWhereScalarFilterJsonAlloc(alloc, tokens, where_index, end_index, schema, virtual_schema, source_ref, false, unnest) catch |err| switch (err) {
        error.UnsupportedSqlShape => return error.DocumentSqlBoundedScanUnsupportedResidual,
        else => return err,
    };
    errdefer if (residual_filter_json) |filter| alloc.free(@constCast(filter));
    var scan = try boundedDocumentScanFromPolicy(policy);
    scan.residual_filter_json = residual_filter_json;
    return .{ .bounded_scan = scan };
}

pub fn lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
) !DocumentAlgebraicAggregatePlan {
    return try lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyParsedSqlAlloc(alloc, parsed_sql, schema, null);
}

pub fn lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !DocumentAlgebraicAggregatePlan {
    return try lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        .{},
        bounded_scan_policy,
        documentProducerCapabilitiesForRuntimeSchema(schema, bounded_scan_policy),
        true,
    );
}

fn lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyInternalParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
    producer_capabilities: DocumentProducerCapabilities,
    attach_unfiltered_scan: bool,
) !DocumentAlgebraicAggregatePlan {
    if (schema.storage_mode != .document) return error.InvalidSqlCatalog;
    if ((try validatedDocumentReadStatementKind(parsed_sql)) != .aggregate) return error.UnsupportedSqlShape;

    const tokens = parsed_sql.items();
    if (tokens.len == 0 or !tokens[0].matchesKeywordTag(.select)) return error.UnsupportedSqlShape;

    const statement_end = documentSqlStatementEnd(tokens);
    const from_index = findTopLevelKeyword(tokens, .from) orelse return error.UnsupportedSqlShape;
    const where_index = findTopLevelKeyword(tokens, .where);
    const group_index = findTopLevelKeyword(tokens, .group);
    const having_index = findTopLevelKeyword(tokens, .having);
    const order_index = findTopLevelKeyword(tokens, .order);
    const limit_index = findTopLevelKeyword(tokens, .limit);
    const offset_index = documentStatementTailKeywordIndex(tokens, from_index, .offset);
    const fetch_index = documentStatementTailKeywordIndex(tokens, from_index, .fetch);
    const for_index = documentStatementTailKeywordIndex(tokens, from_index, .@"for");
    const window_index = documentStatementTailKeywordIndex(tokens, from_index, .window);
    const tail_start = minOptionalIndex(&.{ where_index, group_index, having_index, order_index, limit_index, offset_index, fetch_index, for_index, window_index }) orelse statement_end;
    try rejectUnsupportedDocumentStatementShape(tokens, from_index, tail_start, true, true);

    if (from_index + 1 >= statement_end or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    try rejectDocumentSelectProjectionModifier(tokens[1..from_index]);
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    var table_name_transferred = false;
    errdefer if (!table_name_transferred) alloc.free(table_name);

    const source_ref = DocumentSourceRef{
        .table_name = tokens[from_index + 1].text,
        .alias = try parseFromTailAlias(tokens[from_index + 2 .. tail_start]),
    };

    var aggregate = try parseDocumentAggregateSpecAlloc(alloc, tokens[1..from_index], schema, virtual_schema, source_ref);
    var aggregate_transferred = false;
    errdefer if (!aggregate_transferred) aggregate.deinit(alloc);

    const limit = if (limit_index) |idx| try parseLimit(tokens, idx) else null;
    var group_by = if (group_index) |idx|
        try parseDocumentAggregateGroupByAlloc(alloc, tokens, idx, having_index orelse order_index orelse limit_index orelse statement_end, schema, virtual_schema, source_ref, bounded_scan_policy == null)
    else
        null;
    var group_by_transferred = false;
    errdefer if (!group_by_transferred) if (group_by) |*group| group.deinit(alloc);

    var candidate_producer: ?DocumentProducer = null;
    var candidate_producer_transferred = false;
    errdefer if (!candidate_producer_transferred) if (candidate_producer) |*producer| producer.deinit(alloc);
    const filter_query_json: ?[]const u8 = if (where_index) |idx| blk: {
        const end_index = group_index orelse having_index orelse order_index orelse limit_index orelse statement_end;
        var producer = parseWhereProducerAlloc(alloc, tokens, idx, end_index, schema, virtual_schema, source_ref, producer_capabilities, null) catch |err| switch (err) {
            error.DocumentSqlIndexUnavailable => if (whereRangeHasExactProducerPredicate(tokens, idx, end_index))
                return error.DocumentSqlBoundedScanMissingExactProducer
            else
                try parseWhereBoundedScanProducerAlloc(
                    alloc,
                    tokens,
                    idx,
                    end_index,
                    schema,
                    virtual_schema,
                    source_ref,
                    bounded_scan_policy,
                    true,
                    null,
                ),
            else => return err,
        };
        errdefer producer.deinit(alloc);
        break :blk switch (producer) {
            .id_lookup => {
                candidate_producer = producer;
                producer = .{ .bounded_scan = .{ .max_rows = 1 } };
                break :blk null;
            },
            .indexed_query => |*query| {
                const filter = if (query.filter_query_json) |filter| try alloc.dupe(u8, filter) else null;
                errdefer if (filter) |owned| alloc.free(@constCast(owned));
                candidate_producer = producer;
                producer = .{ .bounded_scan = .{ .max_rows = 1 } };
                break :blk filter;
            },
            .bounded_scan => {
                candidate_producer = producer;
                producer = .{ .bounded_scan = .{ .max_rows = 1 } };
                break :blk null;
            },
        };
    } else null;
    var filter_query_json_transferred = false;
    errdefer if (!filter_query_json_transferred) if (filter_query_json) |filter| alloc.free(@constCast(filter));

    const having: []DocumentAggregateHavingPredicate = if (having_index) |idx|
        try parseDocumentAggregateHavingPredicatesAlloc(alloc, tokens, idx, order_index orelse limit_index orelse statement_end, group_by, aggregate, schema, virtual_schema, source_ref)
    else
        &.{};
    var having_transferred = false;
    errdefer if (!having_transferred) deinitDocumentAggregateHavingPredicates(alloc, having);

    const order_by = if (order_index) |idx|
        try parseDocumentAggregateOrderBy(alloc, tokens, idx, limit_index orelse statement_end, group_by, aggregate, schema, virtual_schema, source_ref)
    else
        null;

    var plan = DocumentAlgebraicAggregatePlan{
        .table_name = table_name,
        .candidate_producer = candidate_producer,
        .filter_query_json = filter_query_json,
        .group_by = group_by,
        .aggregate = aggregate,
        .having = having,
        .order_by = order_by,
        .limit = limit,
    };
    table_name_transferred = true;
    candidate_producer_transferred = true;
    filter_query_json_transferred = true;
    group_by_transferred = true;
    aggregate_transferred = true;
    having_transferred = true;
    errdefer plan.deinit(alloc);
    if (attach_unfiltered_scan and where_index == null) {
        try attachDocumentAggregateUnfilteredBoundedScanFallback(&plan, bounded_scan_policy);
    }
    if (aggregate.op != .count and (attach_unfiltered_scan or where_index != null)) {
        try requireBoundedDocumentAggregateInputProducer(&plan, bounded_scan_policy);
    }
    return plan;
}

pub fn lowerDocumentAlgebraicAggregatePlanWithIndexesJsonParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
) !DocumentAlgebraicAggregatePlan {
    var plan = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, parsed_sql, schema);
    errdefer plan.deinit(alloc);
    var matched = try documentAlgebraicAggregateMaterializationForPlanAlloc(alloc, indexes_json, plan);
    errdefer matched.deinit(alloc);
    plan.index_name = matched.takeIndexName();
    plan.materialization_name = matched.takeMaterializationName();
    return plan;
}

pub fn lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    indexes_json: ?[]const u8,
) !DocumentAlgebraicAggregatePlan {
    return try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        indexes_json,
        null,
    );
}

pub fn lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    indexes_json: ?[]const u8,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !DocumentAlgebraicAggregatePlan {
    var plan = try lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyInternalParsedSqlAlloc(alloc, parsed_sql, schema, .{}, bounded_scan_policy, .{}, false);
    errdefer plan.deinit(alloc);
    return try finishDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyAlloc(alloc, &plan, indexes_json, bounded_scan_policy);
}

pub fn lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    indexes_json: ?[]const u8,
    capabilities: source_binding.DocumentSqlCapabilities,
) !DocumentAlgebraicAggregatePlan {
    var plan = try lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        virtual_schema,
        capabilities.bounded_scan,
        .{
            .indexed_scalar_filters = capabilities.indexed_scalar_filters,
            .indexed_scalar_filter_paths = capabilities.indexed_scalar_filter_paths,
            .indexed_array_element_paths = capabilities.indexed_array_element_paths,
            .runtime_schema_scalar_filters = capabilities.runtime_schema_scalar_filters,
            .full_text_filters = capabilities.full_text_filters,
            .full_text_indexes = capabilities.full_text_indexes,
            .residual_candidate_limit = documentProducerResidualCandidateLimit(capabilities.bounded_scan),
        },
        false,
    );
    errdefer plan.deinit(alloc);
    return try finishDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyAlloc(alloc, &plan, indexes_json, capabilities.bounded_scan);
}

pub fn lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(
    alloc: std.mem.Allocator,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    indexes_json: ?[]const u8,
    capabilities: source_binding.DocumentSqlCapabilities,
) !DocumentAlgebraicAggregatePlan {
    var plan = try lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyInternalParsedSqlAlloc(
        alloc,
        parsed_sql,
        schema,
        .{},
        capabilities.bounded_scan,
        .{
            .indexed_scalar_filters = capabilities.indexed_scalar_filters,
            .indexed_scalar_filter_paths = capabilities.indexed_scalar_filter_paths,
            .indexed_array_element_paths = capabilities.indexed_array_element_paths,
            .runtime_schema_scalar_filters = capabilities.runtime_schema_scalar_filters,
            .full_text_filters = capabilities.full_text_filters,
            .full_text_indexes = capabilities.full_text_indexes,
            .residual_candidate_limit = documentProducerResidualCandidateLimit(capabilities.bounded_scan),
        },
        false,
    );
    errdefer plan.deinit(alloc);
    return try finishDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyAlloc(alloc, &plan, indexes_json, capabilities.bounded_scan);
}

fn finishDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyAlloc(
    alloc: std.mem.Allocator,
    plan: *DocumentAlgebraicAggregatePlan,
    indexes_json: ?[]const u8,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !DocumentAlgebraicAggregatePlan {
    const json = indexes_json orelse {
        try attachDocumentAggregateUnfilteredBoundedScanFallback(plan, bounded_scan_policy);
        try requireDocumentAggregateInputProducerUnlessMaterialized(plan, bounded_scan_policy);
        return plan.*;
    };
    if (json.len == 0) {
        try attachDocumentAggregateUnfilteredBoundedScanFallback(plan, bounded_scan_policy);
        try requireDocumentAggregateInputProducerUnlessMaterialized(plan, bounded_scan_policy);
        return plan.*;
    }
    var matched = documentAlgebraicAggregateMaterializationForPlanAlloc(alloc, json, plan.*) catch |err| switch (err) {
        error.DocumentSqlIndexUnavailable => {
            try attachDocumentAggregateUnfilteredBoundedScanFallback(plan, bounded_scan_policy);
            try requireDocumentAggregateInputProducerUnlessMaterialized(plan, bounded_scan_policy);
            return plan.*;
        },
        else => return err,
    };
    errdefer matched.deinit(alloc);
    plan.index_name = matched.takeIndexName();
    plan.materialization_name = matched.takeMaterializationName();
    return plan.*;
}

fn requireDocumentAggregateInputProducerUnlessMaterialized(
    plan: *DocumentAlgebraicAggregatePlan,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !void {
    if (plan.aggregate.op == .count) return;
    if (plan.index_name != null or plan.materialization_name != null) return;
    try requireBoundedDocumentAggregateInputProducer(plan, bounded_scan_policy);
}

fn attachDocumentAggregateUnfilteredBoundedScanFallback(
    plan: *DocumentAlgebraicAggregatePlan,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !void {
    if (plan.candidate_producer != null) return;
    if (plan.index_name != null or plan.materialization_name != null) return;
    const policy = bounded_scan_policy orelse return;
    plan.candidate_producer = .{ .bounded_scan = boundedDocumentScanFromPolicy(policy) catch return };
}

fn requireBoundedDocumentAggregateInputProducer(
    plan: *DocumentAlgebraicAggregatePlan,
    bounded_scan_policy: ?source_binding.BoundedScanPolicy,
) !void {
    if (plan.candidate_producer) |*producer| switch (producer.*) {
        .id_lookup => return,
        .bounded_scan => return,
        .indexed_query => |*query| {
            if (query.max_candidate_rows == null) {
                query.max_candidate_rows = documentProducerResidualCandidateLimit(bounded_scan_policy) orelse return error.DocumentSqlBoundedScanMissingExactProducer;
            }
            return;
        },
    };
    return error.DocumentSqlBoundedScanMissingExactProducer;
}

const DocumentAlgebraicAggregateMaterialization = struct {
    index_name: []const u8,
    materialization_name: []const u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.index_name.len > 0) alloc.free(@constCast(self.index_name));
        if (self.materialization_name.len > 0) alloc.free(@constCast(self.materialization_name));
        self.* = undefined;
    }

    fn takeIndexName(self: *@This()) []const u8 {
        const value = self.index_name;
        self.index_name = "";
        return value;
    }

    fn takeMaterializationName(self: *@This()) []const u8 {
        const value = self.materialization_name;
        self.materialization_name = "";
        return value;
    }
};

fn documentAlgebraicAggregateMaterializationForPlanAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    plan: DocumentAlgebraicAggregatePlan,
) !DocumentAlgebraicAggregateMaterialization {
    if (plan.candidate_producer != null) return error.DocumentSqlIndexUnavailable;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSqlCatalog;
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        const index_value = entry.value_ptr.*;
        if (!documentIndexValueIsAlgebraic(index_value)) continue;
        const materialization = documentAlgebraicMaterializationNameForPlan(index_value, plan) orelse continue;
        return .{
            .index_name = try alloc.dupe(u8, entry.key_ptr.*),
            .materialization_name = try alloc.dupe(u8, materialization),
        };
    }
    return error.DocumentSqlIndexUnavailable;
}

fn documentIndexValueIsAlgebraic(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "algebraic");
}

fn documentAlgebraicMaterializationNameForPlan(value: std.json.Value, plan: DocumentAlgebraicAggregatePlan) ?[]const u8 {
    if (value != .object) return null;
    const materializations_value = value.object.get("materializations") orelse return null;
    if (materializations_value != .array) return null;
    for (materializations_value.array.items) |materialization| {
        if (!documentAlgebraicMaterializationMatchesPlan(materialization, plan)) continue;
        const name_value = materialization.object.get("name") orelse continue;
        if (name_value != .string) continue;
        return name_value.string;
    }
    return null;
}

fn documentAlgebraicMaterializationMatchesPlan(value: std.json.Value, plan: DocumentAlgebraicAggregatePlan) bool {
    if (value != .object) return false;
    const op_value = value.object.get("op") orelse return false;
    if (op_value != .string or !std.mem.eql(u8, op_value.string, @tagName(plan.aggregate.op))) return false;
    if (plan.aggregate.op == .count) {
        if (value.object.get("measure") != null) return false;
    } else {
        const input = plan.aggregate.input orelse return false;
        const measure = value.object.get("measure") orelse return false;
        if (measure != .string or !std.mem.eql(u8, measure.string, input.source_field)) return false;
    }
    if (value.object.get("join") != null or
        value.object.get("time") != null or
        value.object.get("bucket") != null)
    {
        return false;
    }
    if (value.object.get("axes")) |axes| return documentAlgebraicGroupFieldsMatchPlan(axes, plan);
    if (value.object.get("group_by")) |group_by| return documentAlgebraicGroupFieldsMatchPlan(group_by, plan);
    return plan.group_by == null;
}

fn documentAlgebraicGroupFieldsMatchPlan(value: std.json.Value, plan: DocumentAlgebraicAggregatePlan) bool {
    if (value != .array) return false;
    if (plan.group_by) |group| {
        if (value.array.items.len != 1) return false;
        const field_value = value.array.items[0];
        return field_value == .string and std.mem.eql(u8, field_value.string, group.source_field);
    }
    return value.array.items.len == 0;
}

fn parseProjectionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    unnest: ?DocumentUnnest,
) ![]DocumentProjection {
    if (tokens.len == 0) return error.UnsupportedSqlShape;
    if (tokens.len == 1 and tokens[0].kind == .star) return try selectAllProjectionAlloc(alloc, schema, virtual_schema);
    if (try projectionItemIsQualifiedStar(tokens, source_ref)) return try selectAllProjectionAlloc(alloc, schema, virtual_schema);

    var out = std.ArrayListUnmanaged(DocumentProjection).empty;
    errdefer {
        for (out.items) |*projection| projection.deinit(alloc);
        out.deinit(alloc);
    }

    var start: usize = 0;
    while (start < tokens.len) {
        const comma = findComma(tokens, start) orelse tokens.len;
        if (comma == start) return error.UnsupportedSqlShape;
        const item = tokens[start..comma];
        if (try projectionItemIsStar(item, source_ref)) {
            try appendSelectAllProjectionAlloc(alloc, &out, schema, virtual_schema);
        } else {
            try out.append(alloc, try parseProjectionItemAlloc(alloc, item, schema, virtual_schema, source_ref, unnest));
        }
        start = comma + 1;
    }
    return try out.toOwnedSlice(alloc);
}

fn rejectDocumentSelectProjectionModifier(tokens: []const Token) !void {
    if (tokens.len == 0) return error.UnsupportedSqlShape;
    if (tokens[0].matchesKeywordTag(.distinct) or tokens[0].matchesKeywordTag(.all)) return error.DocumentSqlProjectionModifierUnsupported;
}

fn selectAllProjectionAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
) ![]DocumentProjection {
    const virtual_field_count = if (virtual_schema.fields.len > 0) virtual_schema.fields.len else schema.relational_columns.len;
    const metadata_columns: usize = (if (virtual_schema.exposes_doc_id) @as(usize, 1) else 0) +
        (if (virtual_schema.exposes_doc) @as(usize, 1) else 0);
    var out = try alloc.alloc(DocumentProjection, virtual_field_count + metadata_columns);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*projection| projection.deinit(alloc);
        alloc.free(out);
    }
    if (virtual_schema.exposes_doc_id) {
        out[initialized] = .{
            .kind = .id,
            .output = try alloc.dupe(u8, "_id"),
        };
        initialized += 1;
    }
    if (virtual_schema.exposes_doc) {
        out[initialized] = .{
            .kind = .doc,
            .output = try alloc.dupe(u8, "_doc"),
        };
        initialized += 1;
    }
    if (virtual_schema.fields.len > 0) {
        for (virtual_schema.fields) |field| {
            out[initialized] = .{
                .kind = .field,
                .field = try alloc.dupe(u8, field.path),
                .output = try alloc.dupe(u8, field.name),
            };
            initialized += 1;
        }
    } else {
        for (schema.relational_columns) |column| {
            out[initialized] = .{
                .kind = .field,
                .field = try alloc.dupe(u8, column.name),
                .output = try alloc.dupe(u8, column.name),
            };
            initialized += 1;
        }
    }
    return out;
}

fn appendSelectAllProjectionAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(DocumentProjection),
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
) !void {
    const all = try selectAllProjectionAlloc(alloc, schema, virtual_schema);
    defer alloc.free(all);
    errdefer {
        for (all) |*projection| projection.deinit(alloc);
    }
    try out.ensureUnusedCapacity(alloc, all.len);
    for (all) |projection| {
        out.appendAssumeCapacity(projection);
    }
}

fn projectionItemIsStar(tokens: []const Token, source_ref: DocumentSourceRef) !bool {
    if (tokens.len == 1 and tokens[0].kind == .star) return true;
    return try projectionItemIsQualifiedStar(tokens, source_ref);
}

fn projectionItemIsQualifiedStar(tokens: []const Token, source_ref: DocumentSourceRef) !bool {
    if (tokens.len != 2 or tokens[0].kind != .identifier or tokens[1].kind != .star) return false;
    const qualifier_token = tokens[0].text;
    if (qualifier_token.len < 2 or qualifier_token[qualifier_token.len - 1] != '.') return error.UnsupportedSqlShape;
    const qualifier = qualifier_token[0 .. qualifier_token.len - 1];
    if (!source_ref.matchesQualifier(qualifier)) return error.InvalidSqlCatalog;
    return true;
}

const DocumentCastExpression = struct {
    expression: []const Token,
    target_type: runtime_schema.AntflyType,
};

fn parseDocumentCastExpression(tokens: []const Token) !?DocumentCastExpression {
    if (tokens.len < 6 or !tokens[0].matchesKeywordTag(.cast) or tokens[1].kind != .lparen or tokens[tokens.len - 1].kind != .rparen) return null;
    const as_index = findTopLevelKeywordInRange(tokens, 2, tokens.len - 1, .as) orelse return error.UnsupportedSqlShape;
    if (as_index == 2 or as_index + 2 != tokens.len - 1) return error.UnsupportedSqlShape;
    const target_type = documentCastTargetType(tokens[as_index + 1]) orelse return error.UnsupportedSqlShape;
    return .{
        .expression = tokens[2..as_index],
        .target_type = target_type,
    };
}

fn documentCastTargetType(token: Token) ?runtime_schema.AntflyType {
    if (token.kind != .identifier) return null;
    const text = token.text;
    if (std.ascii.eqlIgnoreCase(text, "numeric") or
        std.ascii.eqlIgnoreCase(text, "decimal") or
        std.ascii.eqlIgnoreCase(text, "int") or
        std.ascii.eqlIgnoreCase(text, "int2") or
        std.ascii.eqlIgnoreCase(text, "int4") or
        std.ascii.eqlIgnoreCase(text, "int8") or
        std.ascii.eqlIgnoreCase(text, "integer") or
        std.ascii.eqlIgnoreCase(text, "smallint") or
        std.ascii.eqlIgnoreCase(text, "bigint") or
        std.ascii.eqlIgnoreCase(text, "real") or
        std.ascii.eqlIgnoreCase(text, "float4") or
        std.ascii.eqlIgnoreCase(text, "float8"))
    {
        return .numeric;
    }
    if (std.ascii.eqlIgnoreCase(text, "bool") or std.ascii.eqlIgnoreCase(text, "boolean")) return .boolean;
    if (std.ascii.eqlIgnoreCase(text, "text") or
        std.ascii.eqlIgnoreCase(text, "varchar") or
        std.ascii.eqlIgnoreCase(text, "char") or
        std.ascii.eqlIgnoreCase(text, "character"))
    {
        return .text;
    }
    if (std.ascii.eqlIgnoreCase(text, "datetime") or
        std.ascii.eqlIgnoreCase(text, "timestamp") or
        std.ascii.eqlIgnoreCase(text, "timestamptz") or
        std.ascii.eqlIgnoreCase(text, "date"))
    {
        return .datetime;
    }
    return null;
}

fn documentFieldTypeProvesCast(field_type: runtime_schema.AntflyType, target_type: runtime_schema.AntflyType) bool {
    return switch (target_type) {
        .numeric => field_type == .numeric,
        .boolean => field_type == .boolean,
        .datetime => field_type == .datetime,
        .text => field_type == .text or field_type == .keyword or field_type == .search_as_you_type,
        else => false,
    };
}

fn parseProjectionItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    unnest: ?DocumentUnnest,
) !DocumentProjection {
    if (tokens.len == 0) return error.UnsupportedSqlShape;
    const aliased = try splitProjectionAlias(tokens);
    const expression = aliased.expression;
    if (expression.len == 0 or expression[0].kind != .identifier) return error.UnsupportedSqlShape;

    if (try parseDocumentCastProjectionItemAlloc(alloc, expression, aliased.output, schema, virtual_schema, source_ref)) |projection| return projection;
    if (try parseJsonPathProjectionItemAlloc(alloc, expression, aliased.output, schema, virtual_schema, source_ref)) |projection| return projection;

    var output: ?[]const u8 = null;
    if (expression.len != 1) return error.UnsupportedSqlShape;
    output = aliased.output;

    const field = try documentIdentifierName(expression[0], source_ref);
    if (unnest) |binding| {
        if (std.ascii.eqlIgnoreCase(field, binding.alias)) {
            return .{ .kind = .unnest_value, .output = try alloc.dupe(u8, output orelse binding.alias) };
        }
    }
    if (std.mem.eql(u8, field, "_id")) {
        return .{ .kind = .id, .output = try alloc.dupe(u8, output orelse "_id") };
    }
    if (std.mem.eql(u8, field, "_doc")) {
        return .{ .kind = .doc, .output = try alloc.dupe(u8, output orelse "_doc") };
    }
    const virtual_field = documentVirtualField(schema, virtual_schema, field) orelse return error.InvalidSqlCatalog;
    return .{
        .kind = .field,
        .field = try alloc.dupe(u8, virtual_field.path),
        .output = try alloc.dupe(u8, output orelse field),
    };
}

fn parseDocumentCastProjectionItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    output: ?[]const u8,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !?DocumentProjection {
    if ((try parseDocumentCastExpression(tokens)) == null) return null;
    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref, false)) orelse return error.UnsupportedSqlShape;
    errdefer field.deinit(alloc);
    const owned_output = try alloc.dupe(u8, output orelse field.field_name orelse "cast");
    errdefer alloc.free(owned_output);
    return .{
        .kind = .field,
        .field = field.takePath(),
        .output = owned_output,
    };
}

fn documentVirtualField(
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    field: []const u8,
) ?source_binding.DocumentSqlVirtualField {
    if (virtual_schema.fields.len > 0) {
        for (virtual_schema.fields) |candidate| {
            if (std.ascii.eqlIgnoreCase(candidate.name, field)) return candidate;
        }
        return null;
    }
    const column = documentFieldColumn(schema, field) orelse return null;
    return .{
        .name = column.name,
        .path = column.name,
        .source = .declared_schema,
        .field_type = column.field_type,
        .array_item_type = column.array_item_type,
    };
}

const ProjectionAliasSplit = struct {
    expression: []const Token,
    output: ?[]const u8 = null,
};

fn splitProjectionAlias(tokens: []const Token) !ProjectionAliasSplit {
    if (tokens.len >= 3 and tokens[tokens.len - 2].matchesKeywordTag(.as) and tokens[tokens.len - 1].kind == .identifier) {
        return .{ .expression = tokens[0 .. tokens.len - 2], .output = tokens[tokens.len - 1].text };
    }
    if (tokens.len == 2 and tokens[0].kind == .identifier and tokens[1].kind == .identifier) {
        return .{ .expression = tokens[0..1], .output = tokens[1].text };
    }
    if (tokens.len >= 4 and tokens[tokens.len - 1].kind == .identifier and !documentJsonArrowKind(tokens[tokens.len - 2].kind)) {
        return .{ .expression = tokens[0 .. tokens.len - 1], .output = tokens[tokens.len - 1].text };
    }
    return .{ .expression = tokens };
}

fn parseJsonPathProjectionItemAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    output: ?[]const u8,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !?DocumentProjection {
    var expression = (try parseDocumentJsonPathExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref)) orelse return null;
    errdefer expression.deinit(alloc);
    const owned_output = try alloc.dupe(u8, output orelse expression.last_segment);
    errdefer alloc.free(owned_output);
    return .{
        .kind = .field,
        .field = expression.takePath(),
        .output = owned_output,
    };
}

fn documentJsonArrowKind(kind: token_mod.TokenKind) bool {
    return kind == .arrow_json or kind == .arrow_text or kind == .path_arrow_json or kind == .path_arrow_text;
}

fn documentJsonPathArrowKind(kind: token_mod.TokenKind) bool {
    return kind == .path_arrow_json or kind == .path_arrow_text;
}

const DocumentJsonPathExpression = struct {
    root_column: runtime_schema.RelationalColumn,
    path: []u8,
    last_segment: []const u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }

    fn takePath(self: *@This()) []u8 {
        const path = self.path;
        self.path = "";
        return path;
    }
};

fn parseDocumentJsonPathExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !?DocumentJsonPathExpression {
    if (try parseDocumentJsonPathFunctionExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref)) |expression| return expression;
    if (tokens.len < 3 or tokens[0].kind != .identifier or !documentJsonArrowKind(tokens[1].kind)) return null;
    const root = try documentIdentifierName(tokens[0], source_ref);
    if (std.mem.eql(u8, root, "_id")) return error.UnsupportedSqlShape;
    const column: runtime_schema.RelationalColumn = if (std.mem.eql(u8, root, "_doc"))
        .{
            .name = "_doc",
            .path = "",
            .field_type = .json,
        }
    else
        documentJsonPathRootColumn(schema, virtual_schema, root) orelse return error.InvalidSqlCatalog;
    var path = if (std.mem.eql(u8, root, "_doc"))
        try alloc.dupe(u8, "")
    else
        try documentFilterPathAlloc(alloc, column.path);
    errdefer alloc.free(path);

    var last_segment: []const u8 = root;
    var pos: usize = 1;
    while (pos < tokens.len) {
        if (pos + 1 >= tokens.len or !documentJsonArrowKind(tokens[pos].kind)) return error.UnsupportedSqlShape;
        const segment_token = tokens[pos + 1];
        if (segment_token.kind != .string and segment_token.kind != .identifier) return error.UnsupportedSqlShape;
        if (documentJsonPathArrowKind(tokens[pos].kind)) {
            last_segment = try appendDocumentJsonPathLiteralAlloc(alloc, &path, segment_token.text);
        } else {
            try appendDocumentJsonPathSegmentAlloc(alloc, &path, segment_token.text);
            last_segment = segment_token.text;
        }
        pos += 2;
    }

    return .{
        .root_column = column,
        .path = path,
        .last_segment = last_segment,
    };
}

fn parseDocumentJsonPathFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !?DocumentJsonPathExpression {
    if (tokens.len < 6 or !documentJsonPathFunctionToken(tokens[0]) or tokens[1].kind != .lparen or tokens[tokens.len - 1].kind != .rparen) return null;
    if (tokens[2].kind != .identifier) return error.UnsupportedSqlShape;
    const root = try documentIdentifierName(tokens[2], source_ref);
    if (std.mem.eql(u8, root, "_id")) return error.UnsupportedSqlShape;
    const column: runtime_schema.RelationalColumn = if (std.mem.eql(u8, root, "_doc"))
        .{
            .name = "_doc",
            .path = "",
            .field_type = .json,
        }
    else
        documentJsonPathRootColumn(schema, virtual_schema, root) orelse return error.InvalidSqlCatalog;
    var path = if (std.mem.eql(u8, root, "_doc"))
        try alloc.dupe(u8, "")
    else
        try documentFilterPathAlloc(alloc, column.path);
    errdefer alloc.free(path);

    var pos: usize = 3;
    var count: usize = 0;
    var last_segment: []const u8 = root;
    while (pos + 1 < tokens.len - 1) {
        if (tokens[pos].kind != .comma) return error.UnsupportedSqlShape;
        const segment = tokens[pos + 1];
        if (segment.kind != .string) return error.UnsupportedSqlShape;
        try appendDocumentJsonPathSegmentAlloc(alloc, &path, segment.text);
        last_segment = segment.text;
        count += 1;
        pos += 2;
    }
    if (count == 0 or pos != tokens.len - 1) return error.UnsupportedSqlShape;

    return .{
        .root_column = column,
        .path = path,
        .last_segment = last_segment,
    };
}

fn documentJsonPathFunctionToken(token: Token) bool {
    return token.matchesKeywordTag(.json_extract_path) or
        token.matchesKeywordTag(.json_extract_path_text) or
        token.matchesKeywordTag(.jsonb_extract_path) or
        token.matchesKeywordTag(.jsonb_extract_path_text);
}

fn documentJsonPathRootColumn(
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    root: []const u8,
) ?runtime_schema.RelationalColumn {
    if (documentFieldColumn(schema, root)) |column| return column;
    const virtual_field = documentVirtualField(schema, virtual_schema, root) orelse return null;
    if (!documentVirtualFieldProvidesJsonPathRoot(virtual_field.source)) return null;
    if (!documentVirtualAggregatePathIsScalar(virtual_field.path)) return null;
    return .{
        .name = virtual_field.name,
        .path = virtual_field.path,
        .field_type = .json,
        .indexed = false,
        .index_lifecycle = .building,
    };
}

fn appendDocumentJsonPathLiteralAlloc(alloc: std.mem.Allocator, path: *[]u8, literal: []const u8) ![]const u8 {
    if (literal.len == 0) return error.UnsupportedSqlShape;
    const body = if (literal.len >= 2 and literal[0] == '{' and literal[literal.len - 1] == '}')
        literal[1 .. literal.len - 1]
    else
        literal;
    var parts = std.mem.splitScalar(u8, body, ',');
    var count: usize = 0;
    var last_segment: []const u8 = "";
    while (parts.next()) |part| {
        try appendDocumentJsonPathSegmentAlloc(alloc, path, part);
        last_segment = part;
        count += 1;
    }
    if (count == 0) return error.UnsupportedSqlShape;
    return last_segment;
}

fn appendDocumentJsonPathSegmentAlloc(alloc: std.mem.Allocator, path: *[]u8, segment: []const u8) !void {
    if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '/') != null) return error.UnsupportedSqlShape;
    const next = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ path.*, segment });
    alloc.free(path.*);
    path.* = next;
}

fn parseWhereProducerAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    where_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    producer_capabilities: DocumentProducerCapabilities,
    unnest: ?*DocumentUnnest,
) !DocumentProducer {
    const where_tokens = tokens[where_index + 1 .. end_index];
    if (where_tokens.len == 0) return error.UnsupportedSqlShape;

    var parsed = ParsedDocumentWhere{};
    errdefer parsed.deinit(alloc);
    var native_query: ?DocumentIndexQuery = null;
    errdefer if (native_query) |*query| query.deinit(alloc);
    var scalar_ranges = std.ArrayListUnmanaged(DocumentWhereClauseRange).empty;
    defer scalar_ranges.deinit(alloc);

    var start: usize = 0;
    while (start < where_tokens.len) {
        const end = findTopLevelAnd(where_tokens, start) orelse where_tokens.len;
        if (end == start) return error.UnsupportedSqlShape;
        const clause = where_tokens[start..end];
        if (try parseFullTextQueryAlloc(alloc, clause)) |query| {
            if (parsed.full_text_query != null) {
                alloc.free(query);
                return error.UnsupportedSqlShape;
            }
            parsed.full_text_query = query;
        } else if (try parseNativeAntflySearchIndexQueryAlloc(alloc, clause, source_ref, producer_capabilities)) |query| {
            if (native_query != null) {
                var mutable = query;
                mutable.deinit(alloc);
                return error.UnsupportedSqlShape;
            }
            native_query = query;
        } else if (!try parseDocumentIdClauseIntoAlloc(alloc, clause, source_ref, &parsed)) {
            if (unnest) |binding| {
                if (try parseDocumentUnnestFilterIntoAlloc(alloc, clause, binding)) {
                    start = end + 1;
                    continue;
                }
            }
            try scalar_ranges.append(alloc, .{ .start = start, .end = end });
        }
        start = end + 1;
    }

    if (unnest) |binding| {
        if (!parsed.id_lookup_seen and parsed.full_text_query == null and native_query == null and
            (binding.filter_value_json != null or binding.filter_values_json != null or binding.filter_range_json != null or binding.filter_not_query_json != null or binding.filter_pattern_query_json != null or binding.filter_is_not_null or binding.filter_match_none) and
            documentProducerArrayElementPathReady(producer_capabilities, binding.field))
        {
            const clause = try buildDocumentUnnestIndexedFilterClauseAlloc(alloc, binding.*);
            errdefer alloc.free(clause);
            try parsed.filter_clauses.append(alloc, clause);
        }
    }

    if (native_query) |*query| {
        if (parsed.id_lookup_seen or parsed.full_text_query != null) return error.UnsupportedSqlShape;
        for (scalar_ranges.items) |range| {
            const clause = (try parseScalarFilterClauseWithIndexRequirementAlloc(alloc, where_tokens[range.start..range.end], schema, virtual_schema, source_ref, false)) orelse return error.UnsupportedSqlShape;
            errdefer alloc.free(clause);
            try parsed.filter_clauses.append(alloc, clause);
        }
        const residual_filter_json = try buildConjunctiveFilterJsonAlloc(alloc, parsed.filter_clauses.items);
        errdefer if (residual_filter_json) |filter| alloc.free(@constCast(filter));
        if (residual_filter_json != null) {
            query.residual_filter_json = residual_filter_json;
            query.max_candidate_rows = producer_capabilities.residual_candidate_limit orelse return error.DocumentSqlBoundedScanPolicyRequired;
        }
        parsed.deinit(alloc);
        const out = query.*;
        native_query = null;
        return .{ .indexed_query = out };
    }

    if (parsed.id_lookup_seen and parsed.full_text_query != null) {
        return error.UnsupportedSqlShape;
    }
    const scalar_require_index = !parsed.id_lookup_seen and
        !producer_capabilities.indexed_scalar_filters and
        producer_capabilities.indexed_scalar_filter_paths.len == 0 and
        parsed.full_text_query == null;
    for (scalar_ranges.items) |range| {
        const clause = (try parseScalarFilterClauseWithIndexRequirementAlloc(alloc, where_tokens[range.start..range.end], schema, virtual_schema, source_ref, scalar_require_index)) orelse return error.UnsupportedSqlShape;
        errdefer alloc.free(clause);
        try parsed.filter_clauses.append(alloc, clause);
    }

    if (parsed.id_lookup_seen) {
        const ids = try parsed.ids.toOwnedSlice(alloc);
        parsed.ids = .empty;
        const residual_filter_json = try buildConjunctiveFilterJsonAlloc(alloc, parsed.filter_clauses.items);
        errdefer if (residual_filter_json) |filter| alloc.free(@constCast(filter));
        parsed.deinit(alloc);
        return .{ .id_lookup = .{ .ids = ids, .residual_filter_json = residual_filter_json } };
    }
    if (parsed.full_text_query != null or parsed.filter_clauses.items.len > 0) {
        if (parsed.full_text_query != null and !producer_capabilities.full_text_filters) return error.DocumentSqlIndexUnavailable;
        const full_text_query = parsed.full_text_query;
        const index_name = if (full_text_query) |query|
            try documentFullTextIndexNameForQueryAlloc(alloc, producer_capabilities, query)
        else
            null;
        errdefer if (index_name) |name| alloc.free(@constCast(name));
        const scalar_filter_capabilities = if (full_text_query != null)
            documentProducerCapabilitiesForFullTextScalarFilters(producer_capabilities, index_name)
        else
            producer_capabilities;
        var indexed_filter_clauses = std.ArrayListUnmanaged([]const u8).empty;
        defer indexed_filter_clauses.deinit(alloc);
        var residual_filter_clauses = std.ArrayListUnmanaged([]const u8).empty;
        defer residual_filter_clauses.deinit(alloc);
        try partitionScalarFilterClausesByIndexReadinessAlloc(
            alloc,
            scalar_filter_capabilities,
            parsed.filter_clauses.items,
            &indexed_filter_clauses,
            &residual_filter_clauses,
        );
        if (parsed.full_text_query == null and indexed_filter_clauses.items.len == 0) return error.DocumentSqlIndexUnavailable;
        const filter_query_json = try buildConjunctiveFilterJsonAlloc(alloc, indexed_filter_clauses.items);
        errdefer if (filter_query_json) |filter| alloc.free(@constCast(filter));
        const residual_filter_json = if (residual_filter_clauses.items.len > 0) residual: {
            if (producer_capabilities.residual_candidate_limit == null) return error.DocumentSqlBoundedScanPolicyRequired;
            break :residual try buildConjunctiveFilterJsonAlloc(alloc, residual_filter_clauses.items);
        } else null;
        errdefer if (residual_filter_json) |filter| alloc.free(@constCast(filter));
        parsed.full_text_query = null;
        parsed.deinit(alloc);
        return .{ .indexed_query = .{
            .index_name = index_name,
            .full_text_query = full_text_query,
            .filter_query_json = filter_query_json,
            .residual_filter_json = residual_filter_json,
            .max_candidate_rows = if (residual_filter_json != null) producer_capabilities.residual_candidate_limit else null,
        } };
    }

    if (unnest != null) return error.DocumentSqlIndexUnavailable;
    return error.UnsupportedSqlShape;
}

fn parseNativeAntflySearchIndexQueryAlloc(
    alloc: std.mem.Allocator,
    clause: []const Token,
    source_ref: DocumentSourceRef,
    producer_capabilities: DocumentProducerCapabilities,
) !?DocumentIndexQuery {
    if (!clauseHasNativeAntflySearchFunction(clause)) return null;
    var lowered = try query_function.lowerAntflyQueryFunctionExpressionRawBodyTokensAlloc(alloc, clause);
    defer lowered.deinit(alloc);
    if (!source_ref.matchesQualifier(lowered.table_name)) return error.UnsupportedSqlShape;
    switch (lowered.function) {
        .semantic_search => {
            if (!producer_capabilities.semantic_filters) return error.DocumentSqlIndexUnavailable;
            try validateNativeQueryIndexArrayNamesAlloc(alloc, lowered.body_json, producer_capabilities.semantic_index_names);
        },
        .vector_search => {
            if (!producer_capabilities.vector_filters) return error.DocumentSqlIndexUnavailable;
            try validateNativeQueryIndexArrayNamesAlloc(alloc, lowered.body_json, producer_capabilities.vector_index_names);
        },
        .hybrid_search => {
            if (!producer_capabilities.hybrid_filters) return error.DocumentSqlIndexUnavailable;
            try validateNativeHybridQueryIndexNamesAlloc(alloc, lowered.body_json, lowered.primary_text_index_name, producer_capabilities);
        },
        .graph_traverse, .graph_neighbors, .graph_shortest_path, .graph_k_shortest_paths, .graph_match => {
            if (!producer_capabilities.graph_filters) return error.DocumentSqlIndexUnavailable;
            try validateNativeQueryGraphSearchIndexNamesAlloc(alloc, lowered.body_json, producer_capabilities.graph_index_names);
        },
        .graph_metric => {
            if (!producer_capabilities.graph_metric_filters) return error.DocumentSqlIndexUnavailable;
            try validateNativeQueryObjectIndexFieldAlloc(alloc, lowered.body_json, "graph_metric", producer_capabilities.graph_metric_index_names);
        },
        .graph_metric_rerank => {
            if (!producer_capabilities.graph_metric_filters) return error.DocumentSqlIndexUnavailable;
            try validateNativeQueryObjectIndexFieldAlloc(alloc, lowered.body_json, "graph_metric_rerank", producer_capabilities.graph_metric_index_names);
        },
        else => return error.DocumentSqlNativeSearchRequiresTableFunction,
    }
    return .{
        .native_query_json = try alloc.dupe(u8, lowered.body_json),
    };
}

fn documentNativeIndexNameReady(index_names: []const []const u8, index_name: []const u8) bool {
    if (index_names.len == 0) return true;
    for (index_names) |candidate| {
        if (std.mem.eql(u8, candidate, index_name)) return true;
    }
    return false;
}

fn documentFullTextIndexNameReady(indexes: []const source_binding.DocumentSqlFullTextIndex, index_name: []const u8) bool {
    if (indexes.len == 0) return true;
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) return true;
    }
    return false;
}

fn documentNativeIndexNameReadyInAny(
    index_names_a: []const []const u8,
    index_names_b: []const []const u8,
    index_name: []const u8,
) bool {
    if (index_names_a.len == 0 and index_names_b.len == 0) return true;
    return documentNativeIndexNameReady(index_names_a, index_name) or
        documentNativeIndexNameReady(index_names_b, index_name);
}

fn validateNativeHybridQueryIndexNamesAlloc(
    alloc: std.mem.Allocator,
    body_json: []const u8,
    primary_text_index_name: ?[]const u8,
    producer_capabilities: DocumentProducerCapabilities,
) !void {
    if (primary_text_index_name) |index_name| {
        if (!documentFullTextIndexNameReady(producer_capabilities.full_text_indexes, index_name)) {
            return error.DocumentSqlIndexUnavailable;
        }
    } else if (producer_capabilities.full_text_indexes.len > 0 and std.mem.indexOf(u8, body_json, "\"full_text_search\"") != null) {
        return error.DocumentSqlIndexUnavailable;
    }

    if (producer_capabilities.semantic_index_names.len == 0 and
        producer_capabilities.vector_index_names.len == 0)
    {
        return;
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const indexes = parsed.value.object.get("indexes") orelse return;
    if (indexes != .array or indexes.array.items.len == 0) return error.DocumentSqlIndexUnavailable;
    for (indexes.array.items) |index_value| {
        if (index_value != .string) return error.UnsupportedSqlShape;
        if (!documentNativeIndexNameReadyInAny(
            producer_capabilities.semantic_index_names,
            producer_capabilities.vector_index_names,
            index_value.string,
        )) return error.DocumentSqlIndexUnavailable;
    }
}

fn validateNativeQueryIndexArrayNamesAlloc(
    alloc: std.mem.Allocator,
    body_json: []const u8,
    index_names: []const []const u8,
) !void {
    if (index_names.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const indexes = parsed.value.object.get("indexes") orelse return error.DocumentSqlIndexUnavailable;
    if (indexes != .array or indexes.array.items.len == 0) return error.DocumentSqlIndexUnavailable;
    for (indexes.array.items) |index_value| {
        if (index_value != .string) return error.UnsupportedSqlShape;
        if (!documentNativeIndexNameReady(index_names, index_value.string)) return error.DocumentSqlIndexUnavailable;
    }
}

fn validateNativeQueryGraphSearchIndexNamesAlloc(
    alloc: std.mem.Allocator,
    body_json: []const u8,
    index_names: []const []const u8,
) !void {
    if (index_names.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const graph_searches = parsed.value.object.get("graph_searches") orelse return error.DocumentSqlIndexUnavailable;
    if (graph_searches != .object or graph_searches.object.count() == 0) return error.DocumentSqlIndexUnavailable;
    var it = graph_searches.object.iterator();
    while (it.next()) |entry| {
        const query_value = entry.value_ptr.*;
        if (query_value != .object) return error.UnsupportedSqlShape;
        const index_value = query_value.object.get("index_name") orelse return error.DocumentSqlIndexUnavailable;
        if (index_value != .string) return error.UnsupportedSqlShape;
        if (!documentNativeIndexNameReady(index_names, index_value.string)) return error.DocumentSqlIndexUnavailable;
    }
}

fn validateNativeQueryObjectIndexFieldAlloc(
    alloc: std.mem.Allocator,
    body_json: []const u8,
    object_name: []const u8,
    index_names: []const []const u8,
) !void {
    if (index_names.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.UnsupportedSqlShape;
    const query_value = parsed.value.object.get(object_name) orelse return error.DocumentSqlIndexUnavailable;
    if (query_value != .object) return error.UnsupportedSqlShape;
    const index_value = query_value.object.get("index") orelse
        query_value.object.get("index_name") orelse
        return error.DocumentSqlIndexUnavailable;
    if (index_value != .string) return error.UnsupportedSqlShape;
    if (!documentNativeIndexNameReady(index_names, index_value.string)) return error.DocumentSqlIndexUnavailable;
}

fn parseWhereScalarFilterJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    where_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
    unnest: ?*DocumentUnnest,
) !?[]const u8 {
    const where_tokens = tokens[where_index + 1 .. end_index];
    if (where_tokens.len == 0) return error.UnsupportedSqlShape;

    var filter_clauses = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (filter_clauses.items) |clause| alloc.free(@constCast(clause));
        filter_clauses.deinit(alloc);
    }

    var start: usize = 0;
    while (start < where_tokens.len) {
        const end = findTopLevelAnd(where_tokens, start) orelse where_tokens.len;
        if (end == start) return error.UnsupportedSqlShape;
        if (unnest) |binding| {
            if (try parseDocumentUnnestFilterIntoAlloc(alloc, where_tokens[start..end], binding)) {
                start = end + 1;
                continue;
            }
        }
        const clause = (try parseScalarFilterClauseWithIndexRequirementAlloc(alloc, where_tokens[start..end], schema, virtual_schema, source_ref, require_index)) orelse return error.UnsupportedSqlShape;
        errdefer alloc.free(clause);
        try filter_clauses.append(alloc, clause);
        start = end + 1;
    }

    return try buildConjunctiveFilterJsonAlloc(alloc, filter_clauses.items);
}

fn partitionScalarFilterClausesByIndexReadinessAlloc(
    alloc: std.mem.Allocator,
    capabilities: DocumentProducerCapabilities,
    clauses: []const []const u8,
    indexed: *std.ArrayListUnmanaged([]const u8),
    residual: *std.ArrayListUnmanaged([]const u8),
) !void {
    if (capabilities.indexed_scalar_filters) {
        try indexed.ensureUnusedCapacity(alloc, clauses.len);
        for (clauses) |clause| indexed.appendAssumeCapacity(clause);
        return;
    }
    if (capabilities.indexed_scalar_filter_paths.len == 0 and
        capabilities.indexed_array_element_paths.len == 0 and
        capabilities.runtime_schema_scalar_filters == null)
    {
        try residual.ensureUnusedCapacity(alloc, clauses.len);
        for (clauses) |clause| residual.appendAssumeCapacity(clause);
        return;
    }
    for (clauses) |clause| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, clause, .{}) catch return error.UnsupportedSqlShape;
        defer parsed.deinit();
        if (documentScalarFilterValueIndexReady(capabilities, parsed.value)) {
            try indexed.append(alloc, clause);
        } else {
            try residual.append(alloc, clause);
        }
    }
}

fn documentProducerCapabilitiesForFullTextScalarFilters(
    capabilities: DocumentProducerCapabilities,
    index_name: ?[]const u8,
) DocumentProducerCapabilities {
    var out = capabilities;
    out.indexed_scalar_filters = false;
    out.indexed_scalar_filter_paths = &.{};
    out.runtime_schema_scalar_filters = null;
    const name = index_name orelse return out;
    for (capabilities.full_text_indexes) |index| {
        if (std.mem.eql(u8, index.name, name)) {
            out.indexed_scalar_filter_paths = index.paths;
            return out;
        }
    }
    return out;
}

fn documentScalarFilterValueIndexReady(capabilities: DocumentProducerCapabilities, value: std.json.Value) bool {
    return switch (value) {
        .object => |object| documentScalarFilterObjectIndexReady(capabilities, object),
        .array => |array| blk: {
            for (array.items) |item| {
                if (!documentScalarFilterValueIndexReady(capabilities, item)) break :blk false;
            }
            break :blk true;
        },
        else => false,
    };
}

fn documentScalarFilterObjectIndexReady(capabilities: DocumentProducerCapabilities, object: std.json.ObjectMap) bool {
    if (object.get("match_none") != null) return true;
    if (object.get("match_all") != null) return false;
    if (object.get("bool")) |bool_value| return documentScalarFilterBoolIndexReady(capabilities, bool_value);
    if (object.get("conjuncts")) |items| return documentScalarFilterValueIndexReady(capabilities, items);
    if (object.get("disjuncts")) |items| return documentScalarFilterValueIndexReady(capabilities, items);

    if (documentScalarFilterOperatorPath(object, "term")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "terms")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "prefix")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "wildcard")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "array_any")) |path| return documentProducerArrayElementPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "numeric_range")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "date_range")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "term_range")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "exists")) |path| return documentProducerScalarPathReady(capabilities, path);
    return false;
}

fn documentScalarFilterBoolIndexReady(capabilities: DocumentProducerCapabilities, value: std.json.Value) bool {
    if (value != .object) return false;
    const object = value.object;
    if (object.get("filter")) |items| {
        if (!documentScalarFilterValueIndexReady(capabilities, items)) return false;
    }
    if (object.get("must")) |items| {
        if (!documentScalarFilterValueIndexReady(capabilities, items)) return false;
    }
    if (object.get("must_not")) |items| {
        if (!documentScalarFilterValueIndexReady(capabilities, items)) return false;
    }
    if (object.get("should")) |items| {
        if (!documentScalarFilterValueIndexReady(capabilities, items)) return false;
    }
    return true;
}

fn documentScalarFilterOperatorPath(object: std.json.ObjectMap, op: []const u8) ?[]const u8 {
    const value = object.get(op) orelse return null;
    switch (value) {
        .string => |path| return path,
        .object => |spec| {
            if (spec.get("path")) |path_value| {
                if (path_value == .string) return path_value.string;
            }
            if (spec.get("field")) |field_value| {
                if (field_value == .string) return field_value.string;
            }
            var it = spec.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "value") or
                    std.mem.eql(u8, entry.key_ptr.*, "values") or
                    std.mem.eql(u8, entry.key_ptr.*, "term") or
                    std.mem.eql(u8, entry.key_ptr.*, "min") or
                    std.mem.eql(u8, entry.key_ptr.*, "max") or
                    std.mem.eql(u8, entry.key_ptr.*, "start") or
                    std.mem.eql(u8, entry.key_ptr.*, "end") or
                    std.mem.eql(u8, entry.key_ptr.*, "prefix") or
                    std.mem.eql(u8, entry.key_ptr.*, "pattern"))
                {
                    continue;
                }
                return entry.key_ptr.*;
            }
            return null;
        },
        else => return null,
    }
}

fn documentProducerScalarPathReady(capabilities: DocumentProducerCapabilities, path: []const u8) bool {
    if (capabilities.indexed_scalar_filters) return true;
    const as_capabilities = source_binding.DocumentSqlCapabilities{
        .indexed_scalar_filter_paths = capabilities.indexed_scalar_filter_paths,
    };
    if (source_binding.documentScalarFilterPathReady(as_capabilities, path)) return true;
    if (documentProducerArrayElementPathReady(capabilities, path)) return true;
    if (capabilities.runtime_schema_scalar_filters) |schema| {
        return documentRuntimeSchemaScalarPathReady(schema, path);
    }
    return false;
}

fn documentProducerArrayElementPathReady(capabilities: DocumentProducerCapabilities, path: []const u8) bool {
    if (capabilities.indexed_array_element_paths.len == 0) return false;
    const as_capabilities = source_binding.DocumentSqlCapabilities{
        .indexed_scalar_filter_paths = capabilities.indexed_array_element_paths,
    };
    return source_binding.documentScalarFilterPathReady(as_capabilities, path);
}

fn documentRuntimeSchemaScalarPathReady(schema: runtime_schema.TableSchema, path: []const u8) bool {
    if (documentColumnForPath(schema, path)) |column| {
        return column.indexed and column.index_lifecycle == .ready;
    }
    for (schema.relational_columns) |column| {
        if (!column.indexed or column.index_lifecycle != .ready) continue;
        if (column.field_type != .json) continue;
        if (documentPathContainsPath(column.path, path)) return true;
    }
    return false;
}

fn documentFullTextIndexNameForQueryAlloc(
    alloc: std.mem.Allocator,
    capabilities: DocumentProducerCapabilities,
    query: []const u8,
) !?[]const u8 {
    if (capabilities.full_text_indexes.len == 0) return null;
    const query_field = documentFullTextQueryLeadingField(query);
    if (query_field) |field| {
        for (capabilities.full_text_indexes) |index| {
            if (index.paths.len == 0) continue;
            for (index.paths) |path| {
                if (documentPathEqual(path, field)) return try alloc.dupe(u8, index.name);
            }
        }
    }
    for (capabilities.full_text_indexes) |index| {
        if (index.paths.len == 0) return try alloc.dupe(u8, index.name);
    }
    if (query_field == null and capabilities.full_text_indexes.len == 1) {
        return try alloc.dupe(u8, capabilities.full_text_indexes[0].name);
    }
    return null;
}

fn documentFullTextQueryLeadingField(query: []const u8) ?[]const u8 {
    const colon = std.mem.indexOfScalar(u8, query, ':') orelse return null;
    if (colon == 0) return null;
    const field = std.mem.trim(u8, query[0..colon], " \t\r\n");
    if (field.len == 0) return null;
    for (field) |ch| {
        if (!(std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '-' or ch == '.' or ch == '/')) return null;
    }
    return field;
}

fn documentPathEqual(a: []const u8, b: []const u8) bool {
    var ai: usize = if (a.len > 0 and a[0] == '/') 1 else 0;
    var bi: usize = if (b.len > 0 and b[0] == '/') 1 else 0;
    while (ai < a.len and bi < b.len) : ({
        ai += 1;
        bi += 1;
    }) {
        const ac = if (a[ai] == '.') '/' else a[ai];
        const bc = if (b[bi] == '.') '/' else b[bi];
        if (ac != bc) return false;
    }
    return ai == a.len and bi == b.len;
}

fn parseOrderByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    order_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    unnest: ?DocumentUnnest,
) !DocumentOrderBy {
    if (order_index + 2 >= end_index) return error.UnsupportedSqlShape;
    if (!tokens[order_index + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
    const order_tokens = tokens[order_index + 2 .. end_index];
    if (order_tokens.len == 0) return error.UnsupportedSqlShape;
    if (findComma(order_tokens, 0) != null) return error.UnsupportedSqlShape;

    const direction: DocumentOrderDirection = if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.desc))
        .desc
    else if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.asc))
        .asc
    else
        .asc;
    const expression = if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.desc) or order_tokens[order_tokens.len - 1].matchesKeywordTag(.asc))
        order_tokens[0 .. order_tokens.len - 1]
    else
        order_tokens;
    if (expression.len == 0) return error.UnsupportedSqlShape;
    if (unnest) |binding| {
        if (expression.len == 1 and expression[0].kind == .identifier and std.ascii.eqlIgnoreCase(expression[0].text, binding.alias)) {
            return .{
                .field = try alloc.dupe(u8, binding.alias),
                .field_type = binding.item_type,
                .direction = direction,
            };
        }
    }
    if (expression.len == 1 and expression[0].kind == .identifier and std.mem.eql(u8, try documentIdentifierName(expression[0], source_ref), "_id")) {
        return .{
            .field = try alloc.dupe(u8, "_id"),
            .field_type = .keyword,
            .direction = direction,
        };
    }

    var field = (try documentOrderFieldForExpressionAlloc(alloc, expression, schema, virtual_schema, source_ref)) orelse return error.UnsupportedSqlShape;
    errdefer field.deinit(alloc);
    return .{
        .field = field.takePath(),
        .field_type = field.field_type,
        .direction = direction,
    };
}

fn parseDocumentAggregateSpecAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !DocumentAggregateSpec {
    const aliased = try splitProjectionAlias(tokens);
    const expression = aliased.expression;
    if (expression.len == 4 and
        expression[0].matchesKeywordTag(.count) and
        expression[1].kind == .lparen and
        expression[2].kind == .star and
        expression[3].kind == .rparen)
    {
        return .{
            .op = .count,
            .output = try alloc.dupe(u8, aliased.output orelse "count"),
        };
    }

    if (expression.len >= 4 and expression[1].kind == .lparen and expression[expression.len - 1].kind == .rparen) {
        const op: DocumentAggregateOp = if (expression[0].matchesKeywordTag(.sum))
            .sum
        else if (expression[0].matchesKeywordTag(.avg))
            .avg
        else if (expression[0].matchesKeywordTag(.min))
            .min
        else if (expression[0].matchesKeywordTag(.max))
            .max
        else
            return error.DocumentSqlAggregateUnsupported;
        var field = (try documentAggregateFieldForExpressionAlloc(alloc, expression[2 .. expression.len - 1], schema, virtual_schema, source_ref, false)) orelse return error.DocumentSqlAggregateUnsupported;
        errdefer field.deinit(alloc);
        if (field.field_type != .numeric) return error.DocumentSqlAggregateUnsupported;
        const source_field = field.field_name orelse return error.DocumentSqlAggregateUnsupported;
        const output = try alloc.dupe(u8, aliased.output orelse @tagName(op));
        errdefer alloc.free(output);
        const owned_source_field = try alloc.dupe(u8, source_field);
        errdefer alloc.free(owned_source_field);
        return .{
            .op = op,
            .output = output,
            .input = .{
                .field = field.takePath(),
                .source_field = owned_source_field,
                .field_type = field.field_type,
            },
        };
    }

    return error.DocumentSqlAggregateUnsupported;
}

fn parseDocumentAggregateGroupByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    group_index: usize,
    end_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !DocumentAggregateGroupBy {
    if (group_index + 2 >= end_index) return error.UnsupportedSqlShape;
    if (!tokens[group_index + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
    const group_tokens = tokens[group_index + 2 .. end_index];
    if (group_tokens.len == 0) return error.UnsupportedSqlShape;
    if (findComma(group_tokens, 0) != null) return error.DocumentSqlAggregateUnsupported;

    var field = (try documentAggregateFieldForExpressionAlloc(alloc, group_tokens, schema, virtual_schema, source_ref, require_index)) orelse return error.DocumentSqlAggregateUnsupported;
    errdefer field.deinit(alloc);
    const source_field = field.field_name orelse return error.DocumentSqlAggregateUnsupported;
    const output = try alloc.dupe(u8, try documentAggregateOutputName(group_tokens, source_ref));
    errdefer alloc.free(output);
    const owned_source_field = try alloc.dupe(u8, source_field);
    errdefer alloc.free(owned_source_field);
    return .{
        .field = field.takePath(),
        .source_field = owned_source_field,
        .field_type = field.field_type,
        .output = output,
    };
}

fn parseDocumentAggregateOrderBy(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    order_index: usize,
    end_index: usize,
    group_by: ?DocumentAggregateGroupBy,
    aggregate: DocumentAggregateSpec,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !DocumentAggregateOrderBy {
    if (group_by == null) return error.DocumentSqlAggregateUnsupported;
    if (order_index + 2 >= end_index) return error.UnsupportedSqlShape;
    if (!tokens[order_index + 1].matchesKeywordTag(.by)) return error.UnsupportedSqlShape;
    const order_tokens = tokens[order_index + 2 .. end_index];
    if (order_tokens.len == 0) return error.UnsupportedSqlShape;
    if (findComma(order_tokens, 0) != null) return error.DocumentSqlAggregateUnsupported;

    const direction: DocumentOrderDirection = if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.desc))
        .desc
    else if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.asc))
        .asc
    else
        .asc;
    const expression = if (order_tokens[order_tokens.len - 1].matchesKeywordTag(.desc) or order_tokens[order_tokens.len - 1].matchesKeywordTag(.asc))
        order_tokens[0 .. order_tokens.len - 1]
    else
        order_tokens;
    const group = group_by.?;
    if (expression.len == 1 and expression[0].kind == .identifier) {
        const expression_name = try documentIdentifierName(expression[0], source_ref);
        const expression_qualified = std.mem.indexOfScalar(u8, expression[0].text, '.') != null;
        if (!expression_qualified and std.ascii.eqlIgnoreCase(expression_name, aggregate.output)) {
            return .{
                .key = .aggregate,
                .field_type = .numeric,
                .direction = direction,
            };
        }
        if (std.ascii.eqlIgnoreCase(expression_name, group.output) or
            std.ascii.eqlIgnoreCase(expression_name, group.source_field))
        {
            return .{
                .key = .group,
                .field_type = group.field_type,
                .direction = direction,
            };
        }
    }

    if (try documentAggregateExpressionMatchesGroupAlloc(alloc, expression, group, schema, virtual_schema, source_ref)) {
        return .{
            .key = .group,
            .field_type = group.field_type,
            .direction = direction,
        };
    }

    var parsed_aggregate = parseDocumentAggregateSpecAlloc(alloc, expression, schema, virtual_schema, source_ref) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.DocumentSqlAggregateUnsupported => return error.DocumentSqlAggregateUnsupported,
        else => return err,
    };
    defer parsed_aggregate.deinit(alloc);
    if (!documentAggregateSpecMatches(parsed_aggregate, aggregate)) return error.DocumentSqlAggregateUnsupported;
    return .{
        .key = .aggregate,
        .field_type = aggregateHavingFieldType(aggregate),
        .direction = direction,
    };
}

fn documentAggregateExpressionMatchesGroupAlloc(
    alloc: std.mem.Allocator,
    expression: []const Token,
    group: DocumentAggregateGroupBy,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !bool {
    var field = (documentAggregateFieldForExpressionAlloc(alloc, expression, schema, virtual_schema, source_ref, false) catch |err| switch (err) {
        error.InvalidSqlCatalog => if (documentExpressionUsesMatchingQualifier(expression, source_ref)) return false else return err,
        else => return err,
    }) orelse return false;
    defer field.deinit(alloc);
    return field.field_type == group.field_type and
        std.mem.eql(u8, field.path, group.field);
}

fn documentExpressionUsesMatchingQualifier(expression: []const Token, source_ref: DocumentSourceRef) bool {
    if (expression.len != 1 or expression[0].kind != .identifier) return false;
    const dot = std.mem.indexOfScalar(u8, expression[0].text, '.') orelse return false;
    if (dot == 0 or dot + 1 >= expression[0].text.len) return false;
    if (std.mem.indexOfScalar(u8, expression[0].text[dot + 1 ..], '.') != null) return false;
    return source_ref.matchesQualifier(expression[0].text[0..dot]);
}

fn parseDocumentAggregateHavingPredicatesAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    having_index: usize,
    end_index: usize,
    group_by: ?DocumentAggregateGroupBy,
    aggregate: DocumentAggregateSpec,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) ![]DocumentAggregateHavingPredicate {
    if (group_by == null) return error.DocumentSqlAggregateUnsupported;
    if (having_index + 1 >= end_index) return error.UnsupportedSqlShape;
    const having_tokens = tokens[having_index + 1 .. end_index];
    if (having_tokens.len < 3) return error.UnsupportedSqlShape;
    var predicates = std.ArrayListUnmanaged(DocumentAggregateHavingPredicate).empty;
    errdefer {
        for (predicates.items) |*predicate| predicate.deinit(alloc);
        predicates.deinit(alloc);
    }

    var start: usize = 0;
    while (start < having_tokens.len) {
        const end = findTopLevelAnd(having_tokens, start) orelse having_tokens.len;
        if (end == start) return error.DocumentSqlAggregateUnsupported;
        var predicate = try parseDocumentAggregateHavingPredicateAlloc(alloc, having_tokens[start..end], group_by.?, aggregate, schema, virtual_schema, source_ref);
        var predicate_transferred = false;
        errdefer if (!predicate_transferred) predicate.deinit(alloc);
        try predicates.append(alloc, predicate);
        predicate_transferred = true;
        start = if (end < having_tokens.len) end + 1 else having_tokens.len;
    }

    if (predicates.items.len == 0) return error.DocumentSqlAggregateUnsupported;
    return try predicates.toOwnedSlice(alloc);
}

fn parseDocumentAggregateHavingPredicateAlloc(
    alloc: std.mem.Allocator,
    having_tokens: []const Token,
    group: DocumentAggregateGroupBy,
    aggregate: DocumentAggregateSpec,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !DocumentAggregateHavingPredicate {
    if (having_tokens.len < 3) return error.DocumentSqlAggregateUnsupported;
    const op_index = findTopLevelScalarFilterOperator(having_tokens) orelse return error.DocumentSqlAggregateUnsupported;
    if (op_index == 0 or op_index + 2 != having_tokens.len) return error.DocumentSqlAggregateUnsupported;
    const op = documentAggregateHavingOp(having_tokens[op_index].kind) orelse return error.DocumentSqlAggregateUnsupported;
    const value_json = try tokenLiteralJsonAlloc(alloc, having_tokens[op_index + 1]);
    errdefer alloc.free(value_json);

    const expression = having_tokens[0..op_index];
    if (expression.len == 1 and expression[0].kind == .identifier) {
        const expression_name = try documentIdentifierName(expression[0], source_ref);
        const expression_qualified = std.mem.indexOfScalar(u8, expression[0].text, '.') != null;
        if (!expression_qualified and std.ascii.eqlIgnoreCase(expression_name, aggregate.output)) {
            try validateDocumentAggregateHavingLiteral(aggregateHavingFieldType(aggregate), having_tokens[op_index + 1]);
            return .{
                .key = .aggregate,
                .field_type = aggregateHavingFieldType(aggregate),
                .op = op,
                .value_json = value_json,
            };
        }
        if (std.ascii.eqlIgnoreCase(expression_name, group.output) or
            std.ascii.eqlIgnoreCase(expression_name, group.source_field))
        {
            try validateDocumentAggregateHavingLiteral(group.field_type, having_tokens[op_index + 1]);
            return .{
                .key = .group,
                .field_type = group.field_type,
                .op = op,
                .value_json = value_json,
            };
        }
    }

    if (try documentAggregateExpressionMatchesGroupAlloc(alloc, expression, group, schema, virtual_schema, source_ref)) {
        try validateDocumentAggregateHavingLiteral(group.field_type, having_tokens[op_index + 1]);
        return .{
            .key = .group,
            .field_type = group.field_type,
            .op = op,
            .value_json = value_json,
        };
    }

    var parsed_aggregate = parseDocumentAggregateSpecAlloc(alloc, expression, schema, virtual_schema, source_ref) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.DocumentSqlAggregateUnsupported => return error.DocumentSqlAggregateUnsupported,
        else => return err,
    };
    defer parsed_aggregate.deinit(alloc);
    if (!documentAggregateSpecMatches(parsed_aggregate, aggregate)) return error.DocumentSqlAggregateUnsupported;
    try validateDocumentAggregateHavingLiteral(aggregateHavingFieldType(aggregate), having_tokens[op_index + 1]);
    return .{
        .key = .aggregate,
        .field_type = aggregateHavingFieldType(aggregate),
        .op = op,
        .value_json = value_json,
    };
}

fn documentAggregateHavingOp(kind: token_mod.TokenKind) ?DocumentAggregateHavingOp {
    return switch (kind) {
        .eq => .eq,
        .neq => .neq,
        .gt => .gt,
        .gte => .gte,
        .lt => .lt,
        .lte => .lte,
        else => null,
    };
}

fn aggregateHavingFieldType(aggregate: DocumentAggregateSpec) runtime_schema.AntflyType {
    return switch (aggregate.op) {
        .count => .numeric,
        .sum, .avg, .min, .max => if (aggregate.input) |input| input.field_type else .numeric,
    };
}

fn validateDocumentAggregateHavingLiteral(field_type: runtime_schema.AntflyType, token: Token) !void {
    if (tokenIsNullLiteral(token)) return error.DocumentSqlAggregateUnsupported;
    return switch (field_type) {
        .numeric => if (token.kind != .number) error.DocumentSqlAggregateUnsupported else {},
        .boolean => if (!token.matchesKeywordTag(.true) and !token.matchesKeywordTag(.false)) error.DocumentSqlAggregateUnsupported else {},
        .keyword, .text, .search_as_you_type, .datetime => if (token.kind != .string) error.DocumentSqlAggregateUnsupported else {},
        else => error.DocumentSqlAggregateUnsupported,
    };
}

fn documentAggregateSpecMatches(lhs: DocumentAggregateSpec, rhs: DocumentAggregateSpec) bool {
    if (lhs.op != rhs.op) return false;
    if (lhs.input == null or rhs.input == null) return lhs.input == null and rhs.input == null;
    return std.mem.eql(u8, lhs.input.?.source_field, rhs.input.?.source_field) and
        std.mem.eql(u8, lhs.input.?.field, rhs.input.?.field);
}

fn documentAggregateOutputName(tokens: []const Token, source_ref: DocumentSourceRef) ![]const u8 {
    if (tokens.len == 1 and tokens[0].kind == .identifier) return try documentIdentifierName(tokens[0], source_ref);
    if (tokens.len > 0 and tokens[tokens.len - 1].kind == .string) return tokens[tokens.len - 1].text;
    return "group";
}

fn parseWhereClauseIntoAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    out: *ParsedDocumentWhere,
) !void {
    if (try parseFullTextQueryAlloc(alloc, tokens)) |query| {
        if (out.full_text_query != null) {
            alloc.free(query);
            return error.UnsupportedSqlShape;
        }
        out.full_text_query = query;
        return;
    }
    if (try parseDocumentIdClauseIntoAlloc(alloc, tokens, source_ref, out)) return;
    if (try parseScalarFilterClauseAlloc(alloc, tokens, schema, virtual_schema, source_ref)) |clause| {
        errdefer alloc.free(clause);
        try out.filter_clauses.append(alloc, clause);
        return;
    }
    return error.UnsupportedSqlShape;
}

fn parseDocumentIdClauseIntoAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    source_ref: DocumentSourceRef,
    out: *ParsedDocumentWhere,
) !bool {
    if (tokens.len == 3 and tokens[0].kind == .identifier and std.mem.eql(u8, try documentIdentifierName(tokens[0], source_ref), "_id") and tokens[1].kind == .eq) {
        out.id_lookup_seen = true;
        if (tokenIsNullLiteral(tokens[2])) return true;
        const id = try documentIdLiteralAlloc(alloc, tokens[2]);
        errdefer alloc.free(id);
        try out.ids.append(alloc, id);
        return true;
    }
    if (tokens.len >= 5 and tokens[0].kind == .identifier and tokens[1].matchesKeywordTag(.in) and std.mem.eql(u8, try documentIdentifierName(tokens[0], source_ref), "_id")) {
        out.id_lookup_seen = true;
        try parseDocumentIdInListIntoAlloc(alloc, tokens[2..], out);
        return true;
    }
    return false;
}

fn parseDocumentUnnestFilterIntoAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    unnest: *DocumentUnnest,
) !bool {
    if (tokens.len == 0 or tokens[0].kind != .identifier) return false;
    if (!std.ascii.eqlIgnoreCase(tokens[0].text, unnest.alias)) return false;
    if (tokens.len >= 5 and tokens[1].matchesKeywordTag(.in)) {
        if (unnest.filter_pattern_json != null) return error.DocumentSqlUnnestUnsupported;
        const values_json = (try tokenLiteralListSqlInJsonAlloc(alloc, tokens[2..])) orelse try alloc.dupe(u8, "[]");
        errdefer alloc.free(values_json);
        if (unnest.filter_values_json) |existing| {
            if (!std.mem.eql(u8, existing, values_json)) return error.UnsupportedSqlShape;
            alloc.free(values_json);
            return true;
        }
        unnest.filter_values_json = values_json;
        return true;
    }
    if (tokens.len == 3 and tokens[1].matchesKeywordTag(.is) and tokens[2].matchesKeywordTag(.null)) {
        if (unnest.filter_pattern_json != null) return error.DocumentSqlUnnestUnsupported;
        if (unnest.filter_value_json) |existing| {
            if (!std.mem.eql(u8, existing, "null")) return error.DocumentSqlUnnestUnsupported;
            return true;
        }
        unnest.filter_value_json = try alloc.dupe(u8, "null");
        return true;
    }
    if (tokens.len == 4 and tokens[1].matchesKeywordTag(.is) and tokens[2].matchesKeywordTag(.not) and tokens[3].matchesKeywordTag(.null)) {
        if (unnest.filter_pattern_json != null) return error.DocumentSqlUnnestUnsupported;
        unnest.filter_is_not_null = true;
        return true;
    }
    if (tokens.len == 3 and tokens[1].kind == .neq) {
        if (unnest.filter_pattern_json != null) return error.DocumentSqlUnnestUnsupported;
        if (tokenIsNullLiteral(tokens[2])) {
            unnest.filter_match_none = true;
            return true;
        }
        const value_json = try tokenLiteralJsonAlloc(alloc, tokens[2]);
        errdefer alloc.free(value_json);
        if (unnest.filter_not_value_json) |existing| {
            if (!std.mem.eql(u8, existing, value_json)) return error.DocumentSqlUnnestUnsupported;
            alloc.free(value_json);
            return true;
        }
        const query_json = try buildDocumentUnnestNotEqualFilterClauseAlloc(alloc, unnest.*, tokens[2]);
        errdefer alloc.free(query_json);
        unnest.filter_not_value_json = value_json;
        unnest.filter_not_query_json = query_json;
        return true;
    }
    if (tokens.len == 3 and (tokens[1].matchesKeywordTag(.like) or tokens[1].matchesKeywordTag(.ilike))) {
        const case_insensitive = tokens[1].matchesKeywordTag(.ilike);
        if (documentUnnestHasValueFilter(unnest.*)) return error.DocumentSqlUnnestUnsupported;
        if (tokenIsNullLiteral(tokens[2])) {
            unnest.filter_match_none = true;
            return true;
        }
        if (tokens[2].kind != .string) return error.DocumentSqlUnnestUnsupported;
        if (case_insensitive and !documentSqlAsciiOnly(tokens[2].text)) return error.DocumentSqlUnnestUnsupported;
        var pattern = try documentLikePatternToNativeAlloc(alloc, tokens[2].text);
        defer pattern.deinit(alloc);
        if (case_insensitive) try validateDocumentSqlCaseVariantPattern(pattern.pattern);
        const pattern_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(pattern.pattern, .{})});
        errdefer alloc.free(pattern_json);
        if (unnest.filter_pattern_json) |existing| {
            if (!std.mem.eql(u8, existing, pattern_json)) return error.DocumentSqlUnnestUnsupported;
            if (unnest.filter_pattern_case_insensitive != case_insensitive) return error.DocumentSqlUnnestUnsupported;
            alloc.free(pattern_json);
            return true;
        }
        const query_json = try buildDocumentUnnestLikeFilterClauseAlloc(alloc, unnest.*, tokens[2], case_insensitive);
        errdefer alloc.free(query_json);
        unnest.filter_pattern_json = pattern_json;
        unnest.filter_pattern_query_json = query_json;
        unnest.filter_pattern_case_insensitive = case_insensitive;
        return true;
    }
    if (tokens.len == 3) {
        if (documentRangeBound(tokens[1].kind, tokens[2].text) != null) {
            if (unnest.filter_pattern_json != null) return error.DocumentSqlUnnestUnsupported;
            const range_json = try buildDocumentUnnestRangeFilterClauseAlloc(alloc, unnest.*, tokens[1], tokens[2]);
            errdefer alloc.free(range_json);
            if (unnest.filter_range_json) |existing| {
                if (!std.mem.eql(u8, existing, range_json)) return error.DocumentSqlUnnestUnsupported;
                alloc.free(range_json);
                return true;
            }
            unnest.filter_range_json = range_json;
            return true;
        }
    }
    if (tokens.len != 3 or tokens[1].kind != .eq) return error.DocumentSqlUnnestUnsupported;
    if (unnest.filter_pattern_json != null) return error.DocumentSqlUnnestUnsupported;
    const value_json = try tokenLiteralJsonAlloc(alloc, tokens[2]);
    errdefer alloc.free(value_json);
    if (unnest.filter_value_json) |existing| {
        if (!std.mem.eql(u8, existing, value_json)) return error.UnsupportedSqlShape;
        alloc.free(value_json);
        return true;
    }
    unnest.filter_value_json = value_json;
    return true;
}

fn documentUnnestHasValueFilter(unnest: DocumentUnnest) bool {
    return unnest.filter_value_json != null or
        unnest.filter_values_json != null or
        unnest.filter_range_json != null or
        unnest.filter_not_value_json != null or
        unnest.filter_not_query_json != null or
        unnest.filter_is_not_null or
        unnest.filter_match_none;
}

fn parseFullTextQueryAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?[]const u8 {
    if (tokens.len != 4) return null;
    if (!tokens[0].matchesKeywordTag(.full_text_search)) return null;
    if (tokens[1].kind != .lparen or tokens[2].kind != .string or tokens[3].kind != .rparen) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, tokens[2].text);
}

fn whereRangeHasExactProducerPredicate(tokens: []const Token, where_index: usize, end_index: usize) bool {
    if (where_index + 1 >= end_index) return false;
    for (tokens[where_index + 1 .. end_index]) |token| {
        if (token.matchesKeywordTag(.full_text_search)) return true;
        if (tokenMatchesAntflyQualifiedKeywordTagOnly(token, .semantic_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .vector_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .hybrid_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_traverse) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_neighbors) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_shortest_path) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_k_shortest_paths) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_match) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_metric) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_metric_rerank))
        {
            return true;
        }
    }
    return false;
}

fn clauseHasNativeAntflySearchFunction(tokens: []const Token) bool {
    for (tokens) |token| {
        if (tokenMatchesAntflyQualifiedKeywordTagOnly(token, .full_text_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .semantic_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .vector_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .hybrid_search) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_traverse) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_neighbors) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_shortest_path) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_k_shortest_paths) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_match) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_metric) or
            tokenMatchesAntflyQualifiedKeywordTagOnly(token, .graph_metric_rerank))
        {
            return true;
        }
    }
    return false;
}

fn tokenMatchesAntflyQualifiedKeywordTagOnly(token: Token, keyword: TokenKeyword) bool {
    if (token.kind != .identifier) return false;
    const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse return false;
    if (std.mem.indexOfScalar(u8, token.text[dot + 1 ..], '.') != null) return false;
    if (!std.ascii.eqlIgnoreCase(token.text[0..dot], "antfly")) return false;
    const member = token.text[dot + 1 ..];
    return if (token_mod.keywordFromIdentifier(member)) |member_keyword|
        member_keyword == keyword
    else
        false;
}

fn parseScalarFilterClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !?[]const u8 {
    return try parseScalarFilterClauseWithIndexRequirementAlloc(alloc, tokens, schema, virtual_schema, source_ref, true);
}

fn parseScalarFilterClauseWithIndexRequirementAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !?[]const u8 {
    if (tokens.len < 3) return null;
    const op_index = findTopLevelScalarFilterOperator(tokens) orelse return null;
    if (op_index == 0) return null;
    if (try parseTextCaseTermFilterClauseAlloc(alloc, tokens, op_index, schema, virtual_schema, source_ref, require_index)) |clause| {
        return clause;
    }
    if (try parseNumericAbsRangeFilterClauseAlloc(alloc, tokens, op_index, schema, virtual_schema, source_ref, require_index)) |clause| {
        return clause;
    }
    if (try parseDateUtcRangeFilterClauseAlloc(alloc, tokens, op_index, schema, virtual_schema, source_ref, require_index)) |clause| {
        return clause;
    }
    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens[0..op_index], schema, virtual_schema, source_ref, require_index)) orelse return null;
    defer field.deinit(alloc);
    try validateDocumentScalarPredicateField(field);
    if (tokens.len == op_index + 2 and (tokens[op_index].kind == .eq or tokens[op_index].kind == .neq) and tokenIsNullLiteral(tokens[op_index + 1])) {
        return try buildMatchNoneFilterClauseAlloc(alloc);
    }
    if (tokens.len == op_index + 2 and tokens[op_index].kind == .eq) {
        return try buildTermFilterClauseAlloc(alloc, field.path, tokens[op_index + 1]);
    }
    if (tokens.len == op_index + 2 and tokens[op_index].kind == .neq) {
        const term = try buildTermFilterClauseAlloc(alloc, field.path, tokens[op_index + 1]);
        defer alloc.free(term);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"bool\":{{\"must_not\":[{s}]}}}}",
            .{term},
        );
    }
    if (tokens[op_index].matchesKeywordTag(.is)) {
        if (tokens.len == op_index + 2 and tokens[op_index + 1].matchesKeywordTag(.null)) {
            return try buildIsNullFilterClauseAlloc(alloc, field.path);
        }
        if (tokens.len == op_index + 3 and tokens[op_index + 1].matchesKeywordTag(.not) and tokens[op_index + 2].matchesKeywordTag(.null)) {
            return try buildIsNotNullFilterClauseAlloc(alloc, field.path);
        }
        if (try parseBooleanIsFilterClauseAlloc(alloc, field, tokens[op_index + 1 ..])) |clause| {
            return clause;
        }
        return error.UnsupportedSqlShape;
    }
    if (tokens.len >= op_index + 4 and tokens[op_index].matchesKeywordTag(.in)) {
        const values_json = try tokenLiteralListSqlInJsonAlloc(alloc, tokens[op_index + 1 ..]) orelse return try buildMatchNoneFilterClauseAlloc(alloc);
        defer alloc.free(values_json);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"terms\":{{\"path\":{f},\"values\":{s}}}}}",
            .{ std.json.fmt(field.path, .{}), values_json },
        );
    }
    if (tokens.len == op_index + 2 and tokens[op_index].matchesKeywordTag(.like)) {
        if (tokenIsNullLiteral(tokens[op_index + 1])) return try buildMatchNoneFilterClauseAlloc(alloc);
        return try buildLikeFilterClauseAlloc(alloc, field, tokens[op_index + 1]);
    }
    if (tokens[op_index].matchesKeywordTag(.ilike)) return error.UnsupportedSqlShape;
    if (tokens.len == op_index + 4 and tokens[op_index].matchesKeywordTag(.between) and tokens[op_index + 2].matchesKeywordTag(.@"and")) {
        if (tokenIsNullLiteral(tokens[op_index + 1]) or tokenIsNullLiteral(tokens[op_index + 3])) return try buildMatchNoneFilterClauseAlloc(alloc);
        return try buildBetweenRangeFilterClauseAlloc(alloc, field, tokens[op_index + 1], tokens[op_index + 3]);
    }
    if (tokens.len == op_index + 2) {
        if (tokenIsNullLiteral(tokens[op_index + 1])) return try buildMatchNoneFilterClauseAlloc(alloc);
        return try buildRangeFilterClauseAlloc(alloc, field, tokens[op_index], tokens[op_index + 1]);
    }
    return null;
}

fn parseTextCaseTermFilterClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    op_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !?[]const u8 {
    if (tokens.len != op_index + 2 or tokens[op_index].kind != .eq) return null;
    if (op_index < 4 or tokens[0].kind != .identifier) return null;
    const operator = if (std.ascii.eqlIgnoreCase(tokens[0].text, "lower"))
        "text_lower_term"
    else if (std.ascii.eqlIgnoreCase(tokens[0].text, "upper"))
        "text_upper_term"
    else
        return null;
    if (tokens[1].kind != .lparen or tokens[op_index - 1].kind != .rparen) return null;
    _ = require_index;
    if (tokenIsNullLiteral(tokens[op_index + 1])) return try buildMatchNoneFilterClauseAlloc(alloc);
    if (tokens[op_index + 1].kind != .string) return error.UnsupportedSqlShape;
    if (!documentSqlAsciiOnly(tokens[op_index + 1].text)) return error.UnsupportedSqlShape;

    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens[2 .. op_index - 1], schema, virtual_schema, source_ref, false)) orelse return error.UnsupportedSqlShape;
    defer field.deinit(alloc);
    try validateDocumentScalarPredicateField(field);
    return switch (field.field_type) {
        .keyword, .text, .search_as_you_type => try buildTextCaseTermFilterClauseAlloc(alloc, operator, field.path, tokens[op_index + 1].text),
        else => error.UnsupportedSqlShape,
    };
}

fn buildTextCaseTermFilterClauseAlloc(alloc: std.mem.Allocator, operator: []const u8, path: []const u8, value: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"{s}\":{{\"path\":{f},\"value\":{f}}}}}",
        .{ operator, std.json.fmt(path, .{}), std.json.fmt(value, .{}) },
    );
}

fn documentSqlAsciiOnly(text: []const u8) bool {
    for (text) |ch| {
        if (ch >= 0x80) return false;
    }
    return true;
}

fn parseNumericAbsRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    op_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !?[]const u8 {
    if (tokens.len != op_index + 2) return null;
    if (op_index < 4 or tokens[0].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[0].text, "abs")) return null;
    if (tokens[1].kind != .lparen or tokens[op_index - 1].kind != .rparen) return null;
    if (require_index) return error.DocumentSqlIndexUnavailable;
    if (tokenIsNullLiteral(tokens[op_index + 1])) return try buildMatchNoneFilterClauseAlloc(alloc);
    if (tokens[op_index + 1].kind != .number) return error.UnsupportedSqlShape;
    try validateNonNegativeDocumentNumericLiteral(tokens[op_index + 1]);

    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens[2 .. op_index - 1], schema, virtual_schema, source_ref, false)) orelse return error.UnsupportedSqlShape;
    defer field.deinit(alloc);
    try validateDocumentScalarPredicateField(field);
    if (field.field_type != .numeric) return error.UnsupportedSqlShape;

    if (tokens[op_index].kind == .eq) {
        return try buildNumericAbsEqualFilterClauseAlloc(alloc, field.path, tokens[op_index + 1]);
    }
    if (tokens[op_index].kind == .neq) {
        const term = try buildNumericAbsEqualFilterClauseAlloc(alloc, field.path, tokens[op_index + 1]);
        defer alloc.free(term);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"bool\":{{\"must_not\":[{s}]}}}}",
            .{term},
        );
    }
    const bound = documentRangeBound(tokens[op_index].kind, tokens[op_index + 1].text) orelse return null;
    return try buildNumericAbsRangeFilterClauseAlloc(alloc, field.path, bound, tokens[op_index + 1]);
}

fn validateNonNegativeDocumentNumericLiteral(token: Token) !void {
    const value = std.fmt.parseFloat(f64, token.text) catch return error.UnsupportedSqlShape;
    if (!(value >= 0)) return error.UnsupportedSqlShape;
}

fn buildNumericAbsEqualFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8, value: Token) ![]const u8 {
    const bound = DocumentRangeBound{
        .min = value.text,
        .max = value.text,
        .inclusive_min = true,
        .inclusive_max = true,
    };
    return try buildNumericAbsRangeFilterClauseAlloc(alloc, path, bound, value);
}

fn buildNumericAbsRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .number) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"numeric_abs_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"min\":{s}", .{min});
    if (bound.max) |max| try writer.print(",\"max\":{s}", .{max});
    if (bound.min != null) try writer.print(",\"inclusive_min\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_max\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

fn parseDateUtcRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    op_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !?[]const u8 {
    if (tokens.len != op_index + 2) return null;
    if (op_index < 4 or tokens[0].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[0].text, "date_utc")) return null;
    if (tokens[1].kind != .lparen or tokens[op_index - 1].kind != .rparen) return null;

    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens[2 .. op_index - 1], schema, virtual_schema, source_ref, require_index)) orelse return error.UnsupportedSqlShape;
    defer field.deinit(alloc);
    try validateDocumentScalarPredicateField(field);
    if (field.field_type != .datetime) return error.UnsupportedSqlShape;

    if (tokenIsNullLiteral(tokens[op_index + 1])) return try buildMatchNoneFilterClauseAlloc(alloc);
    if (tokens[op_index + 1].kind != .string) return error.UnsupportedSqlShape;
    const day = try parseDocumentSqlUtcDateLiteral(tokens[op_index + 1].text);
    const start = try documentSqlUtcDateStartAlloc(alloc, day);
    defer alloc.free(start);
    const next_day = documentSqlDateNextDay(day);
    const end = try documentSqlUtcDateStartAlloc(alloc, next_day);
    defer alloc.free(end);

    if (tokens[op_index].kind == .eq) {
        return try buildDateRangeFilterClauseAlloc(alloc, field.path, .{
            .min = start,
            .max = end,
            .inclusive_min = true,
            .inclusive_max = false,
        }, .{ .kind = .string, .text = start });
    }
    if (tokens[op_index].kind == .neq) {
        const term = try buildDateRangeFilterClauseAlloc(alloc, field.path, .{
            .min = start,
            .max = end,
            .inclusive_min = true,
            .inclusive_max = false,
        }, .{ .kind = .string, .text = start });
        defer alloc.free(term);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"bool\":{{\"must_not\":[{s}]}}}}",
            .{term},
        );
    }
    const bound = switch (tokens[op_index].kind) {
        .gt => DocumentRangeBound{ .min = end, .inclusive_min = true },
        .gte => DocumentRangeBound{ .min = start, .inclusive_min = true },
        .lt => DocumentRangeBound{ .max = start, .inclusive_max = false },
        .lte => DocumentRangeBound{ .max = end, .inclusive_max = false },
        else => return null,
    };
    return try buildDateRangeFilterClauseAlloc(alloc, field.path, bound, .{ .kind = .string, .text = start });
}

const DocumentSqlUtcDate = struct {
    year: u16,
    month: u8,
    day: u8,
};

fn parseDocumentSqlUtcDateLiteral(text: []const u8) !DocumentSqlUtcDate {
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return error.UnsupportedSqlShape;
    const year = try parseFixedWidthUnsigned(u16, text[0..4]);
    const month = try parseFixedWidthUnsigned(u8, text[5..7]);
    const day = try parseFixedWidthUnsigned(u8, text[8..10]);
    if (month < 1 or month > 12) return error.UnsupportedSqlShape;
    if (day < 1 or day > daysInDocumentSqlMonth(year, month)) return error.UnsupportedSqlShape;
    return .{ .year = year, .month = month, .day = day };
}

fn parseFixedWidthUnsigned(comptime T: type, text: []const u8) !T {
    for (text) |ch| {
        if (ch < '0' or ch > '9') return error.UnsupportedSqlShape;
    }
    return std.fmt.parseUnsigned(T, text, 10) catch return error.UnsupportedSqlShape;
}

fn documentSqlUtcDateStartAlloc(alloc: std.mem.Allocator, day: DocumentSqlUtcDate) ![]const u8 {
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T00:00:00Z", .{ day.year, day.month, day.day });
}

fn documentSqlDateNextDay(day: DocumentSqlUtcDate) DocumentSqlUtcDate {
    const max_day = daysInDocumentSqlMonth(day.year, day.month);
    if (day.day < max_day) {
        return .{ .year = day.year, .month = day.month, .day = day.day + 1 };
    }
    if (day.month < 12) {
        return .{ .year = day.year, .month = day.month + 1, .day = 1 };
    }
    return .{ .year = day.year + 1, .month = 1, .day = 1 };
}

fn daysInDocumentSqlMonth(year: u16, month: u8) u8 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isDocumentSqlLeapYear(year)) 29 else 28,
        else => 0,
    };
}

fn isDocumentSqlLeapYear(year: u16) bool {
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0);
}

fn buildTermFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8, value: Token) ![]const u8 {
    const value_json = try tokenLiteralJsonAlloc(alloc, value);
    defer alloc.free(value_json);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"term\":{{\"path\":{f},\"value\":{s}}}}}",
        .{ std.json.fmt(path, .{}), value_json },
    );
}

fn buildDocumentUnnestIndexedFilterClauseAlloc(
    alloc: std.mem.Allocator,
    unnest: DocumentUnnest,
) ![]const u8 {
    if (unnest.filter_match_none) return try buildMatchNoneFilterClauseAlloc(alloc);
    if (unnest.filter_value_json) |value_json| {
        if (unnest.filter_not_value_json) |not_value_json| {
            if (std.mem.eql(u8, value_json, not_value_json)) return try buildMatchNoneFilterClauseAlloc(alloc);
        }
        return try std.fmt.allocPrint(
            alloc,
            "{{\"array_any\":{{\"path\":{f},\"value\":{s}}}}}",
            .{ std.json.fmt(unnest.field, .{}), value_json },
        );
    }
    if (unnest.filter_values_json) |values_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, values_json, .{});
        defer parsed.deinit();
        if (parsed.value != .array) return error.UnsupportedSqlShape;
        if (parsed.value.array.items.len == 0) return try alloc.dupe(u8, "{\"match_none\":{}}");

        var out: std.ArrayListUnmanaged(u8) = .empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"disjuncts\":[");
        var written: usize = 0;
        for (parsed.value.array.items, 0..) |value, i| {
            _ = i;
            const value_json = try std.json.Stringify.valueAlloc(alloc, value, .{});
            defer alloc.free(value_json);
            if (unnest.filter_not_value_json) |not_value_json| {
                if (std.mem.eql(u8, value_json, not_value_json)) continue;
            }
            if (written > 0) try out.append(alloc, ',');
            const clause = try std.fmt.allocPrint(
                alloc,
                "{{\"array_any\":{{\"path\":{f},\"value\":{s}}}}}",
                .{ std.json.fmt(unnest.field, .{}), value_json },
            );
            defer alloc.free(clause);
            try out.appendSlice(alloc, clause);
            written += 1;
        }
        if (written == 0) {
            out.deinit(alloc);
            return try buildMatchNoneFilterClauseAlloc(alloc);
        }
        try out.appendSlice(alloc, "]}");
        return try out.toOwnedSlice(alloc);
    }
    if (unnest.filter_range_json) |range_json| {
        if (unnest.filter_not_query_json != null) return error.DocumentSqlUnnestUnsupported;
        if (unnest.filter_pattern_query_json != null) return error.DocumentSqlUnnestUnsupported;
        return try alloc.dupe(u8, range_json);
    }
    if (unnest.filter_not_query_json) |query_json| return try alloc.dupe(u8, query_json);
    if (unnest.filter_pattern_query_json) |query_json| return try alloc.dupe(u8, query_json);
    if (unnest.filter_is_not_null) return try buildDocumentUnnestIsNotNullFilterClauseAlloc(alloc, unnest);
    return error.UnsupportedSqlShape;
}

fn buildDocumentUnnestLikeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    unnest: DocumentUnnest,
    value: Token,
    case_insensitive: bool,
) ![]const u8 {
    if (value.kind != .string) return error.DocumentSqlUnnestUnsupported;
    switch (unnest.item_type) {
        .keyword, .text, .search_as_you_type => {},
        else => return error.DocumentSqlUnnestUnsupported,
    }
    var pattern = try documentLikePatternToNativeAlloc(alloc, value.text);
    defer pattern.deinit(alloc);
    if (case_insensitive) {
        if (!documentSqlAsciiOnly(value.text)) return error.DocumentSqlUnnestUnsupported;
        try validateDocumentSqlCaseVariantPattern(pattern.pattern);
    }
    if (!pattern.has_wildcard) {
        if (case_insensitive) {
            return try buildDocumentUnnestCaseVariantFilterClauseAlloc(alloc, unnest.field, "array_any", "value", pattern.pattern);
        }
        return try std.fmt.allocPrint(
            alloc,
            "{{\"array_any\":{{\"path\":{f},\"value\":{f}}}}}",
            .{ std.json.fmt(unnest.field, .{}), std.json.fmt(pattern.pattern, .{}) },
        );
    }
    if (pattern.prefix) {
        if (case_insensitive) {
            return try buildDocumentUnnestCaseVariantFilterClauseAlloc(alloc, unnest.field, "prefix", "value", pattern.pattern[0 .. pattern.pattern.len - 1]);
        }
        return try std.fmt.allocPrint(
            alloc,
            "{{\"prefix\":{{\"path\":{f},\"value\":{f}}}}}",
            .{ std.json.fmt(unnest.field, .{}), std.json.fmt(pattern.pattern[0 .. pattern.pattern.len - 1], .{}) },
        );
    }
    if (case_insensitive) {
        return try buildDocumentUnnestCaseVariantFilterClauseAlloc(alloc, unnest.field, "wildcard", "pattern", pattern.pattern);
    }
    return try std.fmt.allocPrint(
        alloc,
        "{{\"wildcard\":{{\"path\":{f},\"pattern\":{f}}}}}",
        .{ std.json.fmt(unnest.field, .{}), std.json.fmt(pattern.pattern, .{}) },
    );
}

const document_sql_max_case_variant_bits = 6;

fn validateDocumentSqlCaseVariantPattern(pattern: []const u8) !void {
    var bits: usize = 0;
    for (pattern) |ch| {
        if (documentSqlAsciiAlphabetic(ch)) {
            bits += 1;
            if (bits > document_sql_max_case_variant_bits) return error.DocumentSqlUnnestUnsupported;
        } else if (ch >= 0x80) {
            return error.DocumentSqlUnnestUnsupported;
        }
    }
}

fn buildDocumentUnnestCaseVariantFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    operator: []const u8,
    value_name: []const u8,
    pattern: []const u8,
) ![]const u8 {
    const mutable = try alloc.dupe(u8, pattern);
    defer alloc.free(mutable);
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var written: usize = 0;
    try appendDocumentSqlCaseVariantFilterClausesAlloc(alloc, &out, &written, path, operator, value_name, mutable, 0);
    if (written == 0) return error.DocumentSqlUnnestUnsupported;
    if (written == 1) return try out.toOwnedSlice(alloc);

    const clauses = try out.toOwnedSlice(alloc);
    defer alloc.free(clauses);
    return try std.fmt.allocPrint(alloc, "{{\"disjuncts\":[{s}]}}", .{clauses});
}

fn appendDocumentSqlCaseVariantFilterClausesAlloc(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    written: *usize,
    path: []const u8,
    operator: []const u8,
    value_name: []const u8,
    pattern: []u8,
    index: usize,
) !void {
    if (index == pattern.len) {
        if (written.* > 0) try out.append(alloc, ',');
        const clause = if (std.mem.eql(u8, operator, "array_any"))
            try std.fmt.allocPrint(
                alloc,
                "{{\"array_any\":{{\"path\":{f},\"value\":{f}}}}}",
                .{ std.json.fmt(path, .{}), std.json.fmt(pattern, .{}) },
            )
        else
            try std.fmt.allocPrint(
                alloc,
                "{{\"{s}\":{{\"path\":{f},\"{s}\":{f}}}}}",
                .{ operator, std.json.fmt(path, .{}), value_name, std.json.fmt(pattern, .{}) },
            );
        defer alloc.free(clause);
        try out.appendSlice(alloc, clause);
        written.* += 1;
        return;
    }
    const ch = pattern[index];
    if (!documentSqlAsciiAlphabetic(ch)) {
        try appendDocumentSqlCaseVariantFilterClausesAlloc(alloc, out, written, path, operator, value_name, pattern, index + 1);
        return;
    }
    pattern[index] = documentSqlAsciiLower(ch);
    try appendDocumentSqlCaseVariantFilterClausesAlloc(alloc, out, written, path, operator, value_name, pattern, index + 1);
    pattern[index] = documentSqlAsciiUpper(ch);
    try appendDocumentSqlCaseVariantFilterClausesAlloc(alloc, out, written, path, operator, value_name, pattern, index + 1);
    pattern[index] = ch;
}

fn documentSqlAsciiAlphabetic(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z');
}

fn documentSqlAsciiLower(ch: u8) u8 {
    return if (ch >= 'A' and ch <= 'Z') ch + ('a' - 'A') else ch;
}

fn documentSqlAsciiUpper(ch: u8) u8 {
    return if (ch >= 'a' and ch <= 'z') ch - ('a' - 'A') else ch;
}

fn buildDocumentUnnestNotEqualFilterClauseAlloc(
    alloc: std.mem.Allocator,
    unnest: DocumentUnnest,
    value: Token,
) ![]const u8 {
    const lower = DocumentRangeBound{ .max = value.text, .inclusive_max = false };
    const upper = DocumentRangeBound{ .min = value.text, .inclusive_min = false };
    const lower_json = switch (unnest.item_type) {
        .numeric => try buildNumericRangeFilterClauseAlloc(alloc, unnest.field, lower, value),
        .datetime => try buildDateRangeFilterClauseAlloc(alloc, unnest.field, lower, value),
        .keyword, .text, .search_as_you_type => try buildTermRangeFilterClauseAlloc(alloc, unnest.field, lower, value),
        else => return error.DocumentSqlUnnestUnsupported,
    };
    defer alloc.free(lower_json);
    const upper_json = switch (unnest.item_type) {
        .numeric => try buildNumericRangeFilterClauseAlloc(alloc, unnest.field, upper, value),
        .datetime => try buildDateRangeFilterClauseAlloc(alloc, unnest.field, upper, value),
        .keyword, .text, .search_as_you_type => try buildTermRangeFilterClauseAlloc(alloc, unnest.field, upper, value),
        else => unreachable,
    };
    defer alloc.free(upper_json);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"disjuncts\":[{s},{s}]}}",
        .{ lower_json, upper_json },
    );
}

fn buildDocumentUnnestIsNotNullFilterClauseAlloc(
    alloc: std.mem.Allocator,
    unnest: DocumentUnnest,
) ![]const u8 {
    return switch (unnest.item_type) {
        .keyword, .text, .search_as_you_type => try std.fmt.allocPrint(
            alloc,
            "{{\"term_range\":{{\"path\":{f},\"min\":\"\",\"inclusive_min\":true}}}}",
            .{std.json.fmt(unnest.field, .{})},
        ),
        else => error.DocumentSqlUnnestUnsupported,
    };
}

fn buildDocumentUnnestRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    unnest: DocumentUnnest,
    operator: Token,
    value: Token,
) ![]const u8 {
    const bound = documentRangeBound(operator.kind, value.text) orelse return error.DocumentSqlUnnestUnsupported;
    return switch (unnest.item_type) {
        .numeric => try buildNumericRangeFilterClauseAlloc(alloc, unnest.field, bound, value),
        .datetime => try buildDateRangeFilterClauseAlloc(alloc, unnest.field, bound, value),
        .keyword, .text, .search_as_you_type => try buildTermRangeFilterClauseAlloc(alloc, unnest.field, bound, value),
        else => error.DocumentSqlUnnestUnsupported,
    };
}

fn buildIsNullFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8) ![]const u8 {
    const null_term = try buildNullTermFilterClauseAlloc(alloc, path);
    defer alloc.free(null_term);
    const exists = try buildExistsFilterClauseAlloc(alloc, path);
    defer alloc.free(exists);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"bool\":{{\"should\":[{s},{{\"bool\":{{\"must_not\":[{s}]}}}}],\"minimum_should_match\":1}}}}",
        .{ null_term, exists },
    );
}

fn buildIsNotNullFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8) ![]const u8 {
    const exists = try buildExistsFilterClauseAlloc(alloc, path);
    defer alloc.free(exists);
    const null_term = try buildNullTermFilterClauseAlloc(alloc, path);
    defer alloc.free(null_term);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"bool\":{{\"filter\":[{s}],\"must_not\":[{s}]}}}}",
        .{ exists, null_term },
    );
}

fn parseBooleanIsFilterClauseAlloc(
    alloc: std.mem.Allocator,
    field: DocumentFilterField,
    tokens: []const Token,
) !?[]const u8 {
    if (field.field_type != .boolean) return null;
    if (tokens.len == 1 and (tokens[0].matchesKeywordTag(.true) or tokens[0].matchesKeywordTag(.false))) {
        return try buildBooleanTermFilterClauseAlloc(alloc, field.path, tokens[0]);
    }
    if (tokens.len == 2 and tokens[0].matchesKeywordTag(.not) and
        (tokens[1].matchesKeywordTag(.true) or tokens[1].matchesKeywordTag(.false)))
    {
        const term = try buildBooleanTermFilterClauseAlloc(alloc, field.path, tokens[1]);
        defer alloc.free(term);
        return try std.fmt.allocPrint(
            alloc,
            "{{\"bool\":{{\"must_not\":[{s}]}}}}",
            .{term},
        );
    }
    return null;
}

fn buildBooleanTermFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8, value: Token) ![]const u8 {
    if (!value.matchesKeywordTag(.true) and !value.matchesKeywordTag(.false)) return error.UnsupportedSqlShape;
    return try buildTermFilterClauseAlloc(alloc, path, value);
}

fn buildExistsFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"exists\":{{\"path\":{f}}}}}",
        .{std.json.fmt(path, .{})},
    );
}

fn buildNullTermFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"term\":{{\"path\":{f},\"value\":null}}}}",
        .{std.json.fmt(path, .{})},
    );
}

fn buildMatchNoneFilterClauseAlloc(alloc: std.mem.Allocator) ![]const u8 {
    return try alloc.dupe(u8, "{\"match_none\":{}}");
}

fn tokenIsNullLiteral(token: Token) bool {
    return token.matchesKeywordTag(.null);
}

fn validateDocumentScalarPredicateField(field: DocumentFilterField) !void {
    return switch (field.field_type) {
        .array => error.DocumentSqlArrayRequiresUnnest,
        else => {},
    };
}

const DocumentRangeBound = struct {
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
    inclusive_min: bool = true,
    inclusive_max: bool = false,
};

fn buildRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    field: DocumentFilterField,
    operator: Token,
    value: Token,
) !?[]const u8 {
    if (!field.exact_declared_path) return error.DocumentSqlIndexUnavailable;
    const bound = documentRangeBound(operator.kind, value.text) orelse return null;
    return switch (field.field_type) {
        .numeric => try buildNumericRangeFilterClauseAlloc(alloc, field.path, bound, value),
        .datetime => try buildDateRangeFilterClauseAlloc(alloc, field.path, bound, value),
        .keyword, .text, .search_as_you_type => try buildTermRangeFilterClauseAlloc(alloc, field.path, bound, value),
        else => error.UnsupportedSqlShape,
    };
}

fn buildBetweenRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    field: DocumentFilterField,
    lower: Token,
    upper: Token,
) ![]const u8 {
    if (!field.exact_declared_path) return error.DocumentSqlIndexUnavailable;
    const bound = DocumentRangeBound{
        .min = lower.text,
        .max = upper.text,
        .inclusive_min = true,
        .inclusive_max = true,
    };
    return switch (field.field_type) {
        .numeric => blk: {
            if (lower.kind != .number or upper.kind != .number) return error.UnsupportedSqlShape;
            break :blk try buildNumericRangeFilterClauseAlloc(alloc, field.path, bound, lower);
        },
        .datetime => blk: {
            if (lower.kind != .string or upper.kind != .string) return error.UnsupportedSqlShape;
            break :blk try buildDateRangeFilterClauseAlloc(alloc, field.path, bound, lower);
        },
        .keyword, .text, .search_as_you_type => blk: {
            if ((lower.kind != .string and lower.kind != .identifier) or
                (upper.kind != .string and upper.kind != .identifier))
            {
                return error.UnsupportedSqlShape;
            }
            break :blk try buildTermRangeFilterClauseAlloc(alloc, field.path, bound, lower);
        },
        else => error.UnsupportedSqlShape,
    };
}

fn documentRangeBound(kind: token_mod.TokenKind, value: []const u8) ?DocumentRangeBound {
    return switch (kind) {
        .gt => .{ .min = value, .inclusive_min = false },
        .gte => .{ .min = value, .inclusive_min = true },
        .lt => .{ .max = value, .inclusive_max = false },
        .lte => .{ .max = value, .inclusive_max = true },
        else => null,
    };
}

const DocumentLikePattern = struct {
    pattern: []const u8,
    has_wildcard: bool,
    prefix: bool,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.pattern.len > 0) alloc.free(@constCast(self.pattern));
        self.* = undefined;
    }
};

fn buildLikeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    field: DocumentFilterField,
    value: Token,
) ![]const u8 {
    if (!field.exact_declared_path) return error.DocumentSqlIndexUnavailable;
    if (value.kind != .string) return error.UnsupportedSqlShape;
    switch (field.field_type) {
        .keyword, .text, .search_as_you_type => {},
        else => return error.UnsupportedSqlShape,
    }
    var pattern = try documentLikePatternToNativeAlloc(alloc, value.text);
    defer pattern.deinit(alloc);
    if (!pattern.has_wildcard) {
        return try std.fmt.allocPrint(
            alloc,
            "{{\"term\":{{\"path\":{f},\"value\":{f}}}}}",
            .{ std.json.fmt(field.path, .{}), std.json.fmt(pattern.pattern, .{}) },
        );
    }
    if (pattern.prefix) {
        return try std.fmt.allocPrint(
            alloc,
            "{{\"prefix\":{{\"path\":{f},\"value\":{f}}}}}",
            .{ std.json.fmt(field.path, .{}), std.json.fmt(pattern.pattern[0 .. pattern.pattern.len - 1], .{}) },
        );
    }
    return try std.fmt.allocPrint(
        alloc,
        "{{\"wildcard\":{{\"path\":{f},\"pattern\":{f}}}}}",
        .{ std.json.fmt(field.path, .{}), std.json.fmt(pattern.pattern, .{}) },
    );
}

fn documentLikePatternToNativeAlloc(alloc: std.mem.Allocator, sql_pattern: []const u8) !DocumentLikePattern {
    var out: std.ArrayListUnmanaged(u8) = .empty;
    errdefer out.deinit(alloc);
    var has_wildcard = false;
    var star_count: usize = 0;
    var question_count: usize = 0;
    var escaped = false;
    for (sql_pattern) |ch| {
        if (escaped) {
            if (ch == '*' or ch == '?') return error.UnsupportedSqlShape;
            try out.append(alloc, ch);
            escaped = false;
            continue;
        }
        switch (ch) {
            '\\' => escaped = true,
            '%' => {
                try out.append(alloc, '*');
                has_wildcard = true;
                star_count += 1;
            },
            '_' => {
                try out.append(alloc, '?');
                has_wildcard = true;
                question_count += 1;
            },
            '*', '?' => return error.UnsupportedSqlShape,
            else => try out.append(alloc, ch),
        }
    }
    if (escaped) try out.append(alloc, '\\');
    const pattern = try out.toOwnedSlice(alloc);
    return .{
        .pattern = pattern,
        .has_wildcard = has_wildcard,
        .prefix = pattern.len > 0 and pattern[pattern.len - 1] == '*' and star_count == 1 and question_count == 0,
    };
}

fn buildNumericRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .number) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"numeric_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"min\":{s}", .{min});
    if (bound.max) |max| try writer.print(",\"max\":{s}", .{max});
    if (bound.min != null) try writer.print(",\"inclusive_min\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_max\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

fn buildDateRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .string) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"date_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"start\":{f}", .{std.json.fmt(min, .{})});
    if (bound.max) |max| try writer.print(",\"end\":{f}", .{std.json.fmt(max, .{})});
    if (bound.min != null) try writer.print(",\"inclusive_start\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_end\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

fn buildTermRangeFilterClauseAlloc(
    alloc: std.mem.Allocator,
    path: []const u8,
    bound: DocumentRangeBound,
    value: Token,
) ![]const u8 {
    if (value.kind != .string and value.kind != .identifier) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.print("{{\"term_range\":{{\"path\":{f}", .{std.json.fmt(path, .{})});
    if (bound.min) |min| try writer.print(",\"min\":{f}", .{std.json.fmt(min, .{})});
    if (bound.max) |max| try writer.print(",\"max\":{f}", .{std.json.fmt(max, .{})});
    if (bound.min != null) try writer.print(",\"inclusive_min\":{}", .{bound.inclusive_min});
    if (bound.max != null) try writer.print(",\"inclusive_max\":{}", .{bound.inclusive_max});
    try writer.writeAll("}}");
    return try out.toOwnedSlice();
}

const DocumentFilterField = struct {
    path: []u8,
    field_name: ?[]const u8 = null,
    field_type: runtime_schema.AntflyType,
    exact_declared_path: bool = true,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.path.len > 0) alloc.free(self.path);
        self.* = undefined;
    }

    fn takePath(self: *@This()) []u8 {
        const path = self.path;
        self.path = "";
        return path;
    }
};

fn documentFilterFieldForExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !?DocumentFilterField {
    if (try parseDocumentCastExpression(tokens)) |cast| {
        var field = (try documentFilterFieldForExpressionAlloc(alloc, cast.expression, schema, virtual_schema, source_ref, require_index)) orelse return error.UnsupportedSqlShape;
        errdefer field.deinit(alloc);
        if (!documentFieldTypeProvesCast(field.field_type, cast.target_type)) return error.UnsupportedSqlShape;
        field.field_type = cast.target_type;
        return field;
    }
    if (tokens.len == 1 and tokens[0].kind == .identifier) {
        const name = try documentIdentifierName(tokens[0], source_ref);
        if (std.mem.eql(u8, name, "_id")) return null;
        if (documentFieldColumn(schema, name)) |column| {
            if (require_index and !documentColumnIndexReady(column)) return error.DocumentSqlIndexUnavailable;
            return .{
                .path = try documentFilterPathAlloc(alloc, column.path),
                .field_name = column.name,
                .field_type = column.field_type,
            };
        }
        const virtual_field = documentVirtualField(schema, virtual_schema, name) orelse return error.InvalidSqlCatalog;
        if (require_index and virtual_field.source != .index_definition) return error.DocumentSqlIndexUnavailable;
        if (!documentVirtualFieldSupportsDirectFilter(virtual_field)) return error.UnsupportedSqlShape;
        return .{
            .path = try documentFilterPathAlloc(alloc, virtual_field.path),
            .field_name = virtual_field.name,
            .field_type = virtual_field.field_type orelse .keyword,
            .exact_declared_path = virtual_field.field_type != null,
        };
    }

    var expression = (try parseDocumentJsonPathExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref)) orelse return null;
    defer expression.deinit(alloc);
    const exact_column = documentColumnForPath(schema, expression.path);
    if (exact_column) |column| {
        if (require_index and !documentColumnIndexReady(column)) return error.DocumentSqlIndexUnavailable;
        return .{
            .path = try alloc.dupe(u8, expression.path),
            .field_name = column.name,
            .field_type = column.field_type,
        };
    }
    if (source_binding.documentSqlTypedPathType(virtual_schema, expression.path)) |field_type| {
        if (require_index) return error.DocumentSqlIndexUnavailable;
        return .{
            .path = try alloc.dupe(u8, expression.path),
            .field_type = field_type,
            .exact_declared_path = true,
        };
    }
    if (require_index and !documentFilterPathIndexReady(schema, expression.path, expression.root_column)) return error.DocumentSqlIndexUnavailable;
    return .{
        .path = try alloc.dupe(u8, expression.path),
        .field_type = expression.root_column.field_type,
        .exact_declared_path = false,
    };
}

fn documentOrderFieldForExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !?DocumentFilterField {
    if ((try parseDocumentCastExpression(tokens)) != null) {
        return try documentFilterFieldForExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref, false);
    }
    if (tokens.len == 1 and tokens[0].kind == .identifier) {
        const name = try documentIdentifierName(tokens[0], source_ref);
        if (documentFieldColumn(schema, name)) |column| {
            return .{
                .path = try documentFilterPathAlloc(alloc, column.path),
                .field_type = column.field_type,
            };
        }
        const virtual_field = documentVirtualField(schema, virtual_schema, name) orelse return error.InvalidSqlCatalog;
        if (!documentVirtualFieldProvidesJsonPathRoot(virtual_field.source)) return error.InvalidSqlCatalog;
        if (!documentVirtualFieldSupportsDirectFilter(virtual_field)) return error.UnsupportedSqlShape;
        return .{
            .path = try documentFilterPathAlloc(alloc, virtual_field.path),
            .field_type = virtual_field.field_type orelse .keyword,
            .exact_declared_path = virtual_field.field_type != null,
        };
    }

    var expression = (try parseDocumentJsonPathExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref)) orelse return null;
    defer expression.deinit(alloc);
    const exact_column = documentColumnForPath(schema, expression.path);
    const typed_path_type = source_binding.documentSqlTypedPathType(virtual_schema, expression.path);
    return .{
        .path = try alloc.dupe(u8, expression.path),
        .field_type = if (exact_column) |column| column.field_type else typed_path_type orelse .keyword,
        .exact_declared_path = exact_column != null or typed_path_type != null,
    };
}

fn documentAggregateFieldForExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
    require_index: bool,
) !?DocumentFilterField {
    var field = (try documentFilterFieldForExpressionAlloc(alloc, tokens, schema, virtual_schema, source_ref, require_index)) orelse return null;
    errdefer field.deinit(alloc);
    return switch (field.field_type) {
        .keyword, .numeric, .boolean, .datetime, .geopoint, .geoshape => field,
        else => error.UnsupportedSqlShape,
    };
}

fn documentVirtualAggregatePathIsScalar(path: []const u8) bool {
    if (path.len == 0) return false;
    const normalized = if (path[0] == '/') path[1..] else path;
    return std.mem.indexOfAny(u8, normalized, "/.") == null;
}

fn documentVirtualFieldProvidesJsonPathRoot(source: source_binding.DocumentSqlVirtualFieldSource) bool {
    return source == .index_definition or source == .typed_path_metadata or source == .view_mapping;
}

fn documentVirtualFieldSupportsDirectFilter(field: source_binding.DocumentSqlVirtualField) bool {
    if (!documentVirtualFieldProvidesJsonPathRoot(field.source)) return false;
    return field.source == .view_mapping or documentVirtualAggregatePathIsScalar(field.path);
}

fn documentColumnIndexReady(column: runtime_schema.RelationalColumn) bool {
    return column.indexed and column.index_lifecycle == .ready;
}

fn documentFilterPathIndexReady(
    schema: runtime_schema.TableSchema,
    path: []const u8,
    root_column: runtime_schema.RelationalColumn,
) bool {
    if (documentColumnForPath(schema, path)) |column| {
        if (documentColumnIndexReady(column)) return true;
    }
    return root_column.field_type == .json and
        documentColumnIndexReady(root_column) and
        documentPathContainsPath(root_column.path, path);
}

fn documentColumnForPath(schema: runtime_schema.TableSchema, path: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (documentPathEquals(column.path, path)) return column;
    }
    return null;
}

fn documentPathEquals(column_path: []const u8, filter_path: []const u8) bool {
    const normalized_column = if (column_path.len > 0 and column_path[0] == '/') column_path[1..] else column_path;
    const normalized_filter = if (filter_path.len > 0 and filter_path[0] == '/') filter_path[1..] else filter_path;
    return std.mem.eql(u8, normalized_column, normalized_filter);
}

fn documentPathContainsPath(parent_path: []const u8, child_path: []const u8) bool {
    const parent = if (parent_path.len > 0 and parent_path[0] == '/') parent_path[1..] else parent_path;
    const child = if (child_path.len > 0 and child_path[0] == '/') child_path[1..] else child_path;
    return std.mem.eql(u8, parent, child) or
        (child.len > parent.len and std.mem.startsWith(u8, child, parent) and child[parent.len] == '/');
}

fn findTopLevelScalarFilterOperator(tokens: []const Token) ?usize {
    var depth: usize = 0;
    for (tokens, 0..) |token, i| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .eq, .neq, .gt, .gte, .lt, .lte => if (depth == 0) return i,
            .identifier => if (depth == 0 and (token.matchesKeywordTag(.in) or token.matchesKeywordTag(.like) or token.matchesKeywordTag(.ilike) or token.matchesKeywordTag(.between) or token.matchesKeywordTag(.is))) return i,
            else => {},
        }
    }
    return null;
}

fn tokenLiteralJsonAlloc(alloc: std.mem.Allocator, token: Token) ![]u8 {
    return switch (token.kind) {
        .string => try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(token.text, .{})}),
        .number => try alloc.dupe(u8, token.text),
        .identifier => blk: {
            if (token.matchesKeywordTag(.true)) break :blk try alloc.dupe(u8, "true");
            if (token.matchesKeywordTag(.false)) break :blk try alloc.dupe(u8, "false");
            if (token.matchesKeywordTag(.null)) break :blk try alloc.dupe(u8, "null");
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(token.text, .{})});
        },
        else => error.UnsupportedSqlShape,
    };
}

fn tokenLiteralListSqlInJsonAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?[]u8 {
    if (tokens.len < 3 or tokens[0].kind != .lparen or tokens[tokens.len - 1].kind != .rparen) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeByte('[');
    var pos: usize = 1;
    var count: usize = 0;
    var emitted: usize = 0;
    while (pos + 1 < tokens.len) {
        if (count > 0) {
            if (tokens[pos].kind != .comma) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos + 1 >= tokens.len) return error.UnsupportedSqlShape;
        }
        if (!tokenIsNullLiteral(tokens[pos])) {
            if (emitted > 0) try writer.writeByte(',');
            const value_json = try tokenLiteralJsonAlloc(alloc, tokens[pos]);
            defer alloc.free(value_json);
            try writer.writeAll(value_json);
            emitted += 1;
        }
        count += 1;
        pos += 1;
    }
    if (count == 0 or pos != tokens.len - 1) return error.UnsupportedSqlShape;
    if (emitted == 0) {
        out.deinit();
        return null;
    }
    try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn documentIdLiteralAlloc(alloc: std.mem.Allocator, token: Token) ![]const u8 {
    if (token.kind != .string and token.kind != .identifier and token.kind != .number) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn parseDocumentIdInListIntoAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    out: *ParsedDocumentWhere,
) !void {
    if (tokens.len < 3 or tokens[0].kind != .lparen or tokens[tokens.len - 1].kind != .rparen) return error.UnsupportedSqlShape;
    var pos: usize = 1;
    var count: usize = 0;
    while (pos + 1 < tokens.len) {
        if (count > 0) {
            if (tokens[pos].kind != .comma) return error.UnsupportedSqlShape;
            pos += 1;
            if (pos + 1 >= tokens.len) return error.UnsupportedSqlShape;
        }
        if (!tokenIsNullLiteral(tokens[pos])) {
            const id = try documentIdLiteralAlloc(alloc, tokens[pos]);
            errdefer alloc.free(id);
            try out.ids.append(alloc, id);
        }
        count += 1;
        pos += 1;
    }
    if (count == 0 or pos != tokens.len - 1) return error.UnsupportedSqlShape;
}

fn buildConjunctiveFilterJsonAlloc(alloc: std.mem.Allocator, clauses: []const []const u8) !?[]const u8 {
    if (clauses.len == 0) return null;
    if (clauses.len == 1) return try alloc.dupe(u8, clauses[0]);
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"bool\":{\"filter\":[");
    for (clauses, 0..) |clause, i| {
        if (i > 0) try writer.writeByte(',');
        try writer.writeAll(clause);
    }
    try writer.writeAll("]}}");
    return try out.toOwnedSlice();
}

fn documentFilterPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (path.len == 0) return error.InvalidSqlCatalog;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '/');
    var pos: usize = if (path[0] == '/') 1 else 0;
    if (pos >= path.len) return error.InvalidSqlCatalog;
    var previous_was_separator = false;
    while (pos < path.len) : (pos += 1) {
        const c = path[pos];
        if (c == '/' or c == '.') {
            if (previous_was_separator) return error.InvalidSqlCatalog;
            previous_was_separator = true;
            try out.append(alloc, '/');
            continue;
        }
        previous_was_separator = false;
        try out.append(alloc, c);
    }
    if (previous_was_separator) return error.InvalidSqlCatalog;
    return try out.toOwnedSlice(alloc);
}

fn parseLimit(tokens: []const Token, limit_index: usize) !u32 {
    if (limit_index + 1 >= tokens.len or tokens[limit_index + 1].kind != .number) return error.UnsupportedSqlShape;
    if (limit_index + 2 < tokens.len and tokens[limit_index + 2].kind != .semicolon) return error.UnsupportedSqlShape;
    const value = try std.fmt.parseUnsigned(u32, tokens[limit_index + 1].text, 10);
    if (value == 0) return error.UnsupportedSqlShape;
    return value;
}

fn parseDocumentFromTailAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
) !DocumentFromBinding {
    const comma = findComma(tokens, 0);
    if (comma == null) {
        if (findTopLevelKeyword(tokens, .join)) |join_index| {
            return try parseDocumentLateralUnnestFromTailAlloc(alloc, table_name, tokens, join_index, schema, virtual_schema);
        }
        return .{ .source_ref = .{
            .table_name = table_name,
            .alias = try parseFromTailAlias(tokens),
        } };
    }

    const split = comma.?;
    if (findComma(tokens, split + 1) != null) return error.DocumentSqlUnnestUnsupported;
    const source_ref = DocumentSourceRef{
        .table_name = table_name,
        .alias = try parseFromTailAlias(tokens[0..split]),
    };
    var unnest = try parseDocumentUnnestAlloc(alloc, tokens[split + 1 ..], schema, virtual_schema, source_ref);
    errdefer unnest.deinit(alloc);
    return .{
        .source_ref = source_ref,
        .unnest = unnest,
    };
}

fn parseDocumentLateralUnnestFromTailAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    tokens: []const Token,
    join_index: usize,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
) !DocumentFromBinding {
    const join_modifier_start = documentLateralUnnestJoinModifierStart(tokens, join_index);
    const source_ref = DocumentSourceRef{
        .table_name = table_name,
        .alias = try parseFromTailAlias(tokens[0..join_modifier_start]),
    };
    const join_kind = documentLateralUnnestJoinKind(tokens, join_index);
    if (join_kind == .unsupported) return error.DocumentSqlUnsupportedJoin;

    var unnest_start = join_index + 1;
    if (unnest_start >= tokens.len or !tokens[unnest_start].matchesKeywordTag(.lateral)) return error.DocumentSqlUnsupportedJoin;
    unnest_start += 1;
    if (unnest_start >= tokens.len) return error.DocumentSqlUnsupportedJoin;
    if (tokens[unnest_start].kind == .lparen) return error.DocumentSqlLateralRequiresNativeProducer;

    const on_index = findTopLevelKeywordInRange(tokens, unnest_start, tokens.len, .on);
    const unnest_end = on_index orelse tokens.len;
    if (unnest_end <= unnest_start) return error.DocumentSqlUnsupportedJoin;
    switch (join_kind) {
        .inner => {
            const on = on_index orelse return error.DocumentSqlUnsupportedJoin;
            try requireDocumentLateralUnnestOnTrue(tokens[on + 1 ..]);
        },
        .cross => if (on_index != null) return error.DocumentSqlUnsupportedJoin,
        .unsupported => unreachable,
    }

    var unnest = try parseDocumentUnnestAlloc(alloc, tokens[unnest_start..unnest_end], schema, virtual_schema, source_ref);
    errdefer unnest.deinit(alloc);
    return .{
        .source_ref = source_ref,
        .unnest = unnest,
    };
}

const DocumentLateralUnnestJoinKind = enum {
    inner,
    cross,
    unsupported,
};

fn documentLateralUnnestJoinModifierStart(tokens: []const Token, join_index: usize) usize {
    if (join_index == 0) return 0;
    const previous = tokens[join_index - 1];
    if (previous.matchesKeywordTag(.inner) or
        previous.matchesKeywordTag(.cross) or
        previous.matchesKeywordTag(.left) or
        previous.matchesKeywordTag(.right) or
        previous.matchesKeywordTag(.full))
    {
        return join_index - 1;
    }
    return join_index;
}

fn documentLateralUnnestJoinKind(tokens: []const Token, join_index: usize) DocumentLateralUnnestJoinKind {
    if (join_index == 0) return .inner;
    const previous = tokens[join_index - 1];
    if (previous.matchesKeywordTag(.cross)) return .cross;
    if (previous.matchesKeywordTag(.inner)) return .inner;
    if (previous.matchesKeywordTag(.left) or
        previous.matchesKeywordTag(.right) or
        previous.matchesKeywordTag(.full))
    {
        return .unsupported;
    }
    return .inner;
}

fn requireDocumentLateralUnnestOnTrue(tokens: []const Token) !void {
    if (tokens.len != 1) return error.DocumentSqlUnsupportedJoin;
    if (!tokens[0].matchesKeywordTag(.true)) return error.DocumentSqlUnsupportedJoin;
}

fn parseDocumentUnnestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
    virtual_schema: source_binding.DocumentSqlSchema,
    source_ref: DocumentSourceRef,
) !DocumentUnnest {
    if (tokens.len < 5 or tokens[0].kind != .identifier or !std.ascii.eqlIgnoreCase(tokens[0].text, "unnest")) return error.UnsupportedSqlShape;
    if (tokens[1].kind != .lparen) return error.UnsupportedSqlShape;
    var depth: usize = 0;
    var close_index: ?usize = null;
    for (tokens[1..], 1..) |token, i| {
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return error.UnsupportedSqlShape;
                depth -= 1;
                if (depth == 0) {
                    close_index = i;
                    break;
                }
            },
            else => {},
        }
    }
    const close = close_index orelse return error.UnsupportedSqlShape;
    if (close <= 2) return error.UnsupportedSqlShape;
    const alias_tokens = tokens[close + 1 ..];
    if (findComma(alias_tokens, 0) != null) return error.DocumentSqlUnnestUnsupported;
    const alias = (try parseFromTailAlias(alias_tokens)) orelse return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, alias, '.') != null) return error.UnsupportedSqlShape;

    const field_tokens = tokens[2..close];
    if (documentUnnestArgumentHasNestedOrMultipleInputs(field_tokens)) return error.DocumentSqlUnnestUnsupported;
    if (field_tokens.len != 1 or field_tokens[0].kind != .identifier) return error.UnsupportedSqlShape;
    const field_name = try documentIdentifierName(field_tokens[0], source_ref);
    if (std.mem.eql(u8, field_name, "_id") or std.mem.eql(u8, field_name, "_doc")) return error.UnsupportedSqlShape;
    const column = documentVirtualField(schema, virtual_schema, field_name) orelse return error.InvalidSqlCatalog;
    if (column.field_type != .array) return error.DocumentSqlUnnestRequiresArray;
    return .{
        .field = try documentFilterPathAlloc(alloc, column.path),
        .alias = try alloc.dupe(u8, alias),
        .item_type = column.array_item_type orelse .json,
    };
}

fn documentUnnestArgumentHasNestedOrMultipleInputs(tokens: []const Token) bool {
    if (findComma(tokens, 0) != null) return true;
    for (tokens) |token| {
        if (token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "unnest")) return true;
    }
    return false;
}

fn parseFromTailAlias(tokens: []const Token) !?[]const u8 {
    if (tokens.len == 0) return null;
    if (tokens.len == 1) return try parseFromTailAliasIdentifier(tokens[0]);
    if (tokens.len == 2 and tokens[0].matchesKeywordTag(.as)) return try parseFromTailAliasIdentifier(tokens[1]);
    return error.UnsupportedSqlShape;
}

fn parseFromTailAliasIdentifier(token: Token) ![]const u8 {
    if (token.kind != .identifier) return error.UnsupportedSqlShape;
    if (token.keyword != null) return error.UnsupportedSqlShape;
    return token.text;
}

fn rejectUnsupportedDocumentStatementShape(
    tokens: []const Token,
    from_index: usize,
    source_tail_end: usize,
    allow_group_by: bool,
    allow_having: bool,
) !void {
    if (findTopLevelKeywordInRange(tokens, source_tail_end, tokens.len, .offset) != null or
        findTopLevelKeywordInRange(tokens, source_tail_end, tokens.len, .fetch) != null)
    {
        return error.DocumentSqlPaginationUnsupported;
    }
    if (findTopLevelKeywordInRange(tokens, source_tail_end, tokens.len, .@"for") != null) {
        return error.DocumentSqlLockingUnsupported;
    }
    if (findTopLevelKeywordInRange(tokens, source_tail_end, tokens.len, .window) != null) {
        return error.DocumentSqlWindowUnsupported;
    }
    if (findTopLevelKeyword(tokens, .@"union") != null or
        findTopLevelKeyword(tokens, .intersect) != null or
        findTopLevelKeyword(tokens, .except) != null or
        findTopLevelKeyword(tokens, .with) != null or
        findTopLevelKeyword(tokens, .recursive) != null)
    {
        return error.DocumentSqlUnsupportedJoin;
    }
    if (findTopLevelKeyword(tokens, .join) != null and
        !documentFromTailContainsOnlyLateralUnnestJoin(tokens, from_index, source_tail_end))
    {
        if (documentFromTailContainsLateralSubquery(tokens, from_index, source_tail_end)) return error.DocumentSqlLateralRequiresNativeProducer;
        return error.DocumentSqlUnsupportedJoin;
    }
    if (!allow_group_by and findTopLevelKeyword(tokens, .group) != null) return error.UnsupportedSqlShape;
    if (!allow_having and findTopLevelKeyword(tokens, .having) != null) return error.UnsupportedSqlShape;
    if (from_index + 2 < source_tail_end) {
        if (findTopLevelCommaInRange(tokens, from_index + 2, source_tail_end)) |comma| {
            if (!documentFromTailCommaStartsUnnest(tokens, comma, source_tail_end)) return error.DocumentSqlUnsupportedJoin;
        }
    }
}

fn documentFromTailCommaStartsUnnest(tokens: []const Token, comma_index: usize, source_tail_end: usize) bool {
    if (comma_index + 1 >= source_tail_end) return false;
    const token = tokens[comma_index + 1];
    return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "unnest");
}

fn documentFromTailContainsOnlyLateralUnnestJoin(tokens: []const Token, from_index: usize, source_tail_end: usize) bool {
    if (from_index + 2 >= source_tail_end) return false;
    const tail = tokens[from_index + 2 .. source_tail_end];
    const join_index = findTopLevelKeyword(tail, .join) orelse return false;
    if (findTopLevelKeywordInRange(tail, join_index + 1, tail.len, .join) != null) return false;
    if (documentLateralUnnestJoinKind(tail, join_index) == .unsupported) return false;
    var unnest_start = join_index + 1;
    if (unnest_start >= tail.len or !tail[unnest_start].matchesKeywordTag(.lateral)) return false;
    unnest_start += 1;
    if (unnest_start >= tail.len) return false;
    const on_index = findTopLevelKeywordInRange(tail, unnest_start, tail.len, .on);
    const unnest_end = on_index orelse tail.len;
    if (unnest_end <= unnest_start) return false;
    return tail[unnest_start].kind == .identifier and std.ascii.eqlIgnoreCase(tail[unnest_start].text, "unnest");
}

fn documentFromTailContainsLateralSubquery(tokens: []const Token, from_index: usize, source_tail_end: usize) bool {
    if (from_index + 2 >= source_tail_end) return false;
    const tail = tokens[from_index + 2 .. source_tail_end];
    const join_index = findTopLevelKeyword(tail, .join) orelse return false;
    var lateral_index = join_index + 1;
    if (lateral_index >= tail.len) return false;
    if (!tail[lateral_index].matchesKeywordTag(.lateral)) return false;
    lateral_index += 1;
    return lateral_index < tail.len and tail[lateral_index].kind == .lparen;
}

fn documentIdentifierName(token: Token, source_ref: DocumentSourceRef) ![]const u8 {
    if (token.kind != .identifier) return error.UnsupportedSqlShape;
    const dot = std.mem.indexOfScalar(u8, token.text, '.') orelse return token.text;
    if (dot == 0 or dot + 1 >= token.text.len) return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, token.text[dot + 1 ..], '.') != null) return error.UnsupportedSqlShape;
    const qualifier = token.text[0..dot];
    if (!source_ref.matchesQualifier(qualifier)) return error.InvalidSqlCatalog;
    return token.text[dot + 1 ..];
}

fn documentSqlStatementEnd(tokens: []const Token) usize {
    if (tokens.len > 0 and tokens[tokens.len - 1].kind == .semicolon) return tokens.len - 1;
    return tokens.len;
}

fn documentStatementTailKeywordIndex(tokens: []const Token, from_index: usize, keyword: token_mod.TokenKeyword) ?usize {
    const idx = findTopLevelKeyword(tokens, keyword) orelse return null;
    if (idx == from_index + 3 and from_index + 2 < tokens.len and tokens[from_index + 2].matchesKeywordTag(.as)) return null;
    return idx;
}

fn minOptionalIndex(indexes: []const ?usize) ?usize {
    var out: ?usize = null;
    for (indexes) |maybe| {
        if (maybe) |value| {
            out = if (out) |current| @min(current, value) else value;
        }
    }
    return out;
}

fn documentFieldExists(schema: runtime_schema.TableSchema, field: []const u8) bool {
    return documentFieldColumn(schema, field) != null;
}

fn documentFieldColumn(schema: runtime_schema.TableSchema, field: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (std.mem.eql(u8, column.name, field)) return column;
    }
    return null;
}

fn freeProjection(alloc: std.mem.Allocator, projection: []DocumentProjection) void {
    for (projection) |*item| item.deinit(alloc);
    if (projection.len > 0) alloc.free(projection);
}

fn findComma(tokens: []const Token, start: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < tokens.len) : (i += 1) {
        switch (tokens[i].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth == 0) return null;
                depth -= 1;
            },
            .comma => if (depth == 0) return i,
            else => {},
        }
    }
    return null;
}

fn findTopLevelKeyword(tokens: []const Token, keyword: token_mod.TokenKeyword) ?usize {
    return findTopLevelKeywordInRange(tokens, 0, tokens.len, keyword);
}

fn findTopLevelKeywordInRange(tokens: []const Token, start: usize, end: usize, keyword: token_mod.TokenKeyword) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < end and i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0 and token.matchesKeywordTag(keyword)) return i,
            else => {},
        }
    }
    return null;
}

fn findTopLevelCommaInRange(tokens: []const Token, start: usize, end: usize) ?usize {
    var depth: usize = 0;
    var i = start;
    while (i < end and i < tokens.len) : (i += 1) {
        switch (tokens[i].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .comma => if (depth == 0) return i,
            else => {},
        }
    }
    return null;
}

fn findTopLevelAnd(tokens: []const Token, start: usize) ?usize {
    var depth: usize = 0;
    var between_pending_and = false;
    var i = start;
    while (i < tokens.len) : (i += 1) {
        switch (tokens[i].kind) {
            .lparen => depth += 1,
            .rparen => {
                if (depth > 0) depth -= 1;
            },
            .identifier => if (depth == 0) {
                if (tokens[i].matchesKeywordTag(.between)) {
                    between_pending_and = true;
                } else if (tokens[i].matchesKeywordTag(.@"and")) {
                    if (between_pending_and) {
                        between_pending_and = false;
                    } else {
                        return i;
                    }
                }
            },
            else => {},
        }
    }
    return null;
}

test "document SQL lowers id lookup projection" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id = 'doc:a'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("docs", lowered.table_name);
    try std.testing.expectEqual(@as(usize, 2), lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, lowered.projection[0].kind);
    try std.testing.expectEqualStrings("title", lowered.projection[1].field);
    try std.testing.expectEqualStrings("doc:a", lowered.producer.id_lookup.ids[0]);
}

test "document SQL validates retained generated read ast before token planning" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id = 'doc:a'");
    defer parsed.deinit(alloc);

    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);

    if (parsed.generated_statement) |*generated_statement| {
        if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
            .read => |read| read.source_tokens = null,
            else => return error.TestUnexpectedResult,
        } else return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;

    try std.testing.expectEqual(sql_statement_kind.SqlReadStatementKind.query, parsed.readStatementKindIncludingGeneratedAst().?);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL expands star projection with document virtual columns" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT * FROM docs WHERE _id = 'doc:a'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, lowered.projection[0].kind);
    try std.testing.expectEqualStrings("_id", lowered.projection[0].output);
    try std.testing.expectEqual(DocumentProjectionKind.doc, lowered.projection[1].kind);
    try std.testing.expectEqualStrings("_doc", lowered.projection[1].output);
    try std.testing.expectEqualStrings("title", lowered.projection[2].field);
    try std.testing.expectEqualStrings("status", lowered.projection[3].field);
}

test "document SQL expands qualified star projection with document virtual columns" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var table = try tokenized.ParsedSql.initAlloc(alloc, "SELECT docs.* FROM docs WHERE _id = 'doc:a'");
    defer table.deinit(alloc);
    var table_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &table, schema);
    defer table_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), table_lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, table_lowered.projection[0].kind);
    try std.testing.expectEqual(DocumentProjectionKind.doc, table_lowered.projection[1].kind);
    try std.testing.expectEqualStrings("title", table_lowered.projection[2].field);
    try std.testing.expectEqualStrings("status", table_lowered.projection[3].field);

    var alias = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d.*, d.status FROM docs AS d WHERE d._id = 'doc:a'");
    defer alias.deinit(alloc);
    var alias_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &alias, schema);
    defer alias_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 5), alias_lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, alias_lowered.projection[0].kind);
    try std.testing.expectEqual(DocumentProjectionKind.doc, alias_lowered.projection[1].kind);
    try std.testing.expectEqualStrings("title", alias_lowered.projection[2].field);
    try std.testing.expectEqualStrings("status", alias_lowered.projection[3].field);
    try std.testing.expectEqualStrings("status", alias_lowered.projection[4].field);
}

test "document SQL rejects unsupported tail keywords as source aliases" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };

    var keyword_alias_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs offset LIMIT 10");
    defer keyword_alias_tail.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlPaginationUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &keyword_alias_tail, schema));

    var offset_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' OFFSET 1");
    defer offset_tail.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlPaginationUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &offset_tail, schema));

    var explicit_keyword_alias = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs AS fetch LIMIT 10");
    defer explicit_keyword_alias.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &explicit_keyword_alias, schema));

    var fetch_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs FETCH FIRST 10 ROWS ONLY");
    defer fetch_tail.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlPaginationUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &fetch_tail, schema));

    var locking_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' FOR UPDATE");
    defer locking_tail.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlLockingUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &locking_tail, schema));

    var window_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WINDOW w AS () LIMIT 10");
    defer window_tail.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlWindowUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &window_tail, schema));

    try std.testing.expectError(error.UnexpectedToken, tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs offset"));

    var aggregate_having_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs HAVING count(*) > 0");
    defer aggregate_having_tail.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &aggregate_having_tail, schema, .{ .max_rows = 25 }));
}

test "document SQL rejects unsupported compound and multi-source shapes" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };

    var union_read = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' UNION SELECT _id FROM docs WHERE _id = 'doc:b'");
    defer union_read.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnsupportedJoin, lowerDocumentReadPlanParsedSqlAlloc(alloc, &union_read, schema));

    var intersect_read = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' INTERSECT SELECT _id FROM docs WHERE status = 'trial'");
    defer intersect_read.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnsupportedJoin, lowerDocumentReadPlanParsedSqlAlloc(alloc, &intersect_read, schema));

    var comma_join = try tokenized.ParsedSql.initAlloc(alloc, "SELECT docs._id FROM docs, other WHERE docs.status = 'active' LIMIT 10");
    defer comma_join.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnsupportedJoin, lowerDocumentReadPlanParsedSqlAlloc(alloc, &comma_join, schema));

    var explicit_join = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id FROM docs d JOIN other o ON d._id = o.doc_id WHERE d.status = 'active' LIMIT 10");
    defer explicit_join.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnsupportedJoin, lowerDocumentReadPlanParsedSqlAlloc(alloc, &explicit_join, schema));

    var with_read = try tokenized.ParsedSql.initAlloc(alloc, "WITH source AS (SELECT _id FROM docs WHERE status = 'active') SELECT _id FROM source");
    defer with_read.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &with_read, schema));

    var recursive_with = try tokenized.ParsedSql.initAlloc(alloc, "WITH RECURSIVE source AS (SELECT _id FROM docs) SELECT _id FROM source");
    defer recursive_with.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &recursive_with, schema));

    var aggregate_union = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs UNION SELECT count(*) AS row_count FROM docs");
    defer aggregate_union.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &aggregate_union, schema, .{ .max_rows = 25 }));
}

test "document SQL lowers id in lookup projection" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id IN ('doc:a', 'doc:b')");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", lowered.producer.id_lookup.ids[0]);
    try std.testing.expectEqualStrings("doc:b", lowered.producer.id_lookup.ids[1]);
}

test "document SQL rejects select projection modifiers" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "distinct", .path = "distinct", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var distinct = try tokenized.ParsedSql.initAlloc(alloc, "SELECT DISTINCT _id FROM docs WHERE _id = 'doc:a'");
    defer distinct.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlProjectionModifierUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &distinct, schema));

    var distinct_on = try tokenized.ParsedSql.initAlloc(alloc, "SELECT DISTINCT ON (status) _id FROM docs WHERE status = 'active' LIMIT 10");
    defer distinct_on.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlProjectionModifierUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &distinct_on, schema));

    var all = try tokenized.ParsedSql.initAlloc(alloc, "SELECT ALL _id FROM docs WHERE _id = 'doc:a'");
    defer all.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlProjectionModifierUnsupported, lowerDocumentReadPlanParsedSqlAlloc(alloc, &all, schema));
}

test "document SQL lowers id lookup with scalar residual filter" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE _id IN ('doc:a', 'doc:b') AND status = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", lowered.producer.id_lookup.ids[0]);
    try std.testing.expectEqualStrings("doc:b", lowered.producer.id_lookup.ids[1]);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.id_lookup.residual_filter_json.?);

    var reversed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' AND _id = 'doc:a' LIMIT 10");
    defer reversed.deinit(alloc);
    var reversed_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &reversed, schema);
    defer reversed_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), reversed_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", reversed_lowered.producer.id_lookup.ids[0]);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", reversed_lowered.producer.id_lookup.residual_filter_json.?);
}

test "document SQL lowers id lookup null membership to empty lookup" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{},
    };
    var equals_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = NULL LIMIT 10");
    defer equals_null.deinit(alloc);
    var equals_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &equals_null, schema);
    defer equals_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), equals_lowered.producer.id_lookup.ids.len);

    var mixed_in = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id IN (NULL, 'doc:a', NULL, 'doc:b') LIMIT 10");
    defer mixed_in.deinit(alloc);
    var mixed_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &mixed_in, schema);
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), mixed_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", mixed_lowered.producer.id_lookup.ids[0]);
    try std.testing.expectEqualStrings("doc:b", mixed_lowered.producer.id_lookup.ids[1]);

    var null_in = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id IN (NULL) LIMIT 10");
    defer null_in.deinit(alloc);
    var null_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &null_in, schema);
    defer null_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), null_lowered.producer.id_lookup.ids.len);
}

test "document SQL rejects id lookup with full text residual" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{},
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND full_text_search('body:alpha') LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL lowers qualified single table references" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "metadata", .path = "metadata", .field_type = .json, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, d._doc, d.status, d.metadata->>'plan' AS plan FROM docs AS d WHERE d.status = 'active' AND d.metadata->>'plan' = 'pro' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, lowered.projection[0].kind);
    try std.testing.expectEqual(DocumentProjectionKind.doc, lowered.projection[1].kind);
    try std.testing.expectEqualStrings("status", lowered.projection[2].field);
    try std.testing.expectEqualStrings("/metadata/plan", lowered.projection[3].field);
    try std.testing.expectEqualStrings("plan", lowered.projection[3].output);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"term\":{\"path\":\"/status\",\"value\":\"active\"}},{\"term\":{\"path\":\"/metadata/plan\",\"value\":\"pro\"}}]}}",
        lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL matches single table qualifiers case insensitively" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var alias = try tokenized.ParsedSql.initAlloc(alloc, "SELECT DOC_ALIAS._id FROM docs AS doc_alias WHERE Doc_Alias.status = 'active' LIMIT 10");
    defer alias.deinit(alloc);
    var alias_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &alias, schema);
    defer alias_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentProjectionKind.id, alias_lowered.projection[0].kind);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", alias_lowered.producer.indexed_query.filter_query_json.?);

    var table = try tokenized.ParsedSql.initAlloc(alloc, "SELECT DOCS._id FROM docs WHERE Docs.status = 'active' LIMIT 10");
    defer table.deinit(alloc);
    var table_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &table, schema);
    defer table_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentProjectionKind.id, table_lowered.projection[0].kind);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", table_lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL rejects unknown single table qualifier" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT other._id FROM docs AS d WHERE d.status = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var star = try tokenized.ParsedSql.initAlloc(alloc, "SELECT other.* FROM docs AS d WHERE d.status = 'active' LIMIT 10");
    defer star.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, lowerDocumentReadPlanParsedSqlAlloc(alloc, &star, schema));
}

test "document SQL lowers bounded order by over id lookup" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE _id IN ('doc:a', 'doc:b') ORDER BY title DESC LIMIT 1");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings("/title", lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.text, lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, lowered.order_by.?.direction);
    try std.testing.expectEqual(@as(?u32, 1), lowered.limit);
}

test "document SQL lowers ordered indexed query as bounded candidate producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE status = 'active' ORDER BY title DESC LIMIT 2");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanIncompleteTopK, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .indexed_scalar_filters = true,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("docs", lowered.table_name);
    try std.testing.expectEqual(@as(?u32, 2), lowered.limit);
    try std.testing.expectEqualStrings("/title", lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.text, lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, lowered.order_by.?.direction);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 25), lowered.producer.indexed_query.max_candidate_rows);
}

test "document SQL lowers order by over index-backed virtual schema fields" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "category", .path = "category", .source = .index_definition, .field_type = .keyword },
            .{ .name = "metadata", .path = "metadata", .source = .index_definition },
        },
    };

    var virtual_field = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, category FROM docs WHERE status = 'active' ORDER BY category DESC LIMIT 5");
    defer virtual_field.deinit(alloc);
    var virtual_field_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &virtual_field, schema, virtual_schema, .{
        .runtime_schema_scalar_filters = schema,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer virtual_field_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/category", virtual_field_lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, virtual_field_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, virtual_field_lowered.order_by.?.direction);
    try std.testing.expectEqual(@as(u32, 25), virtual_field_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", virtual_field_lowered.producer.bounded_scan.residual_filter_json.?);

    var virtual_json_path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'plan' AS plan FROM docs WHERE metadata->>'plan' = 'pro' ORDER BY metadata->>'plan' ASC LIMIT 5");
    defer virtual_json_path.deinit(alloc);
    var virtual_json_path_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &virtual_json_path, .{
        .storage_mode = .document,
    }, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer virtual_json_path_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/plan", virtual_json_path_lowered.projection[1].field);
    try std.testing.expectEqualStrings("/metadata/plan", virtual_json_path_lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, virtual_json_path_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.asc, virtual_json_path_lowered.order_by.?.direction);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/plan\",\"value\":\"pro\"}}", virtual_json_path_lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 25), virtual_json_path_lowered.producer.indexed_query.max_candidate_rows);
}

test "document SQL lowers algebraic grouped count over indexed facts" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("docs", lowered.table_name);
    try std.testing.expectEqual(DocumentAggregateOp.count, lowered.aggregate.op);
    try std.testing.expectEqualStrings("row_count", lowered.aggregate.output);
    try std.testing.expectEqualStrings("/metadata/plan", lowered.group_by.?.field);
    try std.testing.expectEqualStrings("plan", lowered.group_by.?.source_field);
    try std.testing.expectEqualStrings("plan", lowered.group_by.?.output);
    try std.testing.expect(lowered.candidate_producer == null);
    try std.testing.expect(lowered.filter_query_json == null);
    try std.testing.expectEqual(@as(usize, 0), lowered.having.len);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);

    var ordered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan ORDER BY row_count DESC LIMIT 5");
    defer ordered.deinit(alloc);
    var ordered_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &ordered, schema);
    defer ordered_lowered.deinit(alloc);
    try std.testing.expect(ordered_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, ordered_lowered.order_by.?.key);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, ordered_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, ordered_lowered.order_by.?.direction);

    var unaliased_count = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) FROM docs GROUP BY plan HAVING count > 1 ORDER BY count DESC LIMIT 5");
    defer unaliased_count.deinit(alloc);
    var unaliased_count_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &unaliased_count, schema);
    defer unaliased_count_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("count", unaliased_count_lowered.aggregate.output);
    try std.testing.expectEqual(@as(usize, 1), unaliased_count_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, unaliased_count_lowered.having[0].key);
    try std.testing.expectEqualStrings("1", unaliased_count_lowered.having[0].value_json);
    try std.testing.expect(unaliased_count_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, unaliased_count_lowered.order_by.?.key);
    try std.testing.expectEqual(DocumentOrderDirection.desc, unaliased_count_lowered.order_by.?.direction);

    var ordered_count_expression = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan ORDER BY count(*) DESC LIMIT 5");
    defer ordered_count_expression.deinit(alloc);
    var ordered_count_expression_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &ordered_count_expression, schema);
    defer ordered_count_expression_lowered.deinit(alloc);
    try std.testing.expect(ordered_count_expression_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, ordered_count_expression_lowered.order_by.?.key);
    try std.testing.expectEqual(DocumentOrderDirection.desc, ordered_count_expression_lowered.order_by.?.direction);

    var ordered_sum_expression = try tokenized.ParsedSql.initAlloc(alloc, "SELECT sum(amount) AS total_amount FROM docs GROUP BY plan ORDER BY sum(amount) ASC LIMIT 5");
    defer ordered_sum_expression.deinit(alloc);
    var ordered_sum_expression_lowered = try lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &ordered_sum_expression, schema, .{ .max_rows = 25 });
    defer ordered_sum_expression_lowered.deinit(alloc);
    try std.testing.expect(ordered_sum_expression_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, ordered_sum_expression_lowered.order_by.?.key);
    try std.testing.expectEqual(DocumentOrderDirection.asc, ordered_sum_expression_lowered.order_by.?.direction);

    var ordered_group = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan ORDER BY plan ASC LIMIT 5");
    defer ordered_group.deinit(alloc);
    var ordered_group_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &ordered_group, schema);
    defer ordered_group_lowered.deinit(alloc);
    try std.testing.expect(ordered_group_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, ordered_group_lowered.order_by.?.key);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, ordered_group_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.asc, ordered_group_lowered.order_by.?.direction);

    var having = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan HAVING row_count > 1 ORDER BY row_count DESC LIMIT 5");
    defer having.deinit(alloc);
    var having_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &having, schema);
    defer having_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", having_lowered.filter_query_json.?);
    try std.testing.expectEqual(@as(usize, 1), having_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, having_lowered.having[0].key);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, having_lowered.having[0].field_type);
    try std.testing.expectEqual(DocumentAggregateHavingOp.gt, having_lowered.having[0].op);
    try std.testing.expectEqualStrings("1", having_lowered.having[0].value_json);

    var having_expression = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan HAVING count(*) >= 2 ORDER BY row_count DESC LIMIT 5");
    defer having_expression.deinit(alloc);
    var having_expression_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &having_expression, schema);
    defer having_expression_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), having_expression_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, having_expression_lowered.having[0].key);
    try std.testing.expectEqual(DocumentAggregateHavingOp.gte, having_expression_lowered.having[0].op);
    try std.testing.expectEqualStrings("2", having_expression_lowered.having[0].value_json);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, having_expression_lowered.order_by.?.key);

    var having_group = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan HAVING plan = 'pro' LIMIT 5");
    defer having_group.deinit(alloc);
    var having_group_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &having_group, schema);
    defer having_group_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), having_group_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, having_group_lowered.having[0].key);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, having_group_lowered.having[0].field_type);
    try std.testing.expectEqual(DocumentAggregateHavingOp.eq, having_group_lowered.having[0].op);
    try std.testing.expectEqualStrings("\"pro\"", having_group_lowered.having[0].value_json);

    var having_conjunction = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan HAVING row_count > 1 AND plan = 'pro' LIMIT 5");
    defer having_conjunction.deinit(alloc);
    var having_conjunction_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &having_conjunction, schema);
    defer having_conjunction_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), having_conjunction_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.aggregate, having_conjunction_lowered.having[0].key);
    try std.testing.expectEqual(DocumentAggregateHavingOp.gt, having_conjunction_lowered.having[0].op);
    try std.testing.expectEqualStrings("1", having_conjunction_lowered.having[0].value_json);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, having_conjunction_lowered.having[1].key);
    try std.testing.expectEqual(DocumentAggregateHavingOp.eq, having_conjunction_lowered.having[1].op);
    try std.testing.expectEqualStrings("\"pro\"", having_conjunction_lowered.having[1].value_json);

    var multi_key_group = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan, status LIMIT 5");
    defer multi_key_group.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &multi_key_group, schema));

    var having_bare_identifier = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan HAVING plan = pro LIMIT 5");
    defer having_bare_identifier.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &having_bare_identifier, schema));

    var having_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan HAVING row_count = NULL LIMIT 5");
    defer having_null.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &having_null, schema));

    var ordered_scalar = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs ORDER BY row_count DESC LIMIT 5");
    defer ordered_scalar.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &ordered_scalar, schema));

    var ordered_unknown = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan ORDER BY status DESC LIMIT 5");
    defer ordered_unknown.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &ordered_unknown, schema));

    var ordered_non_emitted_aggregate = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan ORDER BY sum(amount) DESC LIMIT 5");
    defer ordered_non_emitted_aggregate.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &ordered_non_emitted_aggregate, schema));

    var aggregate_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) FILTER (WHERE status = 'active') AS row_count FROM docs GROUP BY plan LIMIT 5");
    defer aggregate_filter.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &aggregate_filter, schema));
}

test "document SQL lowers qualified aggregate group by" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs d WHERE d.status = 'active' GROUP BY d.status LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/status", lowered.group_by.?.field);
    try std.testing.expectEqualStrings("status", lowered.group_by.?.source_field);
    try std.testing.expectEqualStrings("status", lowered.group_by.?.output);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.filter_query_json.?);

    var qualified_tail = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs d WHERE d.status = 'active' GROUP BY d.status HAVING d.status = 'active' ORDER BY d.status DESC LIMIT 5");
    defer qualified_tail.deinit(alloc);
    var qualified_tail_lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &qualified_tail, schema);
    defer qualified_tail_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), qualified_tail_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, qualified_tail_lowered.having[0].key);
    try std.testing.expectEqualStrings("\"active\"", qualified_tail_lowered.having[0].value_json);
    try std.testing.expect(qualified_tail_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, qualified_tail_lowered.order_by.?.key);
    try std.testing.expectEqual(DocumentOrderDirection.desc, qualified_tail_lowered.order_by.?.direction);

    var qualified_aggregate_alias = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs d GROUP BY d.status HAVING d.row_count > 1 LIMIT 5");
    defer qualified_aggregate_alias.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &qualified_aggregate_alias, schema));

    var wrong_qualifier = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs d GROUP BY d.status ORDER BY other.status DESC LIMIT 5");
    defer wrong_qualifier.deinit(alloc);
    try std.testing.expectError(error.InvalidSqlCatalog, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &wrong_qualifier, schema));
}

test "document SQL rejects aggregate projection modifiers" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{},
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT DISTINCT count(*) AS row_count FROM docs LIMIT 5");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlProjectionModifierUnsupported, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL requires algebraic materialization for catalog aggregate plan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    const indexes_json =
        \\{"alg":{"type":"algebraic","materializations":[{"name":"count_by_plan","op":"count","group_by":["plan"]}]}}
    ;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAlgebraicAggregatePlanWithIndexesJsonParsedSqlAlloc(alloc, &parsed, schema, indexes_json);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("alg", lowered.index_name.?);
    try std.testing.expectEqualStrings("count_by_plan", lowered.materialization_name.?);
    try std.testing.expectEqualStrings("/metadata/plan", lowered.group_by.?.field);
    try std.testing.expectEqualStrings("plan", lowered.group_by.?.source_field);
}

test "document SQL matches numeric algebraic aggregate materializations" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
        },
    };
    const indexes_json =
        \\{"alg":{"type":"algebraic","materializations":[{"name":"sum_by_status","op":"sum","group_by":["status"],"measure":"amount"},{"name":"avg_by_status","op":"avg","group_by":["status"],"measure":"amount"},{"name":"max_by_status","op":"max","group_by":["status"],"measure":"amount"}]}}
    ;

    var sum_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT sum(amount) AS total_amount FROM docs GROUP BY status LIMIT 5");
    defer sum_sql.deinit(alloc);
    var sum_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &sum_sql, schema, indexes_json);
    defer sum_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("alg", sum_lowered.index_name.?);
    try std.testing.expectEqualStrings("sum_by_status", sum_lowered.materialization_name.?);

    var avg_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT avg(amount) AS avg_amount FROM docs GROUP BY status LIMIT 5");
    defer avg_sql.deinit(alloc);
    var avg_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &avg_sql, schema, indexes_json);
    defer avg_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("alg", avg_lowered.index_name.?);
    try std.testing.expectEqualStrings("avg_by_status", avg_lowered.materialization_name.?);

    const sum_and_count_only_indexes_json =
        \\{"alg":{"type":"algebraic","materializations":[{"name":"count_by_status","op":"count","group_by":["status"]},{"name":"sum_by_status","op":"sum","group_by":["status"],"measure":"amount"}]}}
    ;
    var avg_without_native = try tokenized.ParsedSql.initAlloc(alloc, "SELECT avg(amount) AS avg_amount FROM docs GROUP BY status LIMIT 5");
    defer avg_without_native.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &avg_without_native, schema, sum_and_count_only_indexes_json));

    var wrong_measure = try tokenized.ParsedSql.initAlloc(alloc, "SELECT min(amount) AS min_amount FROM docs GROUP BY status LIMIT 5");
    defer wrong_measure.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &wrong_measure, schema, indexes_json));
}

test "document SQL matches schema-derived algebraic aggregate materializations" {
    const alloc = std.testing.allocator;
    var parsed_schema = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":0,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"body":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"note":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"metadata":{"type":"json"}},"additionalProperties":true}}}}
    );
    defer parsed_schema.deinit(alloc);
    const schema = try schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema.freeSchema(alloc, schema);

    const indexes_json =
        \\{"full_text_index_v0":{"name":"full_text_index_v0","type":"full_text"},"amount_alg":{"version":2,"table":"docs","schema_version":0,"capability_fingerprint":"8a6d29a74f129f6b","capability_lifecycle_status":"current","group_fields":[{"name":"status","path":"status","type":"string"},{"name":"amount","path":"amount","type":"number"},{"name":"note","path":"note","type":"string"}],"measure_fields":[{"name":"amount","path":"amount","type":"number"}],"time_fields":[],"dynamic_field_rules":[],"laws":[{"name":"count","id":"count","structure":"group","invertible":true},{"name":"sum","id":"sum","structure":"group","invertible":true},{"name":"avg","id":"avg","structure":"group","invertible":true},{"name":"min","id":"min","structure":"lattice","invertible":false},{"name":"max","id":"max","structure":"lattice","invertible":false}],"joins":[],"adaptive":{"observe":true,"lazy_materialization":true,"dematerialization":false,"min_observations":3},"materializations":[{"name":"auto_count_0","op":"count","group_by":["status"]},{"name":"auto_sum_3","op":"sum","group_by":["status"],"measure":"amount"},{"name":"auto_avg_4","op":"avg","group_by":["status"],"measure":"amount"}],"type":"algebraic","name":"amount_alg"}}
    ;

    var avg_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT avg(amount) AS avg_amount FROM docs GROUP BY status LIMIT 10");
    defer avg_sql.deinit(alloc);
    var avg_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &avg_sql, schema, indexes_json);
    defer avg_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("amount_alg", avg_lowered.index_name.?);
    try std.testing.expectEqualStrings("auto_avg_4", avg_lowered.materialization_name.?);
}

test "document SQL keeps filtered aggregate as native candidate producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    const indexes_json =
        \\{"alg":{"type":"algebraic","materializations":[{"name":"count_by_plan","op":"count","group_by":["plan"]}]}}
    ;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &parsed, schema, indexes_json);
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.index_name == null);
    try std.testing.expect(lowered.materialization_name == null);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expect(lowered.filter_query_json != null);
}

test "document SQL lowers aggregate id lookup with scalar residual candidate" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE _id IN ('doc:a', 'doc:b') AND status = 'active'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(usize, 2), lowered.candidate_producer.?.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", lowered.candidate_producer.?.id_lookup.ids[0]);
    try std.testing.expectEqualStrings("doc:b", lowered.candidate_producer.?.id_lookup.ids[1]);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.candidate_producer.?.id_lookup.residual_filter_json.?);
}

test "document SQL lowers grouped aggregate id lookup with scalar residual candidate" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
            .{ .name = "category", .path = "category", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE _id = 'doc:a' AND status = 'active' GROUP BY category LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(usize, 1), lowered.candidate_producer.?.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", lowered.candidate_producer.?.id_lookup.ids[0]);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.candidate_producer.?.id_lookup.residual_filter_json.?);
    try std.testing.expectEqualStrings("/category", lowered.group_by.?.field);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL capability-aware aggregate requires full text producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "body", .path = "body", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE full_text_search('body:alpha')");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .full_text_filters = false,
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .full_text_filters = true,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expectEqualStrings("body:alpha", lowered.candidate_producer.?.indexed_query.full_text_query.?);
}

test "document SQL capability-aware aggregate keeps full text candidate with scalar residual" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "body", .path = "body", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE full_text_search('body:alpha') AND status = 'active'");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .full_text_filters = true,
        .indexed_scalar_filters = false,
    }));

    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .full_text_filters = true,
        .indexed_scalar_filters = false,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expectEqualStrings("body:alpha", lowered.candidate_producer.?.indexed_query.full_text_query.?);
    try std.testing.expect(lowered.candidate_producer.?.indexed_query.filter_query_json == null);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.candidate_producer.?.indexed_query.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 25), lowered.candidate_producer.?.indexed_query.max_candidate_rows);
}

test "document SQL capability-aware aggregate keeps scalar candidate with scalar residual" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "category", .path = "category", .field_type = .keyword },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' AND category = 'release' GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .indexed_scalar_filter_paths = &.{"/status"},
    }));

    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .indexed_scalar_filter_paths = &.{"/status"},
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.candidate_producer.?.indexed_query.filter_query_json.?);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", lowered.candidate_producer.?.indexed_query.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 25), lowered.candidate_producer.?.indexed_query.max_candidate_rows);
    try std.testing.expectEqualStrings("/metadata/plan", lowered.group_by.?.field);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL keeps catalog aggregate plan without algebraic materialization" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    const indexes_json =
        \\{"alg":{"type":"algebraic","materializations":[{"name":"count_by_status","op":"count","group_by":["status"]}]}}
    ;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &parsed, schema, indexes_json);
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.index_name == null);
    try std.testing.expect(lowered.materialization_name == null);
    try std.testing.expect(lowered.filter_query_json != null);
    try std.testing.expectEqualStrings("/metadata/plan", lowered.group_by.?.field);
}

test "document SQL aggregate group by accepts index-backed virtual fields" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "status", .path = "status", .source = .declared_schema },
            .{ .name = "category", .path = "category", .source = .index_definition },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'archived' GROUP BY category LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, null, .{
        .runtime_schema_scalar_filters = schema,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expect(lowered.candidate_producer != null);
    try std.testing.expect(lowered.candidate_producer.? == .bounded_scan);
    try std.testing.expectEqualStrings("/category", lowered.group_by.?.field);
    try std.testing.expectEqualStrings("category", lowered.group_by.?.source_field);
    try std.testing.expectEqualStrings("category", lowered.group_by.?.output);
}

test "document SQL lowers numeric summary aggregates over bounded document producers" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
        },
    };

    var unfiltered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT sum(amount) AS total_amount FROM docs");
    defer unfiltered.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &unfiltered, schema, null, null));

    var unfiltered_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &unfiltered, schema, null, .{ .max_rows = 25 });
    defer unfiltered_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.sum, unfiltered_lowered.aggregate.op);
    try std.testing.expectEqualStrings("total_amount", unfiltered_lowered.aggregate.output);
    try std.testing.expect(unfiltered_lowered.aggregate.input != null);
    try std.testing.expectEqualStrings("/amount", unfiltered_lowered.aggregate.input.?.field);
    try std.testing.expect(unfiltered_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), unfiltered_lowered.candidate_producer.?.bounded_scan.max_rows);

    var min_amount = try tokenized.ParsedSql.initAlloc(alloc, "SELECT min(amount) AS min_amount FROM docs");
    defer min_amount.deinit(alloc);
    var min_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &min_amount, schema, null, .{ .max_rows = 25 });
    defer min_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.min, min_lowered.aggregate.op);
    try std.testing.expectEqualStrings("min_amount", min_lowered.aggregate.output);
    try std.testing.expect(min_lowered.aggregate.input != null);
    try std.testing.expectEqualStrings("/amount", min_lowered.aggregate.input.?.field);
    try std.testing.expect(min_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), min_lowered.candidate_producer.?.bounded_scan.max_rows);

    var avg_amount = try tokenized.ParsedSql.initAlloc(alloc, "SELECT avg(amount) AS avg_amount FROM docs");
    defer avg_amount.deinit(alloc);
    var avg_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &avg_amount, schema, null, .{ .max_rows = 25 });
    defer avg_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.avg, avg_lowered.aggregate.op);
    try std.testing.expectEqualStrings("avg_amount", avg_lowered.aggregate.output);
    try std.testing.expect(avg_lowered.aggregate.input != null);
    try std.testing.expectEqualStrings("/amount", avg_lowered.aggregate.input.?.field);
    try std.testing.expect(avg_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), avg_lowered.candidate_producer.?.bounded_scan.max_rows);

    var max_amount = try tokenized.ParsedSql.initAlloc(alloc, "SELECT max(amount) AS max_amount FROM docs WHERE status = 'active'");
    defer max_amount.deinit(alloc);
    var max_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &max_amount, schema, null, .{ .max_rows = 25 });
    defer max_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.max, max_lowered.aggregate.op);
    try std.testing.expectEqualStrings("max_amount", max_lowered.aggregate.output);
    try std.testing.expect(max_lowered.aggregate.input != null);
    try std.testing.expectEqualStrings("/amount", max_lowered.aggregate.input.?.field);
    try std.testing.expect(max_lowered.candidate_producer != null);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", max_lowered.candidate_producer.?.indexed_query.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 25), max_lowered.candidate_producer.?.indexed_query.max_candidate_rows);

    var empty_index_metadata_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &unfiltered, schema, "", .{ .max_rows = 25 });
    defer empty_index_metadata_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.sum, empty_index_metadata_lowered.aggregate.op);
    try std.testing.expect(empty_index_metadata_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), empty_index_metadata_lowered.candidate_producer.?.bounded_scan.max_rows);

    const unrelated_indexes_json =
        \\{"category_fts":{"type":"full_text","field":"category"}}
    ;
    var unrelated_index_metadata_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &unfiltered, schema, unrelated_indexes_json, .{ .max_rows = 25 });
    defer unrelated_index_metadata_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.sum, unrelated_index_metadata_lowered.aggregate.op);
    try std.testing.expect(unrelated_index_metadata_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), unrelated_index_metadata_lowered.candidate_producer.?.bounded_scan.max_rows);

    var filtered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT sum(amount) AS total_amount FROM docs WHERE status = 'active'");
    defer filtered.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &filtered, schema, null, null));

    var filtered_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &filtered, schema, null, .{ .max_rows = 25 });
    defer filtered_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.sum, filtered_lowered.aggregate.op);
    try std.testing.expect(filtered_lowered.candidate_producer != null);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", filtered_lowered.candidate_producer.?.indexed_query.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 25), filtered_lowered.candidate_producer.?.indexed_query.max_candidate_rows);

    var grouped = try tokenized.ParsedSql.initAlloc(alloc, "SELECT sum(amount) AS total_amount FROM docs GROUP BY status LIMIT 10");
    defer grouped.deinit(alloc);
    var grouped_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &grouped, schema, null, .{ .max_rows = 25 });
    defer grouped_lowered.deinit(alloc);
    try std.testing.expectEqual(DocumentAggregateOp.sum, grouped_lowered.aggregate.op);
    try std.testing.expect(grouped_lowered.group_by != null);
    try std.testing.expectEqualStrings("/status", grouped_lowered.group_by.?.field);
    try std.testing.expect(grouped_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), grouped_lowered.candidate_producer.?.bounded_scan.max_rows);

    const non_numeric_schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var non_numeric = try tokenized.ParsedSql.initAlloc(alloc, "SELECT sum(status) AS status_sum FROM docs");
    defer non_numeric.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &non_numeric, non_numeric_schema, null, .{ .max_rows = 25 }));
}

test "document SQL prefers algebraic materialization over bounded aggregate scan fallback" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    const indexes_json =
        \\{"alg":{"type":"algebraic","materializations":[{"name":"count_by_plan","op":"count","group_by":["plan"]}]}}
    ;
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY plan LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, indexes_json, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("alg", lowered.index_name.?);
    try std.testing.expectEqualStrings("count_by_plan", lowered.materialization_name.?);
    try std.testing.expect(lowered.candidate_producer == null);
}

test "document SQL lowers aggregate to policy bounded scan when no index can answer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = false },
        },
    };
    var filtered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan LIMIT 5");
    defer filtered.deinit(alloc);
    var lowered_filtered = try lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &filtered, schema, null, .{
        .indexed_scalar_filters = false,
        .bounded_scan = .{ .max_rows = 25, .max_bytes = 4096 },
    });
    defer lowered_filtered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered_filtered.candidate_producer.?.bounded_scan.max_rows);
    try std.testing.expectEqual(@as(?u64, 4096), lowered_filtered.candidate_producer.?.bounded_scan.max_bytes);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered_filtered.candidate_producer.?.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqualStrings("/metadata/plan", lowered_filtered.group_by.?.field);

    var unfiltered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs");
    defer unfiltered.deinit(alloc);
    var lowered_unfiltered = try lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &unfiltered, schema, null, .{
        .indexed_scalar_filters = false,
        .bounded_scan = .{ .max_rows = 25, .max_bytes = 8192 },
    });
    defer lowered_unfiltered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered_unfiltered.candidate_producer.?.bounded_scan.max_rows);
    try std.testing.expectEqual(@as(?u64, 8192), lowered_unfiltered.candidate_producer.?.bounded_scan.max_bytes);
    try std.testing.expect(lowered_unfiltered.candidate_producer.?.bounded_scan.residual_filter_json == null);
}

test "document SQL rejects algebraic group by without indexed facts" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "plan", .path = "metadata/plan", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active' GROUP BY plan");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentAlgebraicAggregatePlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL lowers json path projection" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "metadata", .path = "metadata", .field_type = .json },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'status' AS status, metadata#>>'{billing,plan}' AS plan FROM docs WHERE _id = 'doc:a'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), lowered.projection.len);
    try std.testing.expectEqualStrings("/metadata/status", lowered.projection[1].field);
    try std.testing.expectEqualStrings("status", lowered.projection[1].output);
    try std.testing.expectEqualStrings("/metadata/billing/plan", lowered.projection[2].field);
    try std.testing.expectEqualStrings("plan", lowered.projection[2].output);

    var function_projection = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, jsonb_extract_path_text(metadata, 'billing', 'plan') AS plan FROM docs WHERE _id = 'doc:a'");
    defer function_projection.deinit(alloc);
    var function_projection_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &function_projection, schema);
    defer function_projection_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), function_projection_lowered.projection.len);
    try std.testing.expectEqualStrings("/metadata/billing/plan", function_projection_lowered.projection[1].field);
    try std.testing.expectEqualStrings("plan", function_projection_lowered.projection[1].output);

    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "metadata", .path = "metadata", .source = .index_definition },
        },
    };
    var virtual_root = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'plan' AS plan FROM docs WHERE _id = 'doc:a'");
    defer virtual_root.deinit(alloc);
    var virtual_root_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &virtual_root, .{
        .storage_mode = .document,
    }, virtual_schema, .{});
    defer virtual_root_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/plan", virtual_root_lowered.projection[1].field);
    try std.testing.expectEqualStrings("plan", virtual_root_lowered.projection[1].output);

    var virtual_function = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, json_extract_path_text(metadata, 'plan') AS plan FROM docs WHERE _id = 'doc:a'");
    defer virtual_function.deinit(alloc);
    var virtual_function_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &virtual_function, .{
        .storage_mode = .document,
    }, virtual_schema, .{});
    defer virtual_function_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/plan", virtual_function_lowered.projection[1].field);
    try std.testing.expectEqualStrings("plan", virtual_function_lowered.projection[1].output);

    var doc_root = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _doc->>'status' AS status, _doc#>>'{metadata,plan}' AS plan FROM docs WHERE _doc->>'status' = 'active' LIMIT 10");
    defer doc_root.deinit(alloc);
    var doc_root_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &doc_root, schema);
    defer doc_root_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), doc_root_lowered.projection.len);
    try std.testing.expectEqualStrings("/status", doc_root_lowered.projection[0].field);
    try std.testing.expectEqualStrings("status", doc_root_lowered.projection[0].output);
    try std.testing.expectEqualStrings("/metadata/plan", doc_root_lowered.projection[1].field);
    try std.testing.expectEqualStrings("plan", doc_root_lowered.projection[1].output);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", doc_root_lowered.producer.indexed_query.filter_query_json.?);

    var doc_root_function = try tokenized.ParsedSql.initAlloc(alloc, "SELECT jsonb_extract_path_text(_doc, 'metadata', 'plan') AS plan FROM docs WHERE jsonb_extract_path_text(_doc, 'status') = 'active' LIMIT 10");
    defer doc_root_function.deinit(alloc);
    var doc_root_function_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &doc_root_function, schema);
    defer doc_root_function_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/plan", doc_root_function_lowered.projection[0].field);
    try std.testing.expectEqualStrings("plan", doc_root_function_lowered.projection[0].output);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", doc_root_function_lowered.producer.indexed_query.filter_query_json.?);

    var doc_root_scan = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _doc->>'category' AS category FROM docs WHERE _doc->>'category' = 'release' LIMIT 10");
    defer doc_root_scan.deinit(alloc);
    var doc_root_scan_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &doc_root_scan, schema, .{ .max_rows = 25 });
    defer doc_root_scan_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/category", doc_root_scan_lowered.projection[0].field);
    try std.testing.expectEqual(@as(u32, 25), doc_root_scan_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", doc_root_scan_lowered.producer.bounded_scan.residual_filter_json.?);
}

test "document SQL lowers full text producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE full_text_search('title:alpha') LIMIT 5");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
}

test "document SQL lowers vector search predicate to native query producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "embedding", .path = "embedding", .field_type = .embedding, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE antfly.vector_search(table_name => 'docs', index => 'docs_embedding_hnsw', vector => '[0.1,0.2,0.3]', limit => 5)");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    const native_query_json = lowered.producer.indexed_query.native_query_json orelse return error.TestUnexpectedResult;
    try std.testing.expect(std.mem.indexOf(u8, native_query_json, "\"embeddings\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, native_query_json, "\"docs_embedding_hnsw\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, native_query_json, "\"indexes\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, native_query_json, "\"limit\":5") != null);

    const no_vector_schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var no_vector = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE antfly.vector_search(table_name => 'docs', index => 'docs_embedding_hnsw', vector => '[0.1,0.2,0.3]', limit => 5)");
    defer no_vector.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentReadPlanParsedSqlAlloc(alloc, &no_vector, no_vector_schema));
}

test "document SQL rejects antfly query functions as scalar predicates" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    const unsupported_predicates = [_][]const u8{
        "SELECT _id FROM docs WHERE antfly.full_text_search('title:alpha') LIMIT 5",
        "SELECT _id FROM docs WHERE antfly.full_text_search(table_name => 'docs', query => 'title:alpha', limit => 5) LIMIT 5",
    };

    for (unsupported_predicates) |sql| {
        var parsed = try tokenized.ParsedSql.initAlloc(alloc, sql);
        defer parsed.deinit(alloc);
        try std.testing.expectError(error.DocumentSqlNativeSearchRequiresTableFunction, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
    }
}

test "document SQL capability-aware lowering requires full text producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE full_text_search('title:alpha') LIMIT 5");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .full_text_filters = false,
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .full_text_filters = true,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
}

test "document SQL capability-aware lowering keeps full text candidate with scalar residual" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE full_text_search('title:alpha') AND status = 'active' LIMIT 5");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .full_text_filters = true,
        .indexed_scalar_filters = false,
    }));

    var lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .full_text_filters = true,
        .indexed_scalar_filters = false,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expect(lowered.producer.indexed_query.filter_query_json == null);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 25), lowered.producer.indexed_query.max_candidate_rows);
}

test "document SQL lowers scalar equality to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 10), lowered.limit);
}

test "document SQL lowers scalar inequality to native exclusion filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "region", .path = "region", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var bang = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status != 'archived' LIMIT 10");
    defer bang.deinit(alloc);
    var bang_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &bang, schema);
    defer bang_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"bool\":{\"must_not\":[{\"term\":{\"path\":\"/status\",\"value\":\"archived\"}}]}}", bang_lowered.producer.indexed_query.filter_query_json.?);

    var angle = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status <> 'archived' AND region = 'west' LIMIT 10");
    defer angle.deinit(alloc);
    var angle_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &angle, schema);
    defer angle_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"bool\":{\"must_not\":[{\"term\":{\"path\":\"/status\",\"value\":\"archived\"}}]}},{\"term\":{\"path\":\"/region\",\"value\":\"west\"}}]}}",
        angle_lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers scalar null predicates to native existence filters" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var is_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IS NULL LIMIT 10");
    defer is_null.deinit(alloc);
    var is_null_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &is_null, schema);
    defer is_null_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"should\":[{\"term\":{\"path\":\"/status\",\"value\":null}},{\"bool\":{\"must_not\":[{\"exists\":{\"path\":\"/status\"}}]}}],\"minimum_should_match\":1}}",
        is_null_lowered.producer.indexed_query.filter_query_json.?,
    );

    var is_not_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IS NOT NULL LIMIT 10");
    defer is_not_null.deinit(alloc);
    var is_not_null_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &is_not_null, schema);
    defer is_not_null_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"exists\":{\"path\":\"/status\"}}],\"must_not\":[{\"term\":{\"path\":\"/status\",\"value\":null}}]}}",
        is_not_null_lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers null equality comparisons to match none" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var equals_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = NULL LIMIT 10");
    defer equals_null.deinit(alloc);
    var equals_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &equals_null, schema);
    defer equals_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", equals_lowered.producer.indexed_query.filter_query_json.?);

    var not_equals_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status != NULL LIMIT 10");
    defer not_equals_null.deinit(alloc);
    var not_equals_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &not_equals_null, schema);
    defer not_equals_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", not_equals_lowered.producer.indexed_query.filter_query_json.?);

    var distinct_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status <> NULL LIMIT 10");
    defer distinct_null.deinit(alloc);
    var distinct_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &distinct_null, schema);
    defer distinct_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", distinct_lowered.producer.indexed_query.filter_query_json.?);

    var conjunctive = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = NULL AND status = 'active' LIMIT 10");
    defer conjunctive.deinit(alloc);
    var conjunctive_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &conjunctive, schema);
    defer conjunctive_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"match_none\":{}},{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}]}}",
        conjunctive_lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers null range and pattern predicates to match none" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var greater = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount > NULL LIMIT 10");
    defer greater.deinit(alloc);
    var greater_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &greater, schema);
    defer greater_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", greater_lowered.producer.indexed_query.filter_query_json.?);

    var like = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE title LIKE NULL LIMIT 10");
    defer like.deinit(alloc);
    var like_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &like, schema);
    defer like_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", like_lowered.producer.indexed_query.filter_query_json.?);

    var between_lower = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount BETWEEN NULL AND 20 LIMIT 10");
    defer between_lower.deinit(alloc);
    var between_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &between_lower, schema);
    defer between_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", between_lowered.producer.indexed_query.filter_query_json.?);

    var between_upper = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount BETWEEN 10 AND NULL LIMIT 10");
    defer between_upper.deinit(alloc);
    var between_upper_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &between_upper, schema);
    defer between_upper_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", between_upper_lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL lowers like predicates to native prefix and wildcard filters" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var exact = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE title LIKE 'alpha' LIMIT 10");
    defer exact.deinit(alloc);
    var exact_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &exact, schema);
    defer exact_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/title\",\"value\":\"alpha\"}}", exact_lowered.producer.indexed_query.filter_query_json.?);

    var prefix = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE title LIKE 'alp%' LIMIT 10");
    defer prefix.deinit(alloc);
    var prefix_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &prefix, schema);
    defer prefix_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"prefix\":{\"path\":\"/title\",\"value\":\"alp\"}}", prefix_lowered.producer.indexed_query.filter_query_json.?);

    var wildcard = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE title LIKE 'a_ph%' LIMIT 10");
    defer wildcard.deinit(alloc);
    var wildcard_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &wildcard, schema);
    defer wildcard_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"wildcard\":{\"path\":\"/title\",\"pattern\":\"a?ph*\"}}", wildcard_lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL rejects unsupported ilike predicates" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE title ILIKE 'alp%' LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}

test "document SQL lowers json path equality to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'status' AS status FROM docs WHERE metadata->>'status' = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/status", lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);

    var nested = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata#>>'{billing,plan}' AS plan FROM docs WHERE metadata#>>'{billing,plan}' = 'annual' LIMIT 10");
    defer nested.deinit(alloc);
    var nested_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &nested, schema);
    defer nested_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/billing/plan", nested_lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/billing/plan\",\"value\":\"annual\"}}", nested_lowered.producer.indexed_query.filter_query_json.?);

    var function_path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, jsonb_extract_path_text(metadata, 'billing', 'plan') AS plan FROM docs WHERE jsonb_extract_path_text(metadata, 'billing', 'plan') = 'annual' ORDER BY jsonb_extract_path_text(metadata, 'billing', 'plan') ASC LIMIT 10");
    defer function_path.deinit(alloc);
    var function_path_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &function_path, schema, .{ .max_rows = 25 });
    defer function_path_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/billing/plan", function_path_lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/billing/plan\",\"value\":\"annual\"}}", function_path_lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expect(function_path_lowered.order_by != null);
    try std.testing.expectEqualStrings("/metadata/billing/plan", function_path_lowered.order_by.?.field);
    try std.testing.expectEqual(DocumentOrderDirection.asc, function_path_lowered.order_by.?.direction);

    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "metadata", .path = "metadata", .source = .index_definition },
        },
    };
    var virtual_root = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'plan' AS plan FROM docs WHERE metadata->>'plan' = 'pro' LIMIT 10");
    defer virtual_root.deinit(alloc);
    var virtual_root_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &virtual_root, .{
        .storage_mode = .document,
    }, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
    });
    defer virtual_root_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/plan", virtual_root_lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/plan\",\"value\":\"pro\"}}", virtual_root_lowered.producer.indexed_query.filter_query_json.?);

    var virtual_function = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, json_extract_path_text(metadata, 'plan') AS plan FROM docs WHERE json_extract_path_text(metadata, 'plan') = 'pro' LIMIT 10");
    defer virtual_function.deinit(alloc);
    var virtual_function_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &virtual_function, .{
        .storage_mode = .document,
    }, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
    });
    defer virtual_function_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/plan", virtual_function_lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/plan\",\"value\":\"pro\"}}", virtual_function_lowered.producer.indexed_query.filter_query_json.?);

    const mapped_view_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "plan", .path = "/metadata/plan", .source = .view_mapping, .field_type = .keyword },
            .{ .name = "score", .path = "/metrics/score", .source = .view_mapping, .field_type = .numeric },
        },
        .typed_paths = &.{
            .{ .path = "/metadata/plan", .field_type = .keyword },
            .{ .path = "/metrics/score", .field_type = .numeric },
        },
        .view_mappings = &.{
            .{
                .name = "support_view",
                .source_table = "docs",
                .required_indexes = 1,
                .required_indexes_ready = true,
                .source_generation_fresh = true,
                .source_schema_fingerprint_fresh = true,
            },
        },
    };
    var mapped_view = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, plan, score FROM support_view WHERE plan = 'pro' ORDER BY score DESC LIMIT 3");
    defer mapped_view.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &mapped_view, .{
        .storage_mode = .document,
    }, mapped_view_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var mapped_view_residual = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 5");
    defer mapped_view_residual.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &mapped_view_residual, .{
        .storage_mode = .document,
    }, mapped_view_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var mapped_view_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &mapped_view, .{
        .storage_mode = .document,
    }, mapped_view_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer mapped_view_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), mapped_view_lowered.projection.len);
    try std.testing.expect(mapped_view_lowered.view_mapping != null);
    try std.testing.expectEqualStrings("support_view", mapped_view_lowered.view_mapping.?.name);
    try std.testing.expectEqualStrings("docs", mapped_view_lowered.view_mapping.?.source_table);
    try std.testing.expectEqual(@as(usize, 1), mapped_view_lowered.view_mapping.?.required_indexes);
    try std.testing.expect(mapped_view_lowered.view_mapping.?.required_indexes_ready);
    try std.testing.expect(mapped_view_lowered.view_mapping.?.source_generation_fresh);
    try std.testing.expect(mapped_view_lowered.view_mapping.?.source_schema_fingerprint_fresh);
    try std.testing.expectEqualStrings("/metadata/plan", mapped_view_lowered.projection[1].field);
    try std.testing.expectEqualStrings("plan", mapped_view_lowered.projection[1].output);
    try std.testing.expectEqualStrings("/metrics/score", mapped_view_lowered.projection[2].field);
    try std.testing.expectEqualStrings("score", mapped_view_lowered.projection[2].output);
    try std.testing.expectEqual(@as(?u32, 3), mapped_view_lowered.limit);
    try std.testing.expect(mapped_view_lowered.order_by != null);
    try std.testing.expectEqualStrings("/metrics/score", mapped_view_lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, mapped_view_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, mapped_view_lowered.order_by.?.direction);
    const mapped_view_indexed = switch (mapped_view_lowered.producer) {
        .indexed_query => |query| query,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/metadata/plan\",\"value\":\"pro\"}}", mapped_view_indexed.filter_query_json.?);
    try std.testing.expectEqual(@as(?u32, 25), mapped_view_indexed.max_candidate_rows);

    const base_path_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "metadata", .path = "metadata", .source = .typed_path_metadata },
            .{ .name = "metrics", .path = "metrics", .source = .typed_path_metadata },
        },
        .typed_paths = &.{
            .{ .path = "/metadata/plan", .field_type = .keyword },
            .{ .path = "/metrics/score", .field_type = .numeric },
        },
    };
    var equivalent_base = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'plan' AS plan, CAST(metrics->>'score' AS numeric) AS score FROM docs WHERE metadata->>'plan' = 'pro' ORDER BY CAST(metrics->>'score' AS numeric) DESC LIMIT 3");
    defer equivalent_base.deinit(alloc);
    var equivalent_base_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &equivalent_base, .{
        .storage_mode = .document,
    }, base_path_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer equivalent_base_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(equivalent_base_lowered.projection[1].field, mapped_view_lowered.projection[1].field);
    try std.testing.expectEqualStrings(equivalent_base_lowered.projection[1].output, mapped_view_lowered.projection[1].output);
    try std.testing.expectEqualStrings(equivalent_base_lowered.projection[2].field, mapped_view_lowered.projection[2].field);
    try std.testing.expectEqualStrings(equivalent_base_lowered.projection[2].output, mapped_view_lowered.projection[2].output);
    const equivalent_base_indexed = switch (equivalent_base_lowered.producer) {
        .indexed_query => |query| query,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqualStrings(equivalent_base_indexed.filter_query_json.?, mapped_view_indexed.filter_query_json.?);
    try std.testing.expectEqual(equivalent_base_indexed.max_candidate_rows, mapped_view_indexed.max_candidate_rows);
    try std.testing.expectEqual(mapped_view_lowered.limit, equivalent_base_lowered.limit);
    try std.testing.expect(equivalent_base_lowered.order_by != null);
    try std.testing.expectEqualStrings(equivalent_base_lowered.order_by.?.field, mapped_view_lowered.order_by.?.field);
    try std.testing.expectEqual(equivalent_base_lowered.order_by.?.field_type, mapped_view_lowered.order_by.?.field_type);
    try std.testing.expectEqual(equivalent_base_lowered.order_by.?.direction, mapped_view_lowered.order_by.?.direction);

    var mapped_view_in = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, plan FROM support_view WHERE plan IN ('pro', 'team') LIMIT 5");
    defer mapped_view_in.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &mapped_view_in, .{
        .storage_mode = .document,
    }, mapped_view_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var mapped_view_range = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, score FROM support_view WHERE score >= 10 LIMIT 5");
    defer mapped_view_range.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanMissingExactProducer, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &mapped_view_range, .{
        .storage_mode = .document,
    }, mapped_view_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var indexed_view = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10");
    defer indexed_view.deinit(alloc);
    var indexed_view_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &indexed_view, .{
        .storage_mode = .document,
    }, mapped_view_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
    });
    defer indexed_view_lowered.deinit(alloc);
    var indexed_base = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'plan' AS plan FROM docs WHERE metadata->>'plan' = 'pro' LIMIT 10");
    defer indexed_base.deinit(alloc);
    var indexed_base_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &indexed_base, .{
        .storage_mode = .document,
    }, base_path_schema, .{
        .indexed_scalar_filter_paths = &.{"/metadata/plan"},
    });
    defer indexed_base_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(indexed_base_lowered.projection[1].field, indexed_view_lowered.projection[1].field);
    try std.testing.expectEqualStrings(indexed_base_lowered.projection[1].output, indexed_view_lowered.projection[1].output);
    try std.testing.expectEqualStrings(indexed_base_lowered.producer.indexed_query.filter_query_json.?, indexed_view_lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL lowers scalar range predicates to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "published_at", .path = "published_at", .field_type = .datetime, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount >= 10 AND status < 'closed' AND published_at <= '2026-01-03T00:00:00Z' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"inclusive_min\":true}},{\"term_range\":{\"path\":\"/status\",\"max\":\"closed\",\"inclusive_max\":false}},{\"date_range\":{\"path\":\"/published_at\",\"end\":\"2026-01-03T00:00:00Z\",\"inclusive_end\":true}}]}}",
        lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers between predicates to native ranges" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "published_at", .path = "published_at", .field_type = .datetime, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount BETWEEN 10 AND 20 AND status BETWEEN 'active' AND 'closed' AND published_at BETWEEN '2026-01-01T00:00:00Z' AND '2026-01-31T00:00:00Z' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"max\":20,\"inclusive_min\":true,\"inclusive_max\":true}},{\"term_range\":{\"path\":\"/status\",\"min\":\"active\",\"max\":\"closed\",\"inclusive_min\":true,\"inclusive_max\":true}},{\"date_range\":{\"path\":\"/published_at\",\"start\":\"2026-01-01T00:00:00Z\",\"end\":\"2026-01-31T00:00:00Z\",\"inclusive_start\":true,\"inclusive_end\":true}}]}}",
        lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers declared json path range predicates to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json },
            .{ .name = "metadata_score", .path = "metadata/score", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metadata->>'score' AS score FROM docs WHERE metadata->>'score' > 7 LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metadata/score", lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metadata/score\",\"min\":7,\"inclusive_min\":false}}", lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL rejects untyped json subtree range predicates" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "metadata", .path = "metadata", .field_type = .json, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE metadata->>'score' > 7 LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 }));
}

test "document SQL lowers unindexed scalar predicate to policy bounded residual scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 3");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25, .max_bytes = 4096 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqual(@as(?u64, 4096), lowered.producer.bounded_scan.max_bytes);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 3), lowered.limit);
}

test "document SQL scans scalar predicates when only generic full text index exists" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    const capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        schema,
        "{\"fts\":{\"type\":\"full_text\"}}",
    );
    defer {
        var mutable_capabilities = capabilities;
        source_binding.deinitDocumentSqlCapabilities(alloc, &mutable_capabilities);
    }
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 3");
    defer parsed.deinit(alloc);

    var lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, capabilities);
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, source_binding.default_document_sql_bounded_scan_rows), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 3), lowered.limit);
}

test "document SQL external row filters constrain every read producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    const row_filter = "{\"term\":{\"tenant_id\":\"tenant-a\"}}";

    var lookup_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND status = 'active'");
    defer lookup_sql.deinit(alloc);
    var lookup = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &lookup_sql, schema);
    defer lookup.deinit(alloc);
    try applyDocumentReadPlanFilterConstraintAlloc(alloc, &lookup, row_filter);
    try std.testing.expectEqualStrings(
        "{\"conjuncts\":[{\"term\":{\"path\":\"/status\",\"value\":\"active\"}},{\"term\":{\"tenant_id\":\"tenant-a\"}}]}",
        lookup.producer.id_lookup.residual_filter_json.?,
    );

    var indexed_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active'");
    defer indexed_sql.deinit(alloc);
    var indexed = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &indexed_sql, schema);
    defer indexed.deinit(alloc);
    try applyDocumentReadPlanFilterConstraintAlloc(alloc, &indexed, row_filter);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        indexed.producer.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"term\":{\"tenant_id\":\"tenant-a\"}}",
        indexed.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, source_binding.default_document_sql_bounded_scan_rows), indexed.producer.indexed_query.max_candidate_rows);

    var scan_sql = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' LIMIT 5");
    defer scan_sql.deinit(alloc);
    var scan = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &scan_sql, schema, .{
        .indexed_scalar_filters = false,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer scan.deinit(alloc);
    try applyDocumentReadPlanFilterConstraintAlloc(alloc, &scan, row_filter);
    try std.testing.expectEqualStrings(
        "{\"conjuncts\":[{\"term\":{\"path\":\"/status\",\"value\":\"active\"}},{\"term\":{\"tenant_id\":\"tenant-a\"}}]}",
        scan.producer.bounded_scan.residual_filter_json.?,
    );
}

test "document SQL external row filters constrain aggregate candidate producers" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs WHERE status = 'active'");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
        .indexed_scalar_filters = true,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);

    try applyDocumentAggregatePlanFilterConstraintAlloc(alloc, &lowered, "{\"term\":{\"tenant_id\":\"tenant-a\"}}");
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        lowered.candidate_producer.?.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"term\":{\"tenant_id\":\"tenant-a\"}}",
        lowered.candidate_producer.?.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"conjuncts\":[{\"term\":{\"path\":\"/status\",\"value\":\"active\"}},{\"term\":{\"tenant_id\":\"tenant-a\"}}]}",
        lowered.filter_query_json.?,
    );
}

test "document SQL lowers between predicate to bounded residual scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount BETWEEN 10 AND 20 LIMIT 3");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"max\":20,\"inclusive_min\":true,\"inclusive_max\":true}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 3), lowered.limit);
}

test "document SQL lowers scalar inequality to policy bounded residual scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status != 'archived' LIMIT 3");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"bool\":{\"must_not\":[{\"term\":{\"path\":\"/status\",\"value\":\"archived\"}}]}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 3), lowered.limit);
}

test "document SQL lowers scalar null predicate to policy bounded residual scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IS NULL LIMIT 3");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"should\":[{\"term\":{\"path\":\"/status\",\"value\":null}},{\"bool\":{\"must_not\":[{\"exists\":{\"path\":\"/status\"}}]}}],\"minimum_should_match\":1}}",
        lowered.producer.bounded_scan.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 3), lowered.limit);
}

test "document SQL lowers null equality comparisons to policy bounded residual scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = NULL LIMIT 3");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 3), lowered.limit);

    var distinct = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status <> NULL LIMIT 3");
    defer distinct.deinit(alloc);
    var distinct_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &distinct, schema, .{ .max_rows = 25 });
    defer distinct_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), distinct_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", distinct_lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 3), distinct_lowered.limit);
}

test "document SQL lowers null range and pattern predicates to policy bounded residual scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = false },
        },
    };
    var greater = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount > NULL LIMIT 3");
    defer greater.deinit(alloc);
    var greater_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &greater, schema, .{ .max_rows = 25 });
    defer greater_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), greater_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", greater_lowered.producer.bounded_scan.residual_filter_json.?);

    var like = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs WHERE title LIKE NULL LIMIT 3");
    defer like.deinit(alloc);
    var like_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &like, schema, .{ .max_rows = 25 });
    defer like_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), like_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", like_lowered.producer.bounded_scan.residual_filter_json.?);

    var between = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, amount FROM docs WHERE amount BETWEEN 10 AND NULL LIMIT 3");
    defer between.deinit(alloc);
    var between_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &between, schema, .{ .max_rows = 25 });
    defer between_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), between_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", between_lowered.producer.bounded_scan.residual_filter_json.?);
}

test "document SQL fail closes unsupported residual expression shapes under bounded scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };

    var corpus = try document_sql_corpus.parseDocumentSqlCorpusAlloc(alloc);
    defer corpus.deinit();
    for (corpus.value.unsupported_residual_expression_cases) |case| {
        errdefer std.debug.print("document SQL unsupported residual expression corpus case failed: {s}\n", .{case.name});
        var parsed = try tokenized.ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        try std.testing.expectError(
            document_sql_corpus.errorValue(try document_sql_corpus.errorFromName(case.expected_error)),
            lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 }),
        );
    }
}

fn documentSqlCorpusReadPlanSchema(name: []const u8) !runtime_schema.TableSchema {
    if (std.mem.eql(u8, name, "vector_ready") or std.mem.eql(u8, name, "semantic_ready")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "embedding", .path = "embedding", .field_type = .embedding, .indexed = true, .index_lifecycle = .ready },
            },
        };
    }
    if (std.mem.eql(u8, name, "text_only")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "title", .path = "title", .field_type = .text },
            },
        };
    }
    if (std.mem.eql(u8, name, "default_generated")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "status", .path = "status", .field_type = .keyword, .default_value = .{ .kind = .literal, .value_json = "\"new\"" } },
            },
        };
    }
    if (std.mem.eql(u8, name, "vector_with_status")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "embedding", .path = "embedding", .field_type = .embedding, .indexed = true, .index_lifecycle = .ready },
                .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
            },
        };
    }
    if (std.mem.eql(u8, name, "aggregate_ready")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "body", .path = "body", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
                .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
                .{ .name = "category", .path = "category", .field_type = .keyword, .indexed = false },
                .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = false },
            },
        };
    }
    if (std.mem.eql(u8, name, "array_ready")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            },
        };
    }
    if (std.mem.eql(u8, name, "array_unindexed")) {
        return .{
            .storage_mode = .document,
            .relational_columns = &.{
                .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword, .indexed = false },
            },
        };
    }
    return error.InvalidSqlCorpusFixture;
}

const document_sql_corpus_fixture_source_generation: u64 = 7;
const document_sql_corpus_fixture_source_schema_fingerprint = "fixture-schema-v1";

fn lowerDocumentReadPlanForCorpusCaseAlloc(
    alloc: std.mem.Allocator,
    parsed: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    case: document_sql_corpus.DocumentSqlReadPlanCaseJson,
) !DocumentReadPlan {
    if (case.indexes_json) |indexes_json| {
        var capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, indexes_json, document_sql_corpus_fixture_source_generation, document_sql_corpus_fixture_source_schema_fingerprint);
        defer source_binding.deinitDocumentSqlCapabilities(alloc, &capabilities);
        var virtual_schema = try source_binding.documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, indexes_json, "docs", document_sql_corpus_fixture_source_generation, document_sql_corpus_fixture_source_schema_fingerprint);
        defer source_binding.deinitDocumentSqlSchema(alloc, &virtual_schema);
        if (case.bounded_scan_rows) |max_rows| capabilities.bounded_scan = .{ .max_rows = max_rows };
        return try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, parsed, schema, virtual_schema, capabilities);
    }
    if (case.bounded_scan_rows) |max_rows| {
        return try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, parsed, schema, .{ .max_rows = max_rows });
    }
    return try lowerDocumentReadPlanParsedSqlAlloc(alloc, parsed, schema);
}

fn lowerDocumentAggregatePlanForCorpusCaseAlloc(
    alloc: std.mem.Allocator,
    parsed: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    case: document_sql_corpus.DocumentSqlReadPlanCaseJson,
) !DocumentAlgebraicAggregatePlan {
    if (case.indexes_json) |indexes_json| {
        var capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, indexes_json, document_sql_corpus_fixture_source_generation, document_sql_corpus_fixture_source_schema_fingerprint);
        defer source_binding.deinitDocumentSqlCapabilities(alloc, &capabilities);
        var virtual_schema = try source_binding.documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, indexes_json, "docs", document_sql_corpus_fixture_source_generation, document_sql_corpus_fixture_source_schema_fingerprint);
        defer source_binding.deinitDocumentSqlSchema(alloc, &virtual_schema);
        if (case.bounded_scan_rows) |max_rows| capabilities.bounded_scan = .{ .max_rows = max_rows };
        return try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, parsed, schema, virtual_schema, indexes_json, capabilities);
    }
    if (case.bounded_scan_rows) |max_rows| {
        return try lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, parsed, schema, null, .{ .max_rows = max_rows });
    }
    return try lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, parsed, schema, null);
}

fn expectDocumentSqlCorpusAggregateProducer(
    case: document_sql_corpus.DocumentSqlReadPlanCaseJson,
    producer: DocumentProducer,
) !void {
    const expected = case.expected.producer orelse return error.InvalidSqlCorpusFixture;
    if (std.mem.eql(u8, expected, "indexed_filter")) {
        const indexed_query = switch (producer) {
            .indexed_query => |query| query,
            else => return error.TestUnexpectedResult,
        };
        if (case.expected.filter_query_contains.len > 0) {
            const filter_query_json = indexed_query.filter_query_json orelse return error.TestUnexpectedResult;
            for (case.expected.filter_query_contains) |needle| {
                try std.testing.expect(std.mem.indexOf(u8, filter_query_json, needle) != null);
            }
        }
        if (case.expected.residual_filter_json) |expected_json| {
            try std.testing.expectEqualStrings(expected_json, indexed_query.residual_filter_json orelse return error.TestUnexpectedResult);
        }
        if (case.expected.residual_filter_contains.len > 0) {
            const residual_filter_json = indexed_query.residual_filter_json orelse return error.TestUnexpectedResult;
            for (case.expected.residual_filter_contains) |needle| {
                try std.testing.expect(std.mem.indexOf(u8, residual_filter_json, needle) != null);
            }
        }
        if (case.expected.max_candidate_rows) |max_candidate_rows| {
            try std.testing.expectEqual(max_candidate_rows, indexed_query.max_candidate_rows orelse return error.TestUnexpectedResult);
        }
    } else if (std.mem.eql(u8, expected, "bounded_scan")) {
        const bounded_scan = switch (producer) {
            .bounded_scan => |scan| scan,
            else => return error.TestUnexpectedResult,
        };
        if (case.expected.residual_filter_json) |expected_json| {
            try std.testing.expectEqualStrings(expected_json, bounded_scan.residual_filter_json orelse return error.TestUnexpectedResult);
        }
        for (case.expected.residual_filter_contains) |needle| {
            const residual_filter_json = bounded_scan.residual_filter_json orelse return error.TestUnexpectedResult;
            try std.testing.expect(std.mem.indexOf(u8, residual_filter_json, needle) != null);
        }
        if (case.expected.max_candidate_rows) |max_rows| {
            try std.testing.expectEqual(max_rows, bounded_scan.max_rows);
        }
    } else {
        return error.InvalidSqlCorpusFixture;
    }
}

fn expectDocumentSqlCorpusAggregatePlan(
    case: document_sql_corpus.DocumentSqlReadPlanCaseJson,
    lowered: DocumentAlgebraicAggregatePlan,
) !void {
    const expected_op = case.expected.aggregate_op orelse return error.InvalidSqlCorpusFixture;
    try std.testing.expectEqualStrings(expected_op, @tagName(lowered.aggregate.op));
    if (case.expected.aggregate_input) |expected_input| {
        const input = lowered.aggregate.input orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(expected_input, input.field);
    }
    if (case.expected.group_by) |expected_group| {
        const group_by = lowered.group_by orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(expected_group, group_by.field);
    }
    if (case.expected.having) |expected_having| {
        try std.testing.expectEqual(expected_having, lowered.having.len);
    }
    if (case.expected.order_by) |expected_order| {
        const order_by = lowered.order_by orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(expected_order, @tagName(order_by.key));
    }
    if (case.expected.limit) |expected_limit| {
        try std.testing.expectEqual(expected_limit, lowered.limit orelse return error.TestUnexpectedResult);
    }
    const expected_producer = case.expected.producer orelse return error.InvalidSqlCorpusFixture;
    if (std.mem.eql(u8, expected_producer, "algebraic_materialization")) {
        try std.testing.expect(lowered.index_name != null);
        try std.testing.expect(lowered.materialization_name != null);
        try std.testing.expect(lowered.candidate_producer == null);
    } else {
        try expectDocumentSqlCorpusAggregateProducer(case, lowered.candidate_producer orelse return error.TestUnexpectedResult);
    }
}

test "document SQL read plan corpus cases" {
    const alloc = std.testing.allocator;
    var corpus = try document_sql_corpus.parseDocumentSqlCorpusAlloc(alloc);
    defer corpus.deinit();
    for (corpus.value.document_read_plan_cases) |case| {
        errdefer std.debug.print("document SQL read plan corpus case failed: {s}\n", .{case.name});
        const schema = try documentSqlCorpusReadPlanSchema(case.schema);
        var parsed = try tokenized.ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        const statement_kind = try validatedDocumentReadStatementKind(&parsed);
        if (case.expected.expected_error) |expected_error_name| {
            try std.testing.expect(case.expected.producer == null);
            try std.testing.expect(case.expected.native_query_json == null);
            try std.testing.expect(case.expected.residual_filter_json == null);
            try std.testing.expect(case.expected.filter_query_contains.len == 0);
            try std.testing.expect(case.expected.native_query_contains.len == 0);
            try std.testing.expect(case.expected.residual_filter_contains.len == 0);
            if (statement_kind == .aggregate) {
                try std.testing.expectError(
                    document_sql_corpus.errorValue(try document_sql_corpus.errorFromName(expected_error_name)),
                    lowerDocumentAggregatePlanForCorpusCaseAlloc(alloc, &parsed, schema, case),
                );
            } else {
                try std.testing.expectError(
                    document_sql_corpus.errorValue(try document_sql_corpus.errorFromName(expected_error_name)),
                    lowerDocumentReadPlanForCorpusCaseAlloc(alloc, &parsed, schema, case),
                );
            }
            continue;
        }

        if (statement_kind == .aggregate) {
            var lowered_aggregate = try lowerDocumentAggregatePlanForCorpusCaseAlloc(alloc, &parsed, schema, case);
            defer lowered_aggregate.deinit(alloc);
            try expectDocumentSqlCorpusAggregatePlan(case, lowered_aggregate);
            continue;
        }

        var lowered = try lowerDocumentReadPlanForCorpusCaseAlloc(alloc, &parsed, schema, case);
        defer lowered.deinit(alloc);
        const producer = case.expected.producer orelse return error.InvalidSqlCorpusFixture;
        if (std.mem.eql(u8, producer, "native_query")) {
            const indexed_query = switch (lowered.producer) {
                .indexed_query => |query| query,
                else => return error.TestUnexpectedResult,
            };
            const native_query_json = indexed_query.native_query_json orelse return error.TestUnexpectedResult;
            if (case.expected.native_query_json) |expected_json| {
                try std.testing.expectEqualStrings(expected_json, native_query_json);
            }
            for (case.expected.native_query_contains) |needle| {
                try std.testing.expect(std.mem.indexOf(u8, native_query_json, needle) != null);
            }
            if (case.expected.residual_filter_contains.len > 0) {
                const residual_filter_json = indexed_query.residual_filter_json orelse return error.TestUnexpectedResult;
                for (case.expected.residual_filter_contains) |needle| {
                    try std.testing.expect(std.mem.indexOf(u8, residual_filter_json, needle) != null);
                }
            }
            if (case.expected.residual_filter_json) |expected_json| {
                try std.testing.expectEqualStrings(expected_json, indexed_query.residual_filter_json orelse return error.TestUnexpectedResult);
            }
            if (case.expected.max_candidate_rows) |max_candidate_rows| {
                try std.testing.expectEqual(max_candidate_rows, indexed_query.max_candidate_rows orelse return error.TestUnexpectedResult);
            }
        } else if (std.mem.eql(u8, producer, "indexed_filter")) {
            const indexed_query = switch (lowered.producer) {
                .indexed_query => |query| query,
                else => return error.TestUnexpectedResult,
            };
            const filter_query_json = indexed_query.filter_query_json orelse return error.TestUnexpectedResult;
            for (case.expected.filter_query_contains) |needle| {
                try std.testing.expect(std.mem.indexOf(u8, filter_query_json, needle) != null);
            }
        } else if (std.mem.eql(u8, producer, "bounded_scan")) {
            const bounded_scan = switch (lowered.producer) {
                .bounded_scan => |scan| scan,
                else => return error.TestUnexpectedResult,
            };
            const residual_filter_json = bounded_scan.residual_filter_json orelse return error.TestUnexpectedResult;
            for (case.expected.residual_filter_contains) |needle| {
                try std.testing.expect(std.mem.indexOf(u8, residual_filter_json, needle) != null);
            }
        } else {
            return error.InvalidSqlCorpusFixture;
        }
    }
}

test "document SQL read plan corpus cases fail closed on retained generated ast corruption" {
    const alloc = std.testing.allocator;
    var corpus = try document_sql_corpus.parseDocumentSqlCorpusAlloc(alloc);
    defer corpus.deinit();
    for (corpus.value.document_read_plan_cases) |case| {
        errdefer std.debug.print("document SQL retained AST corpus case failed: {s}\n", .{case.name});
        const schema = try documentSqlCorpusReadPlanSchema(case.schema);
        var parsed = try tokenized.ParsedSql.initAlloc(alloc, case.sql);
        defer parsed.deinit(alloc);
        if (parsed.generated_statement) |*generated_statement| {
            if (generated_statement.ast) |*generated_ast| switch (generated_ast.*) {
                .read => |read| read.source_tokens = null,
                else => return error.TestUnexpectedResult,
            } else return error.TestUnexpectedResult;
        } else return error.TestUnexpectedResult;
        try std.testing.expectError(
            error.UnsupportedSqlShape,
            lowerDocumentReadPlanForCorpusCaseAlloc(alloc, &parsed, schema, case),
        );
    }
}

test "document SQL keeps indexed scalar producer when bounded scan policy is present" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL requires explicit limit for policy-backed indexed reads" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active';");
    defer parsed.deinit(alloc);
    try std.testing.expectError(
        error.DocumentSqlBoundedScanIncompleteTopK,
        lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 }),
    );
}

test "document SQL capability-aware lowering scans when scalar index capability is absent" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .indexed_scalar_filters = false,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.bounded_scan.residual_filter_json.?);
}

test "document SQL capability-aware lowering pushes only proven scalar paths" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
            .{ .name = "category", .path = "category", .field_type = .keyword, .indexed = false },
        },
    };
    const capabilities = source_binding.DocumentSqlCapabilities{
        .indexed_scalar_filter_paths = &.{"/status"},
        .bounded_scan = .{ .max_rows = 25 },
    };

    var indexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;");
    defer indexed.deinit(alloc);
    var indexed_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &indexed, schema, capabilities);
    defer indexed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", indexed_lowered.producer.indexed_query.filter_query_json.?);

    var unindexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, category FROM docs WHERE category = 'release' LIMIT 10;");
    defer unindexed.deinit(alloc);
    var unindexed_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &unindexed, schema, capabilities);
    defer unindexed_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), unindexed_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", unindexed_lowered.producer.bounded_scan.residual_filter_json.?);

    var mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' AND category = 'release' LIMIT 10;");
    defer mixed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &mixed, schema, .{
        .indexed_scalar_filter_paths = &.{"/status"},
    }));

    var mixed_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &mixed, schema, capabilities);
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", mixed_lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", mixed_lowered.producer.indexed_query.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 25), mixed_lowered.producer.indexed_query.max_candidate_rows);
}

test "document SQL filters on index-backed virtual scalar fields" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "status", .path = "status", .source = .declared_schema },
            .{ .name = "category", .path = "category", .source = .index_definition },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, category FROM docs WHERE category = 'release' LIMIT 10;");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/category"},
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqualStrings("category", lowered.projection[1].output);
    try std.testing.expectEqualStrings("category", lowered.projection[1].field);
}

test "document SQL combines typed virtual scalar fields with independent index readiness" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "status", .path = "status", .source = .declared_schema, .field_type = .keyword },
            .{ .name = "score", .path = "score", .source = .typed_path_metadata, .field_type = .numeric },
            .{ .name = "metrics", .path = "metrics", .source = .typed_path_metadata },
        },
        .typed_paths = &.{
            .{ .path = "/score", .field_type = .numeric },
            .{ .path = "/metrics/score", .field_type = .numeric },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, score FROM docs WHERE score >= 7 LIMIT 10;");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, .{
        // Readiness is supplied by a real producer capability, not by typed_paths.
        .indexed_scalar_filter_paths = &.{"/score"},
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/score\",\"min\":7,\"inclusive_min\":true}}", lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expectEqualStrings("score", lowered.projection[1].output);
    try std.testing.expectEqualStrings("score", lowered.projection[1].field);

    var nested = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metrics->>'score' AS score FROM docs WHERE metrics->>'score' >= 7 LIMIT 10;");
    defer nested.deinit(alloc);
    var nested_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &nested, schema, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metrics/score"},
    });
    defer nested_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metrics/score", nested_lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", nested_lowered.producer.indexed_query.filter_query_json.?);

    var function_path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, jsonb_extract_path_text(metrics, 'score') AS score FROM docs WHERE jsonb_extract_path_text(metrics, 'score') >= 7 LIMIT 10;");
    defer function_path.deinit(alloc);
    var function_path_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &function_path, schema, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metrics/score"},
    });
    defer function_path_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metrics/score", function_path_lowered.projection[1].field);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", function_path_lowered.producer.indexed_query.filter_query_json.?);

    var cast_path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, CAST(metrics->>'score' AS numeric) AS score FROM docs WHERE CAST(metrics->>'score' AS numeric) >= 7 ORDER BY CAST(metrics->>'score' AS numeric) DESC LIMIT 10;");
    defer cast_path.deinit(alloc);
    var cast_path_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &cast_path, schema, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metrics/score"},
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer cast_path_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("/metrics/score", cast_path_lowered.projection[1].field);
    try std.testing.expectEqualStrings("score", cast_path_lowered.projection[1].output);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", cast_path_lowered.producer.indexed_query.filter_query_json.?);
    try std.testing.expect(cast_path_lowered.order_by != null);
    try std.testing.expectEqualStrings("/metrics/score", cast_path_lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, cast_path_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, cast_path_lowered.order_by.?.direction);

    var wrong_cast = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE CAST(metrics->>'score' AS text) = '7' LIMIT 10;");
    defer wrong_cast.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &wrong_cast, schema, virtual_schema, .{
        .indexed_scalar_filter_paths = &.{"/metrics/score"},
    }));
}

test "document SQL typed paths prove scalar type but not index readiness" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    const virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{ .name = "metrics", .path = "metrics", .source = .typed_path_metadata },
        },
        .typed_paths = &.{
            .{ .path = "/metrics/score", .field_type = .numeric },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, metrics->>'score' AS score FROM docs WHERE metrics->>'score' >= 7 LIMIT 10;");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, .{}));

    var lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqualStrings("/metrics/score", lowered.projection[1].field);

    var function_path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, jsonb_extract_path_text(metrics, 'score') AS score FROM docs WHERE jsonb_extract_path_text(metrics, 'score') >= 7 LIMIT 10;");
    defer function_path.deinit(alloc);
    var function_path_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &function_path, schema, virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer function_path_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), function_path_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", function_path_lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqualStrings("/metrics/score", function_path_lowered.projection[1].field);

    var cast_path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, CAST(metrics->>'score' AS numeric) AS score FROM docs WHERE CAST(metrics->>'score' AS numeric) >= 7 LIMIT 10;");
    defer cast_path.deinit(alloc);
    var cast_path_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &cast_path, schema, virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer cast_path_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), cast_path_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", cast_path_lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqualStrings("/metrics/score", cast_path_lowered.projection[1].field);

    var aggregate = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY metrics->>'score' LIMIT 10;");
    defer aggregate.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &aggregate, schema, virtual_schema, null, .{}));

    var aggregate_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &aggregate, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer aggregate_lowered.deinit(alloc);
    try std.testing.expect(aggregate_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), aggregate_lowered.candidate_producer.?.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("/metrics/score", aggregate_lowered.group_by.?.field);

    var cast_aggregate = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY CAST(metrics->>'score' AS numeric) LIMIT 10;");
    defer cast_aggregate.deinit(alloc);
    var cast_aggregate_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &cast_aggregate, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer cast_aggregate_lowered.deinit(alloc);
    try std.testing.expect(cast_aggregate_lowered.candidate_producer != null);
    try std.testing.expectEqual(@as(u32, 25), cast_aggregate_lowered.candidate_producer.?.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("/metrics/score", cast_aggregate_lowered.group_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, cast_aggregate_lowered.group_by.?.field_type);

    var ordered_aggregate = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY metrics->>'score' ORDER BY metrics->>'score' DESC LIMIT 10;");
    defer ordered_aggregate.deinit(alloc);
    var ordered_aggregate_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &ordered_aggregate, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer ordered_aggregate_lowered.deinit(alloc);
    try std.testing.expect(ordered_aggregate_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, ordered_aggregate_lowered.order_by.?.key);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, ordered_aggregate_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, ordered_aggregate_lowered.order_by.?.direction);

    var default_group_output = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY metrics->>'score' HAVING group >= 7 ORDER BY group DESC LIMIT 10;");
    defer default_group_output.deinit(alloc);
    var default_group_output_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &default_group_output, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer default_group_output_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("group", default_group_output_lowered.group_by.?.output);
    try std.testing.expectEqual(@as(usize, 1), default_group_output_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, default_group_output_lowered.having[0].key);
    try std.testing.expectEqualStrings("7", default_group_output_lowered.having[0].value_json);
    try std.testing.expect(default_group_output_lowered.order_by != null);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, default_group_output_lowered.order_by.?.key);
    try std.testing.expectEqual(DocumentOrderDirection.desc, default_group_output_lowered.order_by.?.direction);

    var having_aggregate = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY metrics->>'score' HAVING metrics->>'score' >= 7 LIMIT 10;");
    defer having_aggregate.deinit(alloc);
    var having_aggregate_lowered = try lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &having_aggregate, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer having_aggregate_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), having_aggregate_lowered.having.len);
    try std.testing.expectEqual(DocumentAggregateOrderKey.group, having_aggregate_lowered.having[0].key);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, having_aggregate_lowered.having[0].field_type);
    try std.testing.expectEqual(DocumentAggregateHavingOp.gte, having_aggregate_lowered.having[0].op);
    try std.testing.expectEqualStrings("7", having_aggregate_lowered.having[0].value_json);

    var wrong_order_expression = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY metrics->>'score' ORDER BY status DESC LIMIT 10;");
    defer wrong_order_expression.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &wrong_order_expression, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var wrong_having_expression = try tokenized.ParsedSql.initAlloc(alloc, "SELECT count(*) AS row_count FROM docs GROUP BY metrics->>'score' HAVING status = 'active' LIMIT 10;");
    defer wrong_having_expression.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlAggregateUnsupported, lowerDocumentAggregatePlanWithOptionalIndexesAndVirtualSchemaCapabilitiesParsedSqlAlloc(alloc, &wrong_having_expression, schema, virtual_schema, null, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));
}

test "document SQL treats field-scoped full text index as scalar-capable for that path" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "body", .path = "body", .field_type = .text, .indexed = false },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };
    var capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        schema,
        "{\"body_fts\":{\"type\":\"full_text\",\"field\":\"body\"}}",
    );
    defer source_binding.deinitDocumentSqlCapabilities(alloc, &capabilities);

    var covered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, body FROM docs WHERE body LIKE 'alpha%' LIMIT 10;");
    defer covered.deinit(alloc);
    var covered_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &covered, schema, capabilities);
    defer covered_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"prefix\":{\"path\":\"/body\",\"value\":\"alpha\"}}", covered_lowered.producer.indexed_query.filter_query_json.?);

    var uncovered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;");
    defer uncovered.deinit(alloc);
    var uncovered_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &uncovered, schema, capabilities);
    defer uncovered_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, source_binding.default_document_sql_bounded_scan_rows), uncovered_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", uncovered_lowered.producer.bounded_scan.residual_filter_json.?);
}

test "document SQL selects compatible full text producer by query field" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "body", .path = "body", .field_type = .text, .indexed = false },
            .{ .name = "category", .path = "category", .field_type = .keyword, .indexed = false },
        },
    };
    var capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        schema,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"category_fts\":{\"type\":\"full_text\",\"field\":\"category\"},\"body_fts\":{\"type\":\"full_text\",\"field\":\"body\"}}",
    );
    defer source_binding.deinitDocumentSqlCapabilities(alloc, &capabilities);

    var body_query = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE full_text_search('body:alpha') LIMIT 10;");
    defer body_query.deinit(alloc);
    var body_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &body_query, schema, capabilities);
    defer body_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("body_fts", body_lowered.producer.indexed_query.index_name.?);

    var category_query = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE full_text_search('category:release') LIMIT 10;");
    defer category_query.deinit(alloc);
    var category_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &category_query, schema, capabilities);
    defer category_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("category_fts", category_lowered.producer.indexed_query.index_name.?);

    var title_query = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE full_text_search('title:alpha') LIMIT 10;");
    defer title_query.deinit(alloc);
    var title_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &title_query, schema, capabilities);
    defer title_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("full_text_index_v0", title_lowered.producer.indexed_query.index_name.?);
}

test "document SQL scalar index capability still requires field-level readiness" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "category", .path = "category", .field_type = .keyword, .indexed = false },
        },
    };

    var indexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10");
    defer indexed.deinit(alloc);
    var indexed_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &indexed, schema);
    defer indexed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", indexed_lowered.producer.indexed_query.filter_query_json.?);

    var unindexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, category FROM docs WHERE category = 'release' LIMIT 10");
    defer unindexed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &unindexed, schema));

    var unindexed_scan = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &unindexed, schema, .{ .max_rows = 25 });
    defer unindexed_scan.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), unindexed_scan.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", unindexed_scan.producer.bounded_scan.residual_filter_json.?);
}

test "document SQL lowers ASCII case-fold text predicates as residual-only filters" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "category", .path = "category", .field_type = .keyword, .indexed = false },
        },
    };

    var lookup = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND lower(status) = 'active' LIMIT 10");
    defer lookup.deinit(alloc);
    var lookup_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &lookup, schema);
    defer lookup_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), lookup_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings(
        "{\"text_lower_term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        lookup_lowered.producer.id_lookup.residual_filter_json.?,
    );

    var scan = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE lower(category) = 'release' LIMIT 10");
    defer scan.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &scan, schema));

    var scan_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &scan, schema, .{ .max_rows = 25 });
    defer scan_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), scan_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings(
        "{\"text_lower_term\":{\"path\":\"/category\",\"value\":\"release\"}}",
        scan_lowered.producer.bounded_scan.residual_filter_json.?,
    );

    var mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' AND lower(category) = 'release' LIMIT 10");
    defer mixed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &mixed, schema));

    var mixed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &mixed, schema, .{ .max_rows = 25 });
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        mixed_lowered.producer.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"text_lower_term\":{\"path\":\"/category\",\"value\":\"release\"}}",
        mixed_lowered.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 25), mixed_lowered.producer.indexed_query.max_candidate_rows);

    var upper_lookup = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND upper(status) = 'ACTIVE' LIMIT 10");
    defer upper_lookup.deinit(alloc);
    var upper_lookup_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &upper_lookup, schema);
    defer upper_lookup_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), upper_lookup_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings(
        "{\"text_upper_term\":{\"path\":\"/status\",\"value\":\"ACTIVE\"}}",
        upper_lookup_lowered.producer.id_lookup.residual_filter_json.?,
    );

    var upper_mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' AND upper(category) = 'RELEASE' LIMIT 10");
    defer upper_mixed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &upper_mixed, schema));

    var upper_mixed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &upper_mixed, schema, .{ .max_rows = 25 });
    defer upper_mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        upper_mixed_lowered.producer.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"text_upper_term\":{\"path\":\"/category\",\"value\":\"RELEASE\"}}",
        upper_mixed_lowered.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 25), upper_mixed_lowered.producer.indexed_query.max_candidate_rows);
}

test "document SQL case-fold text predicate rejects unsupported types and non-ASCII literals" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };

    var numeric = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE lower(amount) = '10' LIMIT 10");
    defer numeric.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &numeric, schema, .{ .max_rows = 25 }));

    var non_ascii = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE lower(status) = 'caf\xc3\xa9' LIMIT 10");
    defer non_ascii.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &non_ascii, schema, .{ .max_rows = 25 }));

    var upper_numeric = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE upper(amount) = '10' LIMIT 10");
    defer upper_numeric.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &upper_numeric, schema, .{ .max_rows = 25 }));

    var upper_non_ascii = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE upper(status) = 'CAF\xc3\x89' LIMIT 10");
    defer upper_non_ascii.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &upper_non_ascii, schema, .{ .max_rows = 25 }));
}

test "document SQL lowers numeric abs predicate as residual-only filter" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "delta", .path = "delta", .field_type = .numeric, .indexed = false },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };

    var lookup = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND abs(delta) <= 10 LIMIT 10");
    defer lookup.deinit(alloc);
    var lookup_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &lookup, schema);
    defer lookup_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), lookup_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings(
        "{\"numeric_abs_range\":{\"path\":\"/delta\",\"max\":10,\"inclusive_max\":true}}",
        lookup_lowered.producer.id_lookup.residual_filter_json.?,
    );

    var scan = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE abs(delta) = 7 LIMIT 10");
    defer scan.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &scan, schema));

    var scan_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &scan, schema, .{ .max_rows = 25 });
    defer scan_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), scan_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings(
        "{\"numeric_abs_range\":{\"path\":\"/delta\",\"min\":7,\"max\":7,\"inclusive_min\":true,\"inclusive_max\":true}}",
        scan_lowered.producer.bounded_scan.residual_filter_json.?,
    );

    var mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE amount >= 10 AND abs(delta) > 5 LIMIT 10");
    defer mixed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &mixed, schema));

    var mixed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &mixed, schema, .{ .max_rows = 25 });
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"numeric_range\":{\"path\":\"/amount\",\"min\":10,\"inclusive_min\":true}}",
        mixed_lowered.producer.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"numeric_abs_range\":{\"path\":\"/delta\",\"min\":5,\"inclusive_min\":false}}",
        mixed_lowered.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 25), mixed_lowered.producer.indexed_query.max_candidate_rows);

    var not_equal = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND abs(delta) != 7 LIMIT 10");
    defer not_equal.deinit(alloc);
    var not_equal_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &not_equal, schema);
    defer not_equal_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must_not\":[{\"numeric_abs_range\":{\"path\":\"/delta\",\"min\":7,\"max\":7,\"inclusive_min\":true,\"inclusive_max\":true}}]}}",
        not_equal_lowered.producer.id_lookup.residual_filter_json.?,
    );

    var wrong_type = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE abs(status) = 7 LIMIT 10");
    defer wrong_type.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &wrong_type, schema, .{ .max_rows = 25 }));

    var negative_bound = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE abs(delta) < -1 LIMIT 10");
    defer negative_bound.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &negative_bound, schema, .{ .max_rows = 25 }));
}

test "document SQL lowers UTC date helper predicates over datetime fields" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "published_at", .path = "published_at", .field_type = .datetime, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "expires_at", .path = "expires_at", .field_type = .datetime, .indexed = false },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };

    var indexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE date_utc(published_at) = '2026-02-28' LIMIT 10");
    defer indexed.deinit(alloc);
    var indexed_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &indexed, schema);
    defer indexed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"date_range\":{\"path\":\"/published_at\",\"start\":\"2026-02-28T00:00:00Z\",\"end\":\"2026-03-01T00:00:00Z\",\"inclusive_start\":true,\"inclusive_end\":false}}",
        indexed_lowered.producer.indexed_query.filter_query_json.?,
    );

    var lookup = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND date_utc(expires_at) > '2024-02-28' LIMIT 10");
    defer lookup.deinit(alloc);
    var lookup_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &lookup, schema);
    defer lookup_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), lookup_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings(
        "{\"date_range\":{\"path\":\"/expires_at\",\"start\":\"2024-02-29T00:00:00Z\",\"inclusive_start\":true}}",
        lookup_lowered.producer.id_lookup.residual_filter_json.?,
    );

    var scan = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE date_utc(expires_at) <= '2026-12-31' LIMIT 10");
    defer scan.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &scan, schema));

    var scan_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &scan, schema, .{ .max_rows = 25 });
    defer scan_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), scan_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings(
        "{\"date_range\":{\"path\":\"/expires_at\",\"end\":\"2027-01-01T00:00:00Z\",\"inclusive_end\":false}}",
        scan_lowered.producer.bounded_scan.residual_filter_json.?,
    );

    var mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' AND date_utc(expires_at) != '2026-01-15' LIMIT 10");
    defer mixed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &mixed, schema));

    var mixed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &mixed, schema, .{ .max_rows = 25 });
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        mixed_lowered.producer.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must_not\":[{\"date_range\":{\"path\":\"/expires_at\",\"start\":\"2026-01-15T00:00:00Z\",\"end\":\"2026-01-16T00:00:00Z\",\"inclusive_start\":true,\"inclusive_end\":false}}]}}",
        mixed_lowered.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 25), mixed_lowered.producer.indexed_query.max_candidate_rows);

    var wrong_type = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE date_utc(status) = '2026-01-15' LIMIT 10");
    defer wrong_type.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &wrong_type, schema, .{ .max_rows = 25 }));

    var bad_date = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE date_utc(expires_at) = '2026-02-29' LIMIT 10");
    defer bad_date.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &bad_date, schema, .{ .max_rows = 25 }));
}

test "document SQL lowers boolean IS predicates over boolean fields" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "published", .path = "published", .field_type = .boolean, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "archived", .path = "archived", .field_type = .boolean, .indexed = false },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = false },
        },
    };

    var indexed_true = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE published IS TRUE LIMIT 10");
    defer indexed_true.deinit(alloc);
    var indexed_true_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &indexed_true, schema);
    defer indexed_true_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/published\",\"value\":true}}",
        indexed_true_lowered.producer.indexed_query.filter_query_json.?,
    );

    var lookup_false = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE _id = 'doc:a' AND archived IS FALSE LIMIT 10");
    defer lookup_false.deinit(alloc);
    var lookup_false_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &lookup_false, schema);
    defer lookup_false_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), lookup_false_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/archived\",\"value\":false}}",
        lookup_false_lowered.producer.id_lookup.residual_filter_json.?,
    );

    var scan_not_true = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE archived IS NOT TRUE LIMIT 10");
    defer scan_not_true.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &scan_not_true, schema));

    var scan_not_true_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &scan_not_true, schema, .{ .max_rows = 25 });
    defer scan_not_true_lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), scan_not_true_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must_not\":[{\"term\":{\"path\":\"/archived\",\"value\":true}}]}}",
        scan_not_true_lowered.producer.bounded_scan.residual_filter_json.?,
    );

    var mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE published IS TRUE AND archived IS NOT FALSE LIMIT 10");
    defer mixed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &mixed, schema));

    var mixed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &mixed, schema, .{ .max_rows = 25 });
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/published\",\"value\":true}}",
        mixed_lowered.producer.indexed_query.filter_query_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"must_not\":[{\"term\":{\"path\":\"/archived\",\"value\":false}}]}}",
        mixed_lowered.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 25), mixed_lowered.producer.indexed_query.max_candidate_rows);

    var wrong_type = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status IS TRUE LIMIT 10");
    defer wrong_type.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &wrong_type, schema, .{ .max_rows = 25 }));
}

test "document SQL rejects scalar predicates over array fields without explicit unnest" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };

    var direct = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE tags = 'urgent' LIMIT 10");
    defer direct.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlArrayRequiresUnnest, lowerDocumentReadPlanParsedSqlAlloc(alloc, &direct, schema));

    var in_list = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE tags IN ('urgent') LIMIT 10");
    defer in_list.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlArrayRequiresUnnest, lowerDocumentReadPlanParsedSqlAlloc(alloc, &in_list, schema));

    var path = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE tags->>'0' = 'urgent' LIMIT 10");
    defer path.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlArrayRequiresUnnest, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &path, schema, .{ .max_rows = 25 }));
}

test "document SQL lowers explicit array unnest over bounded scan" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "tags", .path = "tags", .field_type = .array, .array_item_type = .keyword },
        },
    };

    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);

    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expect(lowered.producer.bounded_scan.residual_filter_json == null);
    try std.testing.expect(lowered.unnest != null);
    try std.testing.expectEqualStrings("/tags", lowered.unnest.?.field);
    try std.testing.expectEqualStrings("tag", lowered.unnest.?.alias);
    try std.testing.expectEqualStrings("\"urgent\"", lowered.unnest.?.filter_value_json.?);
    try std.testing.expectEqual(@as(usize, 2), lowered.projection.len);
    try std.testing.expectEqual(DocumentProjectionKind.id, lowered.projection[0].kind);
    try std.testing.expectEqual(DocumentProjectionKind.unnest_value, lowered.projection[1].kind);
    try std.testing.expectEqualStrings("tag", lowered.projection[1].output);

    var in_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag IN ('urgent', 'vip') LIMIT 10");
    defer in_filter.deinit(alloc);
    var in_filter_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &in_filter, schema, .{ .max_rows = 25 });
    defer in_filter_lowered.deinit(alloc);

    try std.testing.expect(in_filter_lowered.unnest != null);
    try std.testing.expect(in_filter_lowered.unnest.?.filter_value_json == null);
    try std.testing.expectEqualStrings("[\"urgent\",\"vip\"]", in_filter_lowered.unnest.?.filter_values_json.?);

    var range_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag > 'u' LIMIT 10");
    defer range_filter.deinit(alloc);
    var range_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &range_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer range_filter_lowered.deinit(alloc);

    try std.testing.expect(range_filter_lowered.unnest != null);
    try std.testing.expectEqualStrings(
        "{\"term_range\":{\"path\":\"/tags\",\"min\":\"u\",\"inclusive_min\":false}}",
        range_filter_lowered.unnest.?.filter_range_json.?,
    );
    try std.testing.expectEqualStrings(
        "{\"term_range\":{\"path\":\"/tags\",\"min\":\"u\",\"inclusive_min\":false}}",
        range_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var pattern_prefix_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag LIKE 'u%' LIMIT 10");
    defer pattern_prefix_filter.deinit(alloc);
    var pattern_prefix_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &pattern_prefix_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer pattern_prefix_filter_lowered.deinit(alloc);

    try std.testing.expect(pattern_prefix_filter_lowered.unnest != null);
    try std.testing.expectEqualStrings("\"u*\"", pattern_prefix_filter_lowered.unnest.?.filter_pattern_json.?);
    try std.testing.expectEqualStrings(
        "{\"prefix\":{\"path\":\"/tags\",\"value\":\"u\"}}",
        pattern_prefix_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var pattern_wildcard_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag LIKE 'v_p' LIMIT 10");
    defer pattern_wildcard_filter.deinit(alloc);
    var pattern_wildcard_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &pattern_wildcard_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer pattern_wildcard_filter_lowered.deinit(alloc);

    try std.testing.expect(pattern_wildcard_filter_lowered.unnest != null);
    try std.testing.expectEqualStrings("\"v?p\"", pattern_wildcard_filter_lowered.unnest.?.filter_pattern_json.?);
    try std.testing.expectEqualStrings(
        "{\"wildcard\":{\"path\":\"/tags\",\"pattern\":\"v?p\"}}",
        pattern_wildcard_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var pattern_ilike_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag ILIKE 'u%' LIMIT 10");
    defer pattern_ilike_filter.deinit(alloc);
    var pattern_ilike_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &pattern_ilike_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer pattern_ilike_filter_lowered.deinit(alloc);

    try std.testing.expect(pattern_ilike_filter_lowered.unnest != null);
    try std.testing.expect(pattern_ilike_filter_lowered.unnest.?.filter_pattern_case_insensitive);
    try std.testing.expectEqualStrings("\"u*\"", pattern_ilike_filter_lowered.unnest.?.filter_pattern_json.?);
    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"prefix\":{\"path\":\"/tags\",\"value\":\"u\"}},{\"prefix\":{\"path\":\"/tags\",\"value\":\"U\"}}]}",
        pattern_ilike_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var pattern_ilike_wildcard_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag ILIKE 'v_p' LIMIT 10");
    defer pattern_ilike_wildcard_filter.deinit(alloc);
    var pattern_ilike_wildcard_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &pattern_ilike_wildcard_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer pattern_ilike_wildcard_filter_lowered.deinit(alloc);

    try std.testing.expect(pattern_ilike_wildcard_filter_lowered.unnest != null);
    try std.testing.expect(pattern_ilike_wildcard_filter_lowered.unnest.?.filter_pattern_case_insensitive);
    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"wildcard\":{\"path\":\"/tags\",\"pattern\":\"v?p\"}},{\"wildcard\":{\"path\":\"/tags\",\"pattern\":\"v?P\"}},{\"wildcard\":{\"path\":\"/tags\",\"pattern\":\"V?p\"}},{\"wildcard\":{\"path\":\"/tags\",\"pattern\":\"V?P\"}}]}",
        pattern_ilike_wildcard_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var null_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag IS NULL LIMIT 10");
    defer null_filter.deinit(alloc);
    var null_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &null_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer null_filter_lowered.deinit(alloc);

    try std.testing.expect(null_filter_lowered.unnest != null);
    try std.testing.expectEqualStrings("null", null_filter_lowered.unnest.?.filter_value_json.?);
    try std.testing.expectEqualStrings(
        "{\"array_any\":{\"path\":\"/tags\",\"value\":null}}",
        null_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var not_equal_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag <> 'urgent' LIMIT 10");
    defer not_equal_filter.deinit(alloc);
    var not_equal_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &not_equal_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer not_equal_filter_lowered.deinit(alloc);

    try std.testing.expect(not_equal_filter_lowered.unnest != null);
    try std.testing.expectEqualStrings("\"urgent\"", not_equal_filter_lowered.unnest.?.filter_not_value_json.?);
    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"term_range\":{\"path\":\"/tags\",\"max\":\"urgent\",\"inclusive_max\":false}},{\"term_range\":{\"path\":\"/tags\",\"min\":\"urgent\",\"inclusive_min\":false}}]}",
        not_equal_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var compound_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' AND tag <> 'stale' LIMIT 10");
    defer compound_filter.deinit(alloc);
    var compound_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &compound_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer compound_filter_lowered.deinit(alloc);

    try std.testing.expectEqualStrings("\"urgent\"", compound_filter_lowered.unnest.?.filter_value_json.?);
    try std.testing.expectEqualStrings("\"stale\"", compound_filter_lowered.unnest.?.filter_not_value_json.?);
    try std.testing.expectEqualStrings(
        "{\"array_any\":{\"path\":\"/tags\",\"value\":\"urgent\"}}",
        compound_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var contradictory_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' AND tag <> 'urgent' LIMIT 10");
    defer contradictory_filter.deinit(alloc);
    var contradictory_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &contradictory_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer contradictory_filter_lowered.deinit(alloc);

    try std.testing.expectEqualStrings(
        "{\"match_none\":{}}",
        contradictory_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    var not_null_filter = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag IS NOT NULL LIMIT 10");
    defer not_null_filter.deinit(alloc);
    var not_null_filter_lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &not_null_filter, schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer not_null_filter_lowered.deinit(alloc);

    try std.testing.expect(not_null_filter_lowered.unnest != null);
    try std.testing.expect(not_null_filter_lowered.unnest.?.filter_is_not_null);
    try std.testing.expectEqualStrings(
        "{\"term_range\":{\"path\":\"/tags\",\"min\":\"\",\"inclusive_min\":true}}",
        not_null_filter_lowered.producer.indexed_query.filter_query_json.?,
    );

    const view_mapping_virtual_schema = source_binding.DocumentSqlSchema{
        .fields = &.{
            .{
                .name = "tag_list",
                .path = "/tags",
                .source = .view_mapping,
                .field_type = .array,
                .array_item_type = .keyword,
            },
        },
    };
    var view_mapping_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' LIMIT 10");
    defer view_mapping_unnest.deinit(alloc);
    var view_mapping_unnest_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_unnest, schema, view_mapping_virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer view_mapping_unnest_lowered.deinit(alloc);

    try std.testing.expect(view_mapping_unnest_lowered.unnest != null);
    try std.testing.expectEqualStrings("/tags", view_mapping_unnest_lowered.unnest.?.field);
    try std.testing.expectEqualStrings("tag", view_mapping_unnest_lowered.unnest.?.alias);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, view_mapping_unnest_lowered.unnest.?.item_type);
    try std.testing.expectEqualStrings("\"urgent\"", view_mapping_unnest_lowered.unnest.?.filter_value_json.?);

    var view_mapping_lateral_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d JOIN LATERAL UNNEST(d.tag_list) AS tag ON true WHERE tag = 'urgent' LIMIT 10");
    defer view_mapping_lateral_unnest.deinit(alloc);
    var view_mapping_lateral_unnest_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_lateral_unnest, schema, view_mapping_virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer view_mapping_lateral_unnest_lowered.deinit(alloc);

    try std.testing.expect(view_mapping_lateral_unnest_lowered.unnest != null);
    try std.testing.expectEqualStrings("/tags", view_mapping_lateral_unnest_lowered.unnest.?.field);
    try std.testing.expectEqualStrings("tag", view_mapping_lateral_unnest_lowered.unnest.?.alias);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, view_mapping_lateral_unnest_lowered.unnest.?.item_type);
    try std.testing.expectEqualStrings("\"urgent\"", view_mapping_lateral_unnest_lowered.unnest.?.filter_value_json.?);

    var view_mapping_cross_lateral_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d CROSS JOIN LATERAL UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' LIMIT 10");
    defer view_mapping_cross_lateral_unnest.deinit(alloc);
    var view_mapping_cross_lateral_unnest_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_cross_lateral_unnest, schema, view_mapping_virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer view_mapping_cross_lateral_unnest_lowered.deinit(alloc);

    try std.testing.expect(view_mapping_cross_lateral_unnest_lowered.unnest != null);
    try std.testing.expectEqualStrings("/tags", view_mapping_cross_lateral_unnest_lowered.unnest.?.field);
    try std.testing.expectEqualStrings("tag", view_mapping_cross_lateral_unnest_lowered.unnest.?.alias);
    try std.testing.expectEqualStrings("\"urgent\"", view_mapping_cross_lateral_unnest_lowered.unnest.?.filter_value_json.?);

    var view_mapping_left_lateral_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d LEFT JOIN LATERAL UNNEST(d.tag_list) AS tag ON true WHERE tag = 'urgent' LIMIT 10");
    defer view_mapping_left_lateral_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnsupportedJoin, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_left_lateral_unnest, schema, view_mapping_virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var view_mapping_predicated_lateral_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d JOIN LATERAL UNNEST(d.tag_list) AS tag ON tag = 'urgent' LIMIT 10");
    defer view_mapping_predicated_lateral_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnsupportedJoin, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_predicated_lateral_unnest, schema, view_mapping_virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    }));

    var view_mapping_correlated_lateral = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, latest.plan FROM support_view AS d LEFT JOIN LATERAL (SELECT plan FROM support_view AS s WHERE s.plan = d.plan LIMIT 1) AS latest ON true WHERE d.plan = 'pro' LIMIT 10");
    defer view_mapping_correlated_lateral.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlLateralRequiresNativeProducer, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_correlated_lateral, schema, view_mapping_virtual_schema, .{}));

    var view_mapping_indexed_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' LIMIT 10");
    defer view_mapping_indexed_unnest.deinit(alloc);
    var view_mapping_indexed_unnest_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_lowered.deinit(alloc);

    try std.testing.expect(view_mapping_indexed_unnest_lowered.unnest != null);
    try std.testing.expectEqualStrings(
        "{\"array_any\":{\"path\":\"/tags\",\"value\":\"urgent\"}}",
        view_mapping_indexed_unnest_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_in = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag IN ('urgent', 'vip') LIMIT 10");
    defer view_mapping_indexed_unnest_in.deinit(alloc);
    var view_mapping_indexed_unnest_in_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_in, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_in_lowered.deinit(alloc);

    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"array_any\":{\"path\":\"/tags\",\"value\":\"urgent\"}},{\"array_any\":{\"path\":\"/tags\",\"value\":\"vip\"}}]}",
        view_mapping_indexed_unnest_in_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_range = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag > 'm' LIMIT 10");
    defer view_mapping_indexed_unnest_range.deinit(alloc);
    var view_mapping_indexed_unnest_range_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_range, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_range_lowered.deinit(alloc);

    try std.testing.expectEqualStrings(
        "{\"term_range\":{\"path\":\"/tags\",\"min\":\"m\",\"inclusive_min\":false}}",
        view_mapping_indexed_unnest_range_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_pattern = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag LIKE 'u%' LIMIT 10");
    defer view_mapping_indexed_unnest_pattern.deinit(alloc);
    var view_mapping_indexed_unnest_pattern_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_pattern, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_pattern_lowered.deinit(alloc);

    try std.testing.expectEqualStrings("\"u*\"", view_mapping_indexed_unnest_pattern_lowered.unnest.?.filter_pattern_json.?);
    try std.testing.expectEqualStrings(
        "{\"prefix\":{\"path\":\"/tags\",\"value\":\"u\"}}",
        view_mapping_indexed_unnest_pattern_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_ilike = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag ILIKE 'u%' LIMIT 10");
    defer view_mapping_indexed_unnest_ilike.deinit(alloc);
    var view_mapping_indexed_unnest_ilike_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_ilike, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_ilike_lowered.deinit(alloc);

    try std.testing.expect(view_mapping_indexed_unnest_ilike_lowered.unnest.?.filter_pattern_case_insensitive);
    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"prefix\":{\"path\":\"/tags\",\"value\":\"u\"}},{\"prefix\":{\"path\":\"/tags\",\"value\":\"U\"}}]}",
        view_mapping_indexed_unnest_ilike_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag IS NULL LIMIT 10");
    defer view_mapping_indexed_unnest_null.deinit(alloc);
    var view_mapping_indexed_unnest_null_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_null, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_null_lowered.deinit(alloc);

    try std.testing.expectEqualStrings(
        "{\"array_any\":{\"path\":\"/tags\",\"value\":null}}",
        view_mapping_indexed_unnest_null_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_not_equal = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag <> 'urgent' LIMIT 10");
    defer view_mapping_indexed_unnest_not_equal.deinit(alloc);
    var view_mapping_indexed_unnest_not_equal_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_not_equal, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_not_equal_lowered.deinit(alloc);

    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"term_range\":{\"path\":\"/tags\",\"max\":\"urgent\",\"inclusive_max\":false}},{\"term_range\":{\"path\":\"/tags\",\"min\":\"urgent\",\"inclusive_min\":false}}]}",
        view_mapping_indexed_unnest_not_equal_lowered.producer.indexed_query.filter_query_json.?,
    );

    const view_mapping_indexes_json =
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[{\"name\":\"tags_array\",\"lifecycle\":\"ready\",\"generation\":4}],\"fields\":[{\"name\":\"tag_list\",\"path\":\"tags\",\"type\":\"array\",\"item_type\":\"keyword\"}]}},\"tags_array\":{\"type\":\"array_element\",\"path\":\"tags\",\"lifecycle\":\"ready\",\"generation\":4}}";
    var catalog_view_mapping_virtual_schema = try source_binding.documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(alloc, schema, view_mapping_indexes_json, "docs");
    defer source_binding.deinitDocumentSqlSchema(alloc, &catalog_view_mapping_virtual_schema);
    var catalog_view_mapping_capabilities = try source_binding.documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(alloc, schema, view_mapping_indexes_json);
    defer source_binding.deinitDocumentSqlCapabilities(alloc, &catalog_view_mapping_capabilities);

    var catalog_view_mapping_indexed_unnest_not_equal = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag <> 'urgent' LIMIT 10");
    defer catalog_view_mapping_indexed_unnest_not_equal.deinit(alloc);
    var catalog_view_mapping_indexed_unnest_not_equal_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &catalog_view_mapping_indexed_unnest_not_equal, schema, catalog_view_mapping_virtual_schema, catalog_view_mapping_capabilities);
    defer catalog_view_mapping_indexed_unnest_not_equal_lowered.deinit(alloc);

    try std.testing.expectEqualStrings("support_view", catalog_view_mapping_indexed_unnest_not_equal_lowered.view_mapping.?.name);
    try std.testing.expectEqualStrings(
        "{\"disjuncts\":[{\"term_range\":{\"path\":\"/tags\",\"max\":\"urgent\",\"inclusive_max\":false}},{\"term_range\":{\"path\":\"/tags\",\"min\":\"urgent\",\"inclusive_min\":false}}]}",
        catalog_view_mapping_indexed_unnest_not_equal_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_compound = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag = 'urgent' AND tag <> 'stale' LIMIT 10");
    defer view_mapping_indexed_unnest_compound.deinit(alloc);
    var view_mapping_indexed_unnest_compound_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_compound, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_compound_lowered.deinit(alloc);

    try std.testing.expectEqualStrings(
        "{\"array_any\":{\"path\":\"/tags\",\"value\":\"urgent\"}}",
        view_mapping_indexed_unnest_compound_lowered.producer.indexed_query.filter_query_json.?,
    );

    var view_mapping_indexed_unnest_not_null = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM support_view AS d, UNNEST(d.tag_list) AS tag WHERE tag IS NOT NULL LIMIT 10");
    defer view_mapping_indexed_unnest_not_null.deinit(alloc);
    var view_mapping_indexed_unnest_not_null_lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &view_mapping_indexed_unnest_not_null, schema, view_mapping_virtual_schema, .{
        .indexed_array_element_paths = &.{"/tags"},
    });
    defer view_mapping_indexed_unnest_not_null_lowered.deinit(alloc);

    try std.testing.expect(view_mapping_indexed_unnest_not_null_lowered.unnest.?.filter_is_not_null);
    try std.testing.expectEqualStrings(
        "{\"term_range\":{\"path\":\"/tags\",\"min\":\"\",\"inclusive_min\":true}}",
        view_mapping_indexed_unnest_not_null_lowered.producer.indexed_query.filter_query_json.?,
    );

    var residual = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' AND status = 'active' LIMIT 10");
    defer residual.deinit(alloc);
    var residual_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &residual, schema, .{ .max_rows = 25 });
    defer residual_lowered.deinit(alloc);

    try std.testing.expect(residual_lowered.unnest != null);
    try std.testing.expectEqualStrings("\"urgent\"", residual_lowered.unnest.?.filter_value_json.?);
    try std.testing.expectEqual(@as(u32, 25), residual_lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        residual_lowered.producer.bounded_scan.residual_filter_json.?,
    );

    var lookup = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE d._id = 'doc:a' AND tag = 'urgent' LIMIT 10");
    defer lookup.deinit(alloc);
    var lookup_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &lookup, schema);
    defer lookup_lowered.deinit(alloc);

    try std.testing.expect(lookup_lowered.unnest != null);
    try std.testing.expectEqualStrings("/tags", lookup_lowered.unnest.?.field);
    try std.testing.expectEqualStrings("tag", lookup_lowered.unnest.?.alias);
    try std.testing.expectEqualStrings("\"urgent\"", lookup_lowered.unnest.?.filter_value_json.?);
    try std.testing.expectEqual(@as(usize, 1), lookup_lowered.producer.id_lookup.ids.len);
    try std.testing.expectEqualStrings("doc:a", lookup_lowered.producer.id_lookup.ids[0]);
    try std.testing.expectEqual(@as(?u32, 10), lookup_lowered.limit);

    var ordered = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag ORDER BY tag DESC LIMIT 10");
    defer ordered.deinit(alloc);
    var ordered_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &ordered, schema, .{ .max_rows = 25 });
    defer ordered_lowered.deinit(alloc);

    try std.testing.expect(ordered_lowered.unnest != null);
    try std.testing.expect(ordered_lowered.order_by != null);
    try std.testing.expectEqualStrings("tag", ordered_lowered.order_by.?.field);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, ordered_lowered.order_by.?.field_type);
    try std.testing.expectEqual(DocumentOrderDirection.desc, ordered_lowered.order_by.?.direction);

    var indexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE full_text_search('title:alpha') AND tag = 'urgent' LIMIT 10");
    defer indexed.deinit(alloc);
    var indexed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &indexed, schema, .{ .max_rows = 25 });
    defer indexed_lowered.deinit(alloc);

    try std.testing.expect(indexed_lowered.unnest != null);
    try std.testing.expectEqualStrings("title:alpha", indexed_lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqualStrings("\"urgent\"", indexed_lowered.unnest.?.filter_value_json.?);

    var indexed_residual = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE full_text_search('title:alpha') AND tag = 'urgent' AND status = 'active' LIMIT 10");
    defer indexed_residual.deinit(alloc);
    var indexed_residual_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &indexed_residual, schema, .{ .max_rows = 25 });
    defer indexed_residual_lowered.deinit(alloc);

    try std.testing.expect(indexed_residual_lowered.unnest != null);
    try std.testing.expectEqualStrings("title:alpha", indexed_residual_lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqualStrings("\"urgent\"", indexed_residual_lowered.unnest.?.filter_value_json.?);
    try std.testing.expectEqualStrings(
        "{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}",
        indexed_residual_lowered.producer.indexed_query.residual_filter_json.?,
    );
    try std.testing.expectEqual(@as(?u32, 25), indexed_residual_lowered.producer.indexed_query.max_candidate_rows);

    var ordered_indexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE full_text_search('title:alpha') ORDER BY tag ASC LIMIT 10");
    defer ordered_indexed.deinit(alloc);
    var ordered_indexed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &ordered_indexed, schema, .{ .max_rows = 25 });
    defer ordered_indexed_lowered.deinit(alloc);

    try std.testing.expect(ordered_indexed_lowered.unnest != null);
    try std.testing.expect(ordered_indexed_lowered.order_by != null);
    try std.testing.expectEqualStrings("title:alpha", ordered_indexed_lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqual(@as(?u32, 25), ordered_indexed_lowered.producer.indexed_query.max_candidate_rows);

    var unbounded_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag LIMIT 10");
    defer unbounded_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &unbounded_unnest, schema));

    var multiple_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag, label FROM docs AS d, UNNEST(d.tags) AS tag, UNNEST(d.labels) AS label LIMIT 10");
    defer multiple_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnnestUnsupported, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &multiple_unnest, schema, .{ .max_rows = 25 }));

    var nested_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(UNNEST(d.tags)) AS tag LIMIT 10");
    defer nested_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnnestUnsupported, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &nested_unnest, schema, .{ .max_rows = 25 }));

    var multi_argument_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags, d.labels) AS tag LIMIT 10");
    defer multi_argument_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnnestUnsupported, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &multi_argument_unnest, schema, .{ .max_rows = 25 }));

    var non_array_unnest = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, title FROM docs AS d, UNNEST(d.title) AS title LIMIT 10");
    defer non_array_unnest.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlUnnestRequiresArray, lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &non_array_unnest, schema, .{ .max_rows = 25 }));
}

test "document SQL ordered scan requires explicit bounded scan policy" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, title FROM docs ORDER BY title DESC LIMIT 2");
    defer parsed.deinit(alloc);

    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &parsed, schema, .{ .max_rows = 25 });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqual(@as(?u32, 2), lowered.limit);
    try std.testing.expectEqualStrings("/title", lowered.order_by.?.field);
}

test "document SQL lowers scalar in and conjunction to indexed filter producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "published", .path = "published", .field_type = .boolean, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IN ('active', 'pending') AND published = true LIMIT 10");
    defer parsed.deinit(alloc);
    var lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema);
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"terms\":{\"path\":\"/status\",\"values\":[\"active\",\"pending\"]}},{\"term\":{\"path\":\"/published\",\"value\":true}}]}}",
        lowered.producer.indexed_query.filter_query_json.?,
    );
}

test "document SQL lowers scalar in null membership with SQL semantics" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var mixed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IN ('active', NULL, 'pending') LIMIT 10");
    defer mixed.deinit(alloc);
    var mixed_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &mixed, schema);
    defer mixed_lowered.deinit(alloc);
    try std.testing.expectEqualStrings(
        "{\"terms\":{\"path\":\"/status\",\"values\":[\"active\",\"pending\"]}}",
        mixed_lowered.producer.indexed_query.filter_query_json.?,
    );

    var null_only = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE status IN (NULL) LIMIT 10");
    defer null_only.deinit(alloc);
    var null_lowered = try lowerDocumentReadPlanParsedSqlAlloc(alloc, &null_only, schema);
    defer null_lowered.deinit(alloc);
    try std.testing.expectEqualStrings("{\"match_none\":{}}", null_lowered.producer.indexed_query.filter_query_json.?);
}

test "document SQL keeps separate scalar filter residual on full text producer" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text, .indexed = true, .index_lifecycle = .ready },
            .{ .name = "status", .path = "status", .field_type = .keyword, .indexed = true, .index_lifecycle = .ready },
        },
    };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id, status FROM docs WHERE full_text_search('title:alpha') AND status = 'active' LIMIT 10");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanPolicyRequired, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

    var lowered = try lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
        .full_text_filters = true,
        .runtime_schema_scalar_filters = schema,
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqualStrings("title:alpha", lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expect(lowered.producer.indexed_query.filter_query_json == null);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/status\",\"value\":\"active\"}}", lowered.producer.indexed_query.residual_filter_json.?);
    try std.testing.expectEqual(@as(?u32, 25), lowered.producer.indexed_query.max_candidate_rows);
}

test "document SQL requires bounded scan without id predicate" {
    const alloc = std.testing.allocator;
    const schema = runtime_schema.TableSchema{ .storage_mode = .document };
    var parsed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs");
    defer parsed.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlBoundedScanUnboundedSource, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}
