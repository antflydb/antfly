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

const runtime_schema = @import("../storage/schema.zig");
const schema_api = @import("../schema/mod.zig");
const source_binding = @import("source_binding.zig");
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

const DocumentProducerCapabilities = struct {
    indexed_scalar_filters: bool = true,
    indexed_scalar_filter_paths: []const []const u8 = &.{},
    runtime_schema_scalar_filters: ?runtime_schema.TableSchema = null,
    full_text_filters: bool = true,
    full_text_indexes: []const source_binding.DocumentSqlFullTextIndex = &.{},
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
    full_text_query: ?[]const u8 = null,
    filter_query_json: ?[]const u8 = null,
    residual_filter_json: ?[]const u8 = null,
    max_candidate_rows: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.index_name) |index_name| alloc.free(@constCast(index_name));
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

pub const DocumentAlgebraicAggregatePlan = struct {
    table_name: []const u8,
    index_name: ?[]const u8 = null,
    materialization_name: ?[]const u8 = null,
    candidate_producer: ?DocumentProducer = null,
    filter_query_json: ?[]const u8 = null,
    group_by: ?DocumentAggregateGroupBy = null,
    aggregate: DocumentAggregateSpec,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.index_name) |index_name| alloc.free(@constCast(index_name));
        if (self.materialization_name) |materialization_name| alloc.free(@constCast(materialization_name));
        if (self.candidate_producer) |*producer| producer.deinit(alloc);
        if (self.filter_query_json) |filter| alloc.free(@constCast(filter));
        if (self.group_by) |*group_by| group_by.deinit(alloc);
        self.aggregate.deinit(alloc);
        self.* = undefined;
    }
};

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

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.field.len > 0) alloc.free(@constCast(self.field));
        if (self.alias.len > 0) alloc.free(@constCast(self.alias));
        if (self.filter_value_json) |value| alloc.free(@constCast(value));
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

pub const DocumentReadPlan = struct {
    table_name: []const u8,
    projection: []DocumentProjection,
    producer: DocumentProducer,
    order_by: ?DocumentOrderBy = null,
    unnest: ?DocumentUnnest = null,
    limit: ?u32 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
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
            .runtime_schema_scalar_filters = capabilities.runtime_schema_scalar_filters,
            .full_text_filters = capabilities.full_text_filters,
            .full_text_indexes = capabilities.full_text_indexes,
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
            .runtime_schema_scalar_filters = capabilities.runtime_schema_scalar_filters,
            .full_text_filters = capabilities.full_text_filters,
            .full_text_indexes = capabilities.full_text_indexes,
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
        .runtime_schema_scalar_filters = schema,
        .full_text_filters = capabilities.full_text_filters,
        .full_text_indexes = capabilities.full_text_indexes,
        .residual_candidate_limit = documentProducerResidualCandidateLimit(bounded_scan_policy),
    };
}

fn boundedDocumentScanFromPolicy(policy: source_binding.BoundedScanPolicy) !BoundedDocumentScan {
    const max_rows = policy.max_rows orelse return error.DocumentSqlRequiresBoundedScan;
    if (max_rows == 0) return error.DocumentSqlRequiresBoundedScan;
    if (policy.max_bytes) |max_bytes| {
        if (max_bytes == 0) return error.DocumentSqlRequiresBoundedScan;
    }
    return .{
        .max_rows = max_rows,
        .max_bytes = policy.max_bytes,
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
    try rejectUnsupportedDocumentStatementShape(tokens, from_index, tail_start, false);

    if (from_index + 1 >= statement_end or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    errdefer alloc.free(table_name);

    var from_binding = try parseDocumentFromTailAlloc(alloc, tokens[from_index + 1].text, tokens[from_index + 2 .. tail_start], schema);
    errdefer from_binding.deinit(alloc);
    switch (parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape) {
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
            error.DocumentSqlIndexUnavailable => if (whereRangeHasFullTextPredicate(tokens, idx, end_index))
                return err
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
            const policy = bounded_scan_policy orelse return error.DocumentSqlRequiresBoundedScan;
            break :blk DocumentProducer{ .bounded_scan = try boundedDocumentScanFromPolicy(policy) };
        }
        const bounded = limit orelse return error.DocumentSqlRequiresBoundedScan;
        break :blk DocumentProducer{ .bounded_scan = .{ .max_rows = bounded } };
    };
    errdefer {
        var mutable = producer;
        mutable.deinit(alloc);
    }
    if (limit == null and bounded_scan_policy != null) switch (producer) {
        .indexed_query, .bounded_scan => return error.DocumentSqlRequiresBoundedScan,
        .id_lookup => {},
    };
    if (order_by != null) switch (producer) {
        .indexed_query => |*query| {
            if (query.max_candidate_rows == null) {
                query.max_candidate_rows = documentProducerResidualCandidateLimit(bounded_scan_policy) orelse return error.DocumentSqlRequiresBoundedScan;
            }
        },
        else => {},
    };
    return .{
        .table_name = table_name,
        .projection = projection,
        .producer = producer,
        .order_by = order_by,
        .unnest = if (from_binding.unnest) |*unnest| .{
            .field = unnest.takeField(),
            .alias = unnest.takeAlias(),
            .item_type = unnest.item_type,
            .filter_value_json = unnest.takeFilterValueJson(),
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
    if (!has_output_limit_or_aggregate_policy) return error.DocumentSqlRequiresBoundedScan;
    const policy = bounded_scan_policy orelse return error.DocumentSqlRequiresBoundedScan;
    const residual_filter_json = try parseWhereScalarFilterJsonAlloc(alloc, tokens, where_index, end_index, schema, virtual_schema, source_ref, false, unnest);
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
    if ((parsed_sql.readStatementKindIncludingGeneratedAst() orelse return error.UnsupportedSqlShape) != .aggregate) return error.UnsupportedSqlShape;

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
    try rejectUnsupportedDocumentStatementShape(tokens, from_index, tail_start, true);
    if (having_index != null or order_index != null) return error.UnsupportedSqlShape;

    if (from_index + 1 >= statement_end or tokens[from_index + 1].kind != .identifier) return error.UnsupportedSqlShape;
    try rejectDocumentSelectProjectionModifier(tokens[1..from_index]);
    const table_name = try alloc.dupe(u8, tokens[from_index + 1].text);
    errdefer alloc.free(table_name);

    const source_ref = DocumentSourceRef{
        .table_name = tokens[from_index + 1].text,
        .alias = try parseFromTailAlias(tokens[from_index + 2 .. tail_start]),
    };

    var aggregate = try parseDocumentAggregateSpecAlloc(alloc, tokens[1..from_index], schema, virtual_schema, source_ref);
    errdefer aggregate.deinit(alloc);

    const limit = if (limit_index) |idx| try parseLimit(tokens, idx) else null;
    var group_by = if (group_index) |idx|
        try parseDocumentAggregateGroupByAlloc(alloc, tokens, idx, limit_index orelse statement_end, schema, virtual_schema, source_ref, bounded_scan_policy == null)
    else
        null;
    errdefer if (group_by) |*group| group.deinit(alloc);

    var candidate_producer: ?DocumentProducer = null;
    errdefer if (candidate_producer) |*producer| producer.deinit(alloc);
    const filter_query_json: ?[]const u8 = if (where_index) |idx| blk: {
        const end_index = group_index orelse limit_index orelse statement_end;
        var producer = parseWhereProducerAlloc(alloc, tokens, idx, end_index, schema, virtual_schema, source_ref, producer_capabilities, null) catch |err| switch (err) {
            error.DocumentSqlIndexUnavailable => if (whereRangeHasFullTextPredicate(tokens, idx, end_index))
                return err
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
    errdefer if (filter_query_json) |filter| alloc.free(@constCast(filter));

    var plan = DocumentAlgebraicAggregatePlan{
        .table_name = table_name,
        .candidate_producer = candidate_producer,
        .filter_query_json = filter_query_json,
        .group_by = group_by,
        .aggregate = aggregate,
        .limit = limit,
    };
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
                query.max_candidate_rows = documentProducerResidualCandidateLimit(bounded_scan_policy) orelse return error.DocumentSqlRequiresBoundedScan;
            }
            return;
        },
    };
    return error.DocumentSqlRequiresBoundedScan;
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
        } else if (clauseHasNativeAntflySearchFunction(clause)) {
            return error.DocumentSqlNativeSearchRequiresTableFunction;
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
            if (producer_capabilities.residual_candidate_limit == null) return error.DocumentSqlRequiresBoundedScan;
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
    if (capabilities.indexed_scalar_filter_paths.len == 0 and capabilities.runtime_schema_scalar_filters == null) {
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

    if (documentScalarFilterOperatorPath(object, "term")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "terms")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "prefix")) |path| return documentProducerScalarPathReady(capabilities, path);
    if (documentScalarFilterOperatorPath(object, "wildcard")) |path| return documentProducerScalarPathReady(capabilities, path);
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
    if (capabilities.runtime_schema_scalar_filters) |schema| {
        return documentRuntimeSchemaScalarPathReady(schema, path);
    }
    return false;
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
            return error.UnsupportedSqlShape;
        var field = (try documentAggregateFieldForExpressionAlloc(alloc, expression[2 .. expression.len - 1], schema, virtual_schema, source_ref, false)) orelse return error.UnsupportedSqlShape;
        errdefer field.deinit(alloc);
        if (field.field_type != .numeric) return error.UnsupportedSqlShape;
        const source_field = field.field_name orelse return error.UnsupportedSqlShape;
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

    return error.UnsupportedSqlShape;
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
    if (findComma(group_tokens, 0) != null) return error.UnsupportedSqlShape;

    var field = (try documentAggregateFieldForExpressionAlloc(alloc, group_tokens, schema, virtual_schema, source_ref, require_index)) orelse return error.UnsupportedSqlShape;
    errdefer field.deinit(alloc);
    const source_field = field.field_name orelse return error.UnsupportedSqlShape;
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
    if (tokens.len != 3 or tokens[1].kind != .eq) return error.UnsupportedSqlShape;
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

fn parseFullTextQueryAlloc(alloc: std.mem.Allocator, tokens: []const Token) !?[]const u8 {
    if (tokens.len != 4) return null;
    if (!tokens[0].matchesKeywordTag(.full_text_search)) return null;
    if (tokens[1].kind != .lparen or tokens[2].kind != .string or tokens[3].kind != .rparen) return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, tokens[2].text);
}

fn whereRangeHasFullTextPredicate(tokens: []const Token, where_index: usize, end_index: usize) bool {
    if (where_index + 1 >= end_index) return false;
    for (tokens[where_index + 1 .. end_index]) |token| {
        if (token.matchesKeywordTag(.full_text_search)) return true;
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

fn buildTermFilterClauseAlloc(alloc: std.mem.Allocator, path: []const u8, value: Token) ![]const u8 {
    const value_json = try tokenLiteralJsonAlloc(alloc, value);
    defer alloc.free(value_json);
    return try std.fmt.allocPrint(
        alloc,
        "{{\"term\":{{\"path\":{f},\"value\":{s}}}}}",
        .{ std.json.fmt(path, .{}), value_json },
    );
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
        if (!documentVirtualFieldProvidesJsonPathRoot(virtual_field.source) or !documentVirtualAggregatePathIsScalar(virtual_field.path)) return error.UnsupportedSqlShape;
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
        if (!documentVirtualAggregatePathIsScalar(virtual_field.path)) return error.UnsupportedSqlShape;
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
    return source == .index_definition or source == .typed_path_metadata;
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
    if (path[0] == '/') return try alloc.dupe(u8, path);
    return try std.fmt.allocPrint(alloc, "/{s}", .{path});
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
) !DocumentFromBinding {
    const comma = findComma(tokens, 0);
    if (comma == null) {
        return .{ .source_ref = .{
            .table_name = table_name,
            .alias = try parseFromTailAlias(tokens),
        } };
    }

    const split = comma.?;
    const source_ref = DocumentSourceRef{
        .table_name = table_name,
        .alias = try parseFromTailAlias(tokens[0..split]),
    };
    var unnest = try parseDocumentUnnestAlloc(alloc, tokens[split + 1 ..], schema, source_ref);
    errdefer unnest.deinit(alloc);
    return .{
        .source_ref = source_ref,
        .unnest = unnest,
    };
}

fn parseDocumentUnnestAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    schema: runtime_schema.TableSchema,
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
    const alias = (try parseFromTailAlias(tokens[close + 1 ..])) orelse return error.UnsupportedSqlShape;
    if (std.mem.indexOfScalar(u8, alias, '.') != null) return error.UnsupportedSqlShape;

    const field_tokens = tokens[2..close];
    if (field_tokens.len != 1 or field_tokens[0].kind != .identifier) return error.UnsupportedSqlShape;
    const field_name = try documentIdentifierName(field_tokens[0], source_ref);
    if (std.mem.eql(u8, field_name, "_id") or std.mem.eql(u8, field_name, "_doc")) return error.UnsupportedSqlShape;
    const column = documentFieldColumn(schema, field_name) orelse return error.InvalidSqlCatalog;
    if (column.field_type != .array) return error.DocumentSqlArrayRequiresUnnest;
    return .{
        .field = try documentFilterPathAlloc(alloc, column.path),
        .alias = try alloc.dupe(u8, alias),
        .item_type = column.array_item_type orelse .json,
    };
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
        findTopLevelKeyword(tokens, .recursive) != null or
        findTopLevelKeyword(tokens, .join) != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (!allow_group_by and findTopLevelKeyword(tokens, .group) != null) return error.UnsupportedSqlShape;
    if (findTopLevelKeyword(tokens, .having) != null) return error.UnsupportedSqlShape;
    if (from_index + 2 < source_tail_end) {
        if (findTopLevelCommaInRange(tokens, from_index + 2, source_tail_end)) |comma| {
            if (!documentFromTailCommaStartsUnnest(tokens, comma, source_tail_end)) return error.UnsupportedSqlShape;
        }
    }
}

fn documentFromTailCommaStartsUnnest(tokens: []const Token, comma_index: usize, source_tail_end: usize) bool {
    if (comma_index + 1 >= source_tail_end) return false;
    const token = tokens[comma_index + 1];
    return token.kind == .identifier and std.ascii.eqlIgnoreCase(token.text, "unnest");
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
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentAlgebraicAggregatePlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &aggregate_having_tail, schema, .{ .max_rows = 25 }));
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
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &union_read, schema));

    var intersect_read = try tokenized.ParsedSql.initAlloc(alloc, "SELECT _id FROM docs WHERE status = 'active' INTERSECT SELECT _id FROM docs WHERE status = 'trial'");
    defer intersect_read.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &intersect_read, schema));

    var comma_join = try tokenized.ParsedSql.initAlloc(alloc, "SELECT docs._id FROM docs, other WHERE docs.status = 'active' LIMIT 10");
    defer comma_join.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &comma_join, schema));

    var explicit_join = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id FROM docs d JOIN other o ON d._id = o.doc_id WHERE d.status = 'active' LIMIT 10");
    defer explicit_join.deinit(alloc);
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentReadPlanParsedSqlAlloc(alloc, &explicit_join, schema));

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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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
    try std.testing.expectEqual(@as(?u32, 5), lowered.limit);
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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &avg_without_native, schema, sum_and_count_only_indexes_json));

    var wrong_measure = try tokenized.ParsedSql.initAlloc(alloc, "SELECT min(amount) AS min_amount FROM docs GROUP BY status LIMIT 5");
    defer wrong_measure.deinit(alloc);
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentAggregatePlanWithOptionalIndexesJsonParsedSqlAlloc(alloc, &wrong_measure, schema, indexes_json));
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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentAggregatePlanWithOptionalIndexesAndCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, null, .{
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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &unfiltered, schema, null, null));

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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &filtered, schema, null, null));

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
    try std.testing.expectError(error.UnsupportedSqlShape, lowerDocumentAggregatePlanWithOptionalIndexesAndBoundedScanPolicyParsedSqlAlloc(alloc, &non_numeric, non_numeric_schema, null, .{ .max_rows = 25 }));
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
        "SELECT _id FROM docs WHERE antfly.semantic_search(table_name => 'docs', index => 'docs_body_semantic', query => 'alpha', limit => 5) LIMIT 5",
        "SELECT _id FROM docs WHERE antfly.vector_search(table_name => 'docs', index => 'docs_embedding_hnsw', vector => '[0.1,0.2,0.3]', limit => 5) LIMIT 5",
        "SELECT _id FROM docs WHERE antfly.hybrid_search(table_name => 'docs', query => 'alpha', limit => 5) LIMIT 5",
        "SELECT _id FROM docs WHERE antfly.graph_traverse(table_name => 'docs', index => 'docs_edge_graph', start => 'doc:a', limit => 5) LIMIT 5",
        "SELECT _id FROM docs WHERE antfly.graph_metric(table_name => 'docs', index => 'docs_edge_graph', metric => 'pagerank', limit => 5) LIMIT 5",
        "SELECT _id FROM docs WHERE antfly.graph_metric_rerank(table_name => 'docs', full_text_index => 'docs_body_fts', query => 'alpha', graph_index => 'docs_edge_graph', graph_metric => 'pagerank', limit => 5) LIMIT 5",
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

    try std.testing.expectError(error.DocumentSqlIndexUnavailable, lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &parsed, schema, .{
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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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
        error.DocumentSqlRequiresBoundedScan,
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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanWithCapabilitiesParsedSqlAlloc(alloc, &mixed, schema, .{
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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, .{}));

    var lowered = try lowerDocumentReadPlanWithCapabilitiesAndVirtualSchemaParsedSqlAlloc(alloc, &parsed, schema, virtual_schema, .{
        .bounded_scan = .{ .max_rows = 25 },
    });
    defer lowered.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), lowered.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"numeric_range\":{\"path\":\"/metrics/score\",\"min\":7,\"inclusive_min\":true}}", lowered.producer.bounded_scan.residual_filter_json.?);
    try std.testing.expectEqualStrings("/metrics/score", lowered.projection[1].field);

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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &unindexed, schema));

    var unindexed_scan = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &unindexed, schema, .{ .max_rows = 25 });
    defer unindexed_scan.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 25), unindexed_scan.producer.bounded_scan.max_rows);
    try std.testing.expectEqualStrings("{\"term\":{\"path\":\"/category\",\"value\":\"release\"}}", unindexed_scan.producer.bounded_scan.residual_filter_json.?);
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

    var ordered_indexed = try tokenized.ParsedSql.initAlloc(alloc, "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE full_text_search('title:alpha') ORDER BY tag ASC LIMIT 10");
    defer ordered_indexed.deinit(alloc);
    var ordered_indexed_lowered = try lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(alloc, &ordered_indexed, schema, .{ .max_rows = 25 });
    defer ordered_indexed_lowered.deinit(alloc);

    try std.testing.expect(ordered_indexed_lowered.unnest != null);
    try std.testing.expect(ordered_indexed_lowered.order_by != null);
    try std.testing.expectEqualStrings("title:alpha", ordered_indexed_lowered.producer.indexed_query.full_text_query.?);
    try std.testing.expectEqual(@as(?u32, 25), ordered_indexed_lowered.producer.indexed_query.max_candidate_rows);
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

    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));

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
    try std.testing.expectError(error.DocumentSqlRequiresBoundedScan, lowerDocumentReadPlanParsedSqlAlloc(alloc, &parsed, schema));
}
