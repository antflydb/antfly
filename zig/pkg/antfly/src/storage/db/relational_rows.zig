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

const schema_mod = @import("../schema.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

pub const JoinSide = enum {
    left,
    right,
};

pub const MaterializedCte = struct {
    name: []const u8,
    output_fields: []const []const u8 = &.{},
    result: types.RelationalRowsQueryResult,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        freeOwnedConstStringSlice(alloc, self.output_fields);
        self.result.deinit(alloc);
        self.* = undefined;
    }
};

pub const PlannedCte = struct {
    name: []const u8,
    output_fields: []const []const u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        freeOwnedConstStringSlice(alloc, self.output_fields);
        self.* = undefined;
    }
};

pub fn deinitPlannedCtes(
    alloc: Allocator,
    planned_ctes: []PlannedCte,
) void {
    for (planned_ctes) |*cte| cte.deinit(alloc);
    if (planned_ctes.len > 0) alloc.free(planned_ctes);
}

pub fn findPlannedCte(
    planned_ctes: []PlannedCte,
    name: []const u8,
) ?*PlannedCte {
    for (planned_ctes) |*cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

pub fn findMaterializedCte(ctes: []MaterializedCte, name: []const u8) ?*MaterializedCte {
    for (ctes) |*cte| {
        if (std.mem.eql(u8, cte.name, name)) return cte;
    }
    return null;
}

pub fn plannedSourceCteOutputFields(
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsQueryRequest,
) ?[]const []const u8 {
    if (req.source_cte.len == 0) return null;
    const source = findPlannedCte(planned_ctes, req.source_cte) orelse return &.{};
    return source.output_fields;
}

pub fn sourceCteOutputFields(
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsQueryRequest,
) ?[]const []const u8 {
    if (req.source_cte.len == 0) return null;
    const source = findMaterializedCte(materialized_ctes, req.source_cte) orelse return &.{};
    return source.output_fields;
}

pub fn validateMaterializedCtes(
    ctes: []const types.RelationalRowsCte,
    materialized_ctes: []MaterializedCte,
) !void {
    for (ctes, 0..) |cte, idx| {
        if (cte.name.len == 0) return error.InvalidQueryRequest;
        if (findMaterializedCte(materialized_ctes, cte.name) != null) return error.InvalidQueryRequest;
        for (ctes[0..idx]) |prior| {
            if (std.mem.eql(u8, prior.name, cte.name)) return error.InvalidQueryRequest;
        }
        if (cte.query.source_cte.len != 0 and
            findMaterializedCte(materialized_ctes, cte.query.source_cte) == null and
            !cteNameExists(ctes[0..idx], cte.query.source_cte))
        {
            return error.InvalidQueryRequest;
        }
        if (cte.table_function != null and cte.query.source_cte.len != 0) return error.InvalidQueryRequest;
        if (cte.query.row_claim != null or cte.query.doc_key_range != null) return error.UnsupportedQueryRequest;
    }
}

pub fn validateQueryPlanCteReferences(plan: types.RelationalRowsQueryPlan) !void {
    try validateMaterializedCtes(plan.ctes, &.{});
    try validateFinalCteReference(plan.ctes, plan.query.source_cte);
}

pub fn validateQueryPlanRequest(plan: types.RelationalRowsQueryPlan) !void {
    if (plan.query.row_claim != null or plan.query.doc_key_range != null) return error.UnsupportedQueryRequest;
}

pub fn validateBaseQueryRequest(req: types.RelationalRowsQueryRequest) !void {
    if (queryHasDistinctOn(req) and req.row_claim != null) return error.UnsupportedQueryRequest;
    if (req.row_claim) |claim| {
        if (claim.txn_id == null) return error.InvalidQueryRequest;
        if (!claim.mode.usesDurableIntent()) return error.InvalidQueryRequest;
    }
    try validateQueryProjectionOutputs(req);
}

pub fn queryHasDistinctOn(req: types.RelationalRowsQueryRequest) bool {
    return req.distinct_on.len > 0 or req.distinct_on_expressions.len > 0;
}

pub fn validateQueryProjectionOutputDoesNotCollide(
    req: types.RelationalRowsQueryRequest,
    field: []const u8,
) !void {
    if (field.len == 0) return error.InvalidQueryRequest;
    for (req.json_extract) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return error.InvalidQueryRequest;
    }
    for (req.array_length) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return error.InvalidQueryRequest;
    }
    for (req.coalesce) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return error.InvalidQueryRequest;
    }
    for (req.field_aliases) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return error.InvalidQueryRequest;
    }
    for (req.expressions) |projection| {
        if (std.mem.eql(u8, projection.output, field)) return error.InvalidQueryRequest;
    }
}

pub fn queryProjectionOutputAlreadyRendered(req: types.RelationalRowsQueryRequest, output: []const u8) bool {
    for (req.select) |field| {
        if (std.mem.eql(u8, field, output)) return true;
    }
    for (req.json_extract) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    for (req.array_length) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    for (req.coalesce) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    for (req.field_aliases) |projection| {
        if (std.mem.eql(u8, projection.output, output)) return true;
    }
    return false;
}

pub fn appendOutputFieldAlloc(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged([]const u8),
    field: []const u8,
) !void {
    if (field.len == 0) return error.InvalidQueryRequest;
    if (outputFieldsCoverField(fields.items, field)) return;
    const owned = try alloc.dupe(u8, field);
    errdefer alloc.free(owned);
    try fields.append(alloc, owned);
}

pub fn validateOutputField(output_fields: []const []const u8, field: []const u8) !void {
    if (field.len == 0 or !outputFieldsCoverField(output_fields, field)) return error.InvalidQueryRequest;
}

pub fn outputFieldsCoverField(output_fields: []const []const u8, field: []const u8) bool {
    for (output_fields) |output| {
        if (std.mem.eql(u8, output, field)) return true;
        if (field.len > output.len and std.mem.startsWith(u8, field, output) and field[output.len] == '.') return true;
    }
    return false;
}

pub fn validateBaseQueryRequestAgainstOutputFields(
    output_fields: []const []const u8,
    req: types.RelationalRowsQueryRequest,
) !void {
    try validateBaseQueryRequest(req);
    if (!req.select_all) return;
    for (output_fields) |field| try validateQueryProjectionOutputDoesNotCollide(req, field);
}

pub fn validateQueryAgainstCteOutput(
    req: types.RelationalRowsQueryRequest,
    output_fields: []const []const u8,
) !void {
    for (req.predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.array_any) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.array_contains) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.array_eq) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.in_predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.json_contains) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.json_path_eq) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.json_path_exists) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.text_patterns) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.or_predicates) |group| {
        for (group.predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    }
    for (req.not_predicates) |group| {
        for (group.predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    }
    for (req.access_or_predicates) |group| try validateAccessPredicateGroupAgainstCteOutput(output_fields, group);
    for (req.access_not_predicates) |group| try validateAccessPredicateGroupAgainstCteOutput(output_fields, group);
    for (req.expression_predicates) |condition| try validateExpressionConditionAgainstCteOutput(output_fields, condition);
    for (req.expression_or_predicates) |group| {
        for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(output_fields, condition);
    }
    for (req.expression_not_predicates) |group| {
        for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(output_fields, condition);
    }
    for (req.expression_array_contains) |predicate| try validateExpressionAgainstCteOutput(output_fields, predicate.expression);
    if (!req.select_all) {
        for (req.select) |field| try validateOutputField(output_fields, field);
    }
    for (req.distinct_on) |field| try validateOutputField(output_fields, field);
    for (req.distinct_on_expressions) |expression| try validateExpressionAgainstCteOutput(output_fields, expression);
    for (req.order_by) |order| {
        if (order.field.len > 0) try validateOutputField(output_fields, order.field);
        if (order.expression) |expression| try validateExpressionAgainstCteOutput(output_fields, expression);
    }
    for (req.json_extract) |projection| try validateOutputField(output_fields, projection.field);
    for (req.array_length) |projection| try validateOutputField(output_fields, projection.field);
    for (req.coalesce) |projection| {
        for (projection.operands) |operand| {
            if (operand.kind == .field) try validateOutputField(output_fields, operand.field);
        }
    }
    for (req.field_aliases) |projection| try validateOutputField(output_fields, projection.field);
    for (req.expressions) |projection| try validateExpressionAgainstCteOutput(output_fields, projection.expression);
}

pub fn validateQueryOrderAgainstCteOutput(
    output_fields: []const []const u8,
    order: types.RelationalRowsQueryOrder,
) !void {
    if (order.field.len > 0) try validateOutputField(output_fields, order.field);
    if (order.expression) |expression| try validateExpressionAgainstCteOutput(output_fields, expression);
}

pub fn validateExpressionConditionAgainstCteOutput(
    output_fields: []const []const u8,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateExpressionAgainstCteOutput(output_fields, condition.lhs);
    for (condition.rhs) |expression| try validateExpressionAgainstCteOutput(output_fields, expression);
}

pub fn validateExpressionAgainstCteOutput(
    output_fields: []const []const u8,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        if (expression.field_source != .row) return error.InvalidQueryRequest;
        try validateOutputField(output_fields, expression.field);
    }
    for (expression.operands) |operand| try validateExpressionAgainstCteOutput(output_fields, operand);
    for (expression.case_branches) |branch| {
        try validateExpressionConditionAgainstCteOutput(output_fields, branch.when);
        try validateExpressionAgainstCteOutput(output_fields, branch.then);
    }
    for (expression.case_else) |fallback| try validateExpressionAgainstCteOutput(output_fields, fallback);
}

fn validateAccessPredicateGroupAgainstCteOutput(
    output_fields: []const []const u8,
    group: types.RelationalRowsAccessPredicateGroup,
) !void {
    for (group.predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.array_any) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.array_contains) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.array_eq) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.in_predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.json_contains) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.json_path_eq) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.json_path_exists) |predicate| try validateOutputField(output_fields, predicate.field);
    for (group.text_patterns) |predicate| try validateOutputField(output_fields, predicate.field);
}

pub fn tableFunctionOutputFieldsAlloc(
    alloc: Allocator,
    table_function: types.RelationalRowsTableFunction,
    req: types.RelationalRowsQueryRequest,
) ![]const []const u8 {
    _ = table_function;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(@constCast(field));
        fields.deinit(alloc);
    }
    const source_fields = types.relational_rows_graph_table_function_fields[0..];
    if (req.select_all) {
        for (source_fields) |field| try appendOutputFieldAlloc(alloc, &fields, field);
    } else {
        for (req.select) |field| {
            if (!outputFieldExists(source_fields, field)) return error.InvalidQueryRequest;
            try appendOutputFieldAlloc(alloc, &fields, field);
        }
    }
    try appendQueryProjectionOutputFieldsAlloc(alloc, &fields, req);
    return try fields.toOwnedSlice(alloc);
}

pub fn plannedQueryOutputFieldsAlloc(
    alloc: Allocator,
    runtime_schema: schema_mod.TableSchema,
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsQueryRequest,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(@constCast(field));
        fields.deinit(alloc);
    }

    if (req.select_all) {
        if (req.source_cte.len != 0) {
            const source = findPlannedCte(planned_ctes, req.source_cte) orelse return error.InvalidQueryRequest;
            for (source.output_fields) |field| try appendOutputFieldAlloc(alloc, &fields, field);
        } else {
            try appendSchemaOutputFieldsAlloc(alloc, &fields, runtime_schema);
        }
    } else {
        for (req.select) |field| try appendOutputFieldAlloc(alloc, &fields, field);
    }
    try appendQueryProjectionOutputFieldsAlloc(alloc, &fields, req);
    return try fields.toOwnedSlice(alloc);
}

pub fn planCteOutputsAlloc(
    alloc: Allocator,
    runtime_schema: schema_mod.TableSchema,
    ctes: []const types.RelationalRowsCte,
) ![]PlannedCte {
    var planned = std.ArrayListUnmanaged(PlannedCte).empty;
    errdefer {
        for (planned.items) |*cte| cte.deinit(alloc);
        planned.deinit(alloc);
    }
    for (ctes) |cte| {
        if (cte.table_function != null and cte.query.source_cte.len != 0) return error.InvalidQueryRequest;
        try validateQueryAgainstPlannedCteOutput(planned.items, cte.query);
        const output_fields = if (cte.table_function) |table_function|
            try tableFunctionOutputFieldsAlloc(alloc, table_function, cte.query)
        else
            try plannedQueryOutputFieldsAlloc(alloc, runtime_schema, planned.items, cte.query);
        errdefer freeOwnedConstStringSlice(alloc, output_fields);
        try planned.append(alloc, .{
            .name = cte.name,
            .output_fields = output_fields,
        });
    }
    return try planned.toOwnedSlice(alloc);
}

pub fn queryOutputFieldsAlloc(
    alloc: Allocator,
    runtime_schema: schema_mod.TableSchema,
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsQueryRequest,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(@constCast(field));
        fields.deinit(alloc);
    }

    if (req.select_all) {
        if (req.source_cte.len != 0) {
            const source = findMaterializedCte(materialized_ctes, req.source_cte) orelse return error.InvalidQueryRequest;
            for (source.output_fields) |field| try appendOutputFieldAlloc(alloc, &fields, field);
        } else {
            try appendSchemaOutputFieldsAlloc(alloc, &fields, runtime_schema);
        }
    } else {
        for (req.select) |field| try appendOutputFieldAlloc(alloc, &fields, field);
    }
    try appendQueryProjectionOutputFieldsAlloc(alloc, &fields, req);
    return try fields.toOwnedSlice(alloc);
}

pub fn aggregateOutputFieldsAlloc(
    alloc: Allocator,
    req: types.RelationalRowsAggregateRequest,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(@constCast(field));
        fields.deinit(alloc);
    }
    for (req.group_by) |field| try appendOutputFieldAlloc(alloc, &fields, field);
    for (req.group_expressions) |projection| try appendOutputFieldAlloc(alloc, &fields, projection.output);
    for (req.aggregations) |aggregation| try appendOutputFieldAlloc(alloc, &fields, aggregation.name);
    return try fields.toOwnedSlice(alloc);
}

pub fn validateAggregateOutputReferencesAlloc(
    alloc: Allocator,
    req: types.RelationalRowsAggregateRequest,
) !void {
    const output_fields = try aggregateOutputFieldsAlloc(alloc, req);
    defer freeOwnedConstStringSlice(alloc, output_fields);
    for (req.having_predicates) |predicate| try validateOutputField(output_fields, predicate.field);
    for (req.having_expressions) |condition| try validateExpressionConditionAgainstCteOutput(output_fields, condition);
    for (req.having_any) |group| {
        for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(output_fields, condition);
    }
    for (req.having_not) |group| {
        for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(output_fields, condition);
    }
    for (req.order_by) |order| try validateQueryOrderAgainstCteOutput(output_fields, order);
}

pub fn joinOutputFieldsAlloc(
    alloc: Allocator,
    select: []const types.RelationalRowsJoinProjection,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(@constCast(field));
        fields.deinit(alloc);
    }
    if (select.len == 0) {
        try appendOutputFieldAlloc(alloc, &fields, "left");
        try appendOutputFieldAlloc(alloc, &fields, "right");
    } else {
        for (select) |projection| try appendOutputFieldAlloc(alloc, &fields, projection.output);
    }
    return try fields.toOwnedSlice(alloc);
}

pub fn validateJoinOutputReferencesAlloc(
    alloc: Allocator,
    req: types.RelationalRowsJoinRequest,
) !void {
    const output_fields = try joinOutputFieldsAlloc(alloc, req.select);
    defer freeOwnedConstStringSlice(alloc, output_fields);
    for (req.order_by) |order| try validateQueryOrderAgainstCteOutput(output_fields, order);
}

pub fn validateLateralOutputReferencesAlloc(
    alloc: Allocator,
    req: types.RelationalRowsLateralRequest,
) !void {
    const output_fields = try joinOutputFieldsAlloc(alloc, req.select);
    defer freeOwnedConstStringSlice(alloc, output_fields);
    for (req.order_by) |order| try validateQueryOrderAgainstCteOutput(output_fields, order);
}

pub fn windowOutputFieldsAlloc(
    alloc: Allocator,
    runtime_schema: schema_mod.TableSchema,
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsWindowRequest,
) ![]const []const u8 {
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(@constCast(field));
        fields.deinit(alloc);
    }
    if (req.select_all) {
        if (sourceCteOutputFields(materialized_ctes, req.source)) |source_fields| {
            for (source_fields) |field| try appendOutputFieldAlloc(alloc, &fields, field);
        } else {
            try appendSchemaOutputFieldsAlloc(alloc, &fields, runtime_schema);
        }
    } else {
        for (req.select) |field| try appendOutputFieldAlloc(alloc, &fields, field);
    }
    for (req.windows) |window| try appendOutputFieldAlloc(alloc, &fields, window.output);
    return try fields.toOwnedSlice(alloc);
}

pub fn validateWindowOutputReferencesAlloc(
    alloc: Allocator,
    runtime_schema: schema_mod.TableSchema,
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsWindowRequest,
) !void {
    const output_fields = try windowOutputFieldsAlloc(alloc, runtime_schema, materialized_ctes, req);
    defer freeOwnedConstStringSlice(alloc, output_fields);
    for (req.order_by) |order| try validateQueryOrderAgainstCteOutput(output_fields, order);
}

pub fn validateWindowOutputReferencesAgainstPlannedCtesAlloc(
    alloc: Allocator,
    runtime_schema: schema_mod.TableSchema,
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsWindowRequest,
) !void {
    var output_fields = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (output_fields.items) |field| alloc.free(@constCast(field));
        output_fields.deinit(alloc);
    }
    if (req.select_all) {
        if (plannedSourceCteOutputFields(planned_ctes, req.source)) |source_fields| {
            for (source_fields) |field| try appendOutputFieldAlloc(alloc, &output_fields, field);
        } else {
            try appendSchemaOutputFieldsAlloc(alloc, &output_fields, runtime_schema);
        }
    } else {
        for (req.select) |field| try appendOutputFieldAlloc(alloc, &output_fields, field);
    }
    for (req.windows) |window| try appendOutputFieldAlloc(alloc, &output_fields, window.output);
    for (req.order_by) |order| try validateQueryOrderAgainstCteOutput(output_fields.items, order);
}

fn appendSchemaOutputFieldsAlloc(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged([]const u8),
    runtime_schema: schema_mod.TableSchema,
) !void {
    for (runtime_schema.relational_columns) |column| {
        try appendOutputFieldAlloc(alloc, fields, column.name);
        if (!std.mem.eql(u8, column.path, column.name)) try appendOutputFieldAlloc(alloc, fields, column.path);
    }
}

fn appendQueryProjectionOutputFieldsAlloc(
    alloc: Allocator,
    fields: *std.ArrayListUnmanaged([]const u8),
    req: types.RelationalRowsQueryRequest,
) !void {
    for (req.json_extract) |projection| try appendOutputFieldAlloc(alloc, fields, projection.output);
    for (req.array_length) |projection| try appendOutputFieldAlloc(alloc, fields, projection.output);
    for (req.coalesce) |projection| try appendOutputFieldAlloc(alloc, fields, projection.output);
    for (req.field_aliases) |projection| try appendOutputFieldAlloc(alloc, fields, projection.output);
    for (req.expressions) |projection| {
        if (queryProjectionOutputAlreadyRendered(req, projection.output)) continue;
        try appendOutputFieldAlloc(alloc, fields, projection.output);
    }
}

fn outputFieldExists(fields: []const []const u8, name: []const u8) bool {
    for (fields) |field| {
        if (std.mem.eql(u8, field, name)) return true;
    }
    return false;
}

fn validateQueryProjectionOutputs(req: types.RelationalRowsQueryRequest) !void {
    if (!req.select_all) {
        for (req.select) |field| {
            if (field.len == 0) return error.InvalidQueryRequest;
            if (queryProjectionOutputCount(req, field) > 1) return error.InvalidQueryRequest;
        }
    }
    for (req.json_extract) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (queryProjectionOutputCount(req, projection.output) > 1) return error.InvalidQueryRequest;
    }
    for (req.array_length) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (queryProjectionOutputCount(req, projection.output) > 1) return error.InvalidQueryRequest;
    }
    for (req.coalesce) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (queryProjectionOutputCount(req, projection.output) > 1) return error.InvalidQueryRequest;
    }
    for (req.field_aliases) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (queryProjectionOutputCount(req, projection.output) > 1) return error.InvalidQueryRequest;
    }
    for (req.expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (queryProjectionOutputCount(req, projection.output) > 1) return error.InvalidQueryRequest;
    }
}

fn queryProjectionOutputCount(req: types.RelationalRowsQueryRequest, output: []const u8) usize {
    var count: usize = 0;
    if (!req.select_all) {
        for (req.select) |field| {
            if (std.mem.eql(u8, field, output)) count += 1;
        }
    }
    for (req.json_extract) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (req.array_length) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (req.coalesce) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (req.field_aliases) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    for (req.expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
}

pub fn validateAggregateRequest(req: types.RelationalRowsAggregateRequest) !void {
    if (req.aggregations.len == 0 and req.group_by.len == 0 and req.group_expressions.len == 0) return error.InvalidArgument;
    if (req.source.row_claim != null) return error.UnsupportedQueryRequest;
    if (req.source.doc_key_range != null and req.source.source_cte.len != 0) return error.InvalidQueryRequest;
    try validateAggregateOutputNames(req);
}

fn validateAggregateOutputNames(req: types.RelationalRowsAggregateRequest) !void {
    for (req.group_by) |field| {
        if (field.len == 0) return error.InvalidQueryRequest;
        if (aggregateOutputNameCount(req, field) > 1) return error.InvalidQueryRequest;
    }
    for (req.group_expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (aggregateOutputNameCount(req, projection.output) > 1) return error.InvalidQueryRequest;
    }
    for (req.aggregations) |aggregation| {
        if (aggregation.name.len == 0) return error.InvalidQueryRequest;
        if (aggregateOutputNameCount(req, aggregation.name) > 1) return error.InvalidQueryRequest;
    }
}

fn aggregateOutputNameCount(
    req: types.RelationalRowsAggregateRequest,
    name: []const u8,
) usize {
    var count: usize = 0;
    for (req.group_by) |field| {
        if (std.mem.eql(u8, field, name)) count += 1;
    }
    for (req.group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, name)) count += 1;
    }
    for (req.aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, name)) count += 1;
    }
    return count;
}

pub fn validateAggregatePlanCteReferences(plan: types.RelationalRowsAggregatePlan) !void {
    try validateMaterializedCtes(plan.ctes, &.{});
    try validateFinalCteReference(plan.ctes, plan.aggregate.source.source_cte);
}

pub fn validateWindowPlanCteReferences(plan: types.RelationalRowsWindowPlan) !void {
    try validateMaterializedCtes(plan.ctes, &.{});
    try validateFinalCteReference(plan.ctes, plan.window.source.source_cte);
}

pub fn validateJoinPlanCteReferences(plan: types.RelationalRowsJoinPlan) !void {
    try validateMaterializedCtes(plan.ctes, &.{});
    try validateFinalCteReference(plan.ctes, plan.join.left.source_cte);
    try validateFinalCteReference(plan.ctes, plan.join.right.source_cte);
}

pub fn validateJoinRequest(req: types.RelationalRowsJoinRequest) !void {
    if (req.on.len == 0) return error.InvalidArgument;
    if (req.left.row_claim != null or req.right.row_claim != null) return error.UnsupportedQueryRequest;
    if (req.left.doc_key_range != null and req.left.source_cte.len != 0) return error.InvalidQueryRequest;
    if (req.right.doc_key_range != null and req.right.source_cte.len != 0) return error.InvalidQueryRequest;
    try validateJoinMatchExpressionSources(req);
    try validateJoinProjectionOutputs(req.select);
}

fn validateJoinMatchExpressionSources(req: types.RelationalRowsJoinRequest) error{UnsupportedQueryRequest}!void {
    for (req.on_expression_predicates) |condition| try validateJoinMatchExpressionConditionSources(condition);
    for (req.on_expression_or_predicates) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionSources(condition);
    }
    for (req.on_expression_not_predicates) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionSources(condition);
    }
    for (req.on_expression_array_contains) |predicate| try validateJoinMatchExpressionSourcesOne(predicate.expression);
    for (req.match_expression_predicates) |condition| try validateJoinMatchExpressionConditionSources(condition);
    for (req.match_expression_or_predicates) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionSources(condition);
    }
    for (req.match_expression_not_predicates) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionSources(condition);
    }
    for (req.match_expression_array_contains) |predicate| try validateJoinMatchExpressionSourcesOne(predicate.expression);
}

fn validateJoinMatchExpressionConditionSources(condition: types.RelationalRowsExpressionCondition) error{UnsupportedQueryRequest}!void {
    try validateJoinMatchExpressionSourcesOne(condition.lhs);
    for (condition.rhs) |rhs| try validateJoinMatchExpressionSourcesOne(rhs);
}

fn validateJoinMatchExpressionSourcesOne(expression: types.RelationalRowsExpression) error{UnsupportedQueryRequest}!void {
    if (expression.kind == .field and (expression.field_source == .existing or expression.field_source == .proposed)) {
        return error.UnsupportedQueryRequest;
    }
    for (expression.operands) |operand| try validateJoinMatchExpressionSourcesOne(operand);
    for (expression.case_branches) |branch| {
        try validateJoinMatchExpressionConditionSources(branch.when);
        try validateJoinMatchExpressionSourcesOne(branch.then);
    }
    for (expression.case_else) |case_else| try validateJoinMatchExpressionSourcesOne(case_else);
}

fn validateJoinProjectionOutputs(select: []const types.RelationalRowsJoinProjection) !void {
    for (select, 0..) |projection, i| {
        if (projection.output.len == 0 or projection.field.len == 0) return error.InvalidQueryRequest;
        for (select[i + 1 ..]) |other| {
            if (std.mem.eql(u8, projection.output, other.output)) return error.InvalidQueryRequest;
        }
    }
}

pub fn validateLateralPlanCteReferences(plan: types.RelationalRowsLateralPlan) !void {
    try validateMaterializedCtes(plan.ctes, &.{});
    try validateFinalCteReference(plan.ctes, plan.lateral.left.source_cte);
    try validateFinalCteReference(plan.ctes, plan.lateral.right.source_cte);
}

pub fn validateLateralRequest(req: types.RelationalRowsLateralRequest) !void {
    if (req.correlations.len == 0) return error.InvalidArgument;
    if (req.right.limit == null) return error.UnsupportedQueryRequest;
    if (req.left.row_claim != null or req.right.row_claim != null) return error.UnsupportedQueryRequest;
    if (req.left.doc_key_range != null and req.left.source_cte.len != 0) return error.InvalidQueryRequest;
    if (req.right.doc_key_range != null and req.right.source_cte.len != 0) return error.InvalidQueryRequest;
    try validateLateralMatchExpressionSources(req);
    try validateJoinProjectionOutputs(req.select);
}

fn validateLateralMatchExpressionSources(req: types.RelationalRowsLateralRequest) error{UnsupportedQueryRequest}!void {
    for (req.match_expression_predicates) |condition| try validateJoinMatchExpressionConditionSources(condition);
    for (req.match_expression_or_predicates) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionSources(condition);
    }
    for (req.match_expression_not_predicates) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionSources(condition);
    }
    for (req.match_expression_array_contains) |predicate| try validateJoinMatchExpressionSourcesOne(predicate.expression);
}

pub fn validateQueryAgainstPlannedCteOutput(
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsQueryRequest,
) !void {
    const source_output = plannedSourceCteOutputFields(planned_ctes, req) orelse return;
    try validateQueryAgainstCteOutput(req, source_output);
}

pub fn validateAggregateAgainstPlannedCteOutput(
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsAggregateRequest,
) !void {
    const source_output = plannedSourceCteOutputFields(planned_ctes, req.source) orelse return;
    try validateAggregateAgainstOutputFields(source_output, req);
}

pub fn validateWindowAgainstPlannedCteOutput(
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsWindowRequest,
) !void {
    const source_output = plannedSourceCteOutputFields(planned_ctes, req.source) orelse return;
    try validateWindowAgainstOutputFields(source_output, req);
}

pub fn validateJoinAgainstPlannedCteOutput(
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsJoinRequest,
) !void {
    const left_output = plannedSourceCteOutputFields(planned_ctes, req.left);
    const right_output = plannedSourceCteOutputFields(planned_ctes, req.right);
    try validateJoinAgainstOutputFields(left_output, right_output, req);
}

pub fn validateLateralAgainstPlannedCteOutput(
    planned_ctes: []PlannedCte,
    req: types.RelationalRowsLateralRequest,
) !void {
    const left_output = plannedSourceCteOutputFields(planned_ctes, req.left);
    const right_output = plannedSourceCteOutputFields(planned_ctes, req.right);
    try validateLateralAgainstOutputFields(left_output, right_output, req);
}

pub fn validateAggregateAgainstCteOutput(
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsAggregateRequest,
) !void {
    const source_output = sourceCteOutputFields(materialized_ctes, req.source) orelse return;
    try validateAggregateAgainstOutputFields(source_output, req);
}

pub fn validateAggregateAgainstOutputFields(
    source_output: []const []const u8,
    req: types.RelationalRowsAggregateRequest,
) !void {
    for (req.group_by) |field| try validateOutputField(source_output, field);
    for (req.group_expressions) |projection| try validateExpressionAgainstCteOutput(source_output, projection.expression);
    for (req.aggregations) |aggregation| {
        if (aggregation.field) |field| try validateOutputField(source_output, field);
        if (aggregation.expression) |expression| try validateExpressionAgainstCteOutput(source_output, expression);
        for (aggregation.array_order_by) |order| try validateQueryOrderAgainstCteOutput(source_output, order);
        for (aggregation.filter_predicates) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_array_any) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_array_contains) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_array_eq) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_in_predicates) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_json_contains) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_json_path_eq) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_json_path_exists) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_text_patterns) |predicate| try validateOutputField(source_output, predicate.field);
        for (aggregation.filter_expressions) |condition| try validateExpressionConditionAgainstCteOutput(source_output, condition);
        for (aggregation.filter_expression_array_contains) |predicate| try validateExpressionAgainstCteOutput(source_output, predicate.expression);
        for (aggregation.filter_any) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(source_output, condition);
        }
        for (aggregation.filter_not) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(source_output, condition);
        }
    }
}

pub fn validateWindowAgainstCteOutput(
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsWindowRequest,
) !void {
    const source_output = sourceCteOutputFields(materialized_ctes, req.source) orelse return;
    try validateWindowAgainstOutputFields(source_output, req);
}

pub fn validateWindowAgainstOutputFields(
    source_output: []const []const u8,
    req: types.RelationalRowsWindowRequest,
) !void {
    for (req.windows) |window| {
        for (window.partition_by) |field| try validateOutputField(source_output, field);
        for (window.order_by) |order| try validateQueryOrderAgainstCteOutput(source_output, order);
        if (window.value_expression) |expression| try validateExpressionAgainstCteOutput(source_output, expression);
        for (window.filter_predicates) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_array_any) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_array_contains) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_array_eq) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_in_predicates) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_json_contains) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_json_path_eq) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_json_path_exists) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_text_patterns) |predicate| try validateOutputField(source_output, predicate.field);
        for (window.filter_expressions) |condition| try validateExpressionConditionAgainstCteOutput(source_output, condition);
        for (window.filter_expression_array_contains) |predicate| try validateExpressionAgainstCteOutput(source_output, predicate.expression);
        for (window.filter_any) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(source_output, condition);
        }
        for (window.filter_not) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstCteOutput(source_output, condition);
        }
    }
    if (!req.select_all) {
        for (req.select) |field| try validateOutputField(source_output, field);
    }
}

pub fn validateJoinAgainstCteOutput(
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsJoinRequest,
) !void {
    const left_output = sourceCteOutputFields(materialized_ctes, req.left);
    const right_output = sourceCteOutputFields(materialized_ctes, req.right);
    try validateJoinAgainstOutputFields(left_output, right_output, req);
}

pub fn validateJoinAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    req: types.RelationalRowsJoinRequest,
) !void {
    if (left_output) |fields| try validateQueryAgainstCteOutput(joinSideSource(req.left), fields);
    if (right_output) |fields| try validateQueryAgainstCteOutput(joinSideSource(req.right), fields);
    for (req.on) |join_on| {
        if (left_output) |fields| try validateOutputField(fields, join_on.left_field);
        if (right_output) |fields| try validateOutputField(fields, join_on.right_field);
    }
    for (req.select) |projection| switch (projection.side) {
        .left => if (left_output) |fields| try validateOutputField(fields, projection.field),
        .right => if (right_output) |fields| try validateOutputField(fields, projection.field),
    };
    try validateJoinMatchExpressionsAgainstOutputFields(left_output, right_output, req.on_expression_predicates, req.on_expression_or_predicates, req.on_expression_not_predicates, req.on_expression_array_contains);
    try validateJoinMatchExpressionsAgainstOutputFields(left_output, right_output, req.match_expression_predicates, req.match_expression_or_predicates, req.match_expression_not_predicates, req.match_expression_array_contains);
}

pub fn validateLateralAgainstCteOutput(
    materialized_ctes: []MaterializedCte,
    req: types.RelationalRowsLateralRequest,
) !void {
    const left_output = sourceCteOutputFields(materialized_ctes, req.left);
    const right_output = sourceCteOutputFields(materialized_ctes, req.right);
    try validateLateralAgainstOutputFields(left_output, right_output, req);
}

pub fn validateLateralAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    req: types.RelationalRowsLateralRequest,
) !void {
    if (left_output) |fields| try validateQueryAgainstCteOutput(joinSideSource(req.left), fields);
    if (right_output) |fields| try validateQueryAgainstCteOutput(joinSideSource(req.right), fields);
    for (req.correlations) |correlation| {
        if (left_output) |fields| try validateOutputField(fields, correlation.left_field);
        if (right_output) |fields| try validateOutputField(fields, correlation.right_field);
    }
    for (req.select) |projection| switch (projection.side) {
        .left => if (left_output) |fields| try validateOutputField(fields, projection.field),
        .right => if (right_output) |fields| try validateOutputField(fields, projection.field),
    };
    try validateJoinMatchExpressionsAgainstOutputFields(left_output, right_output, req.match_expression_predicates, req.match_expression_or_predicates, req.match_expression_not_predicates, req.match_expression_array_contains);
}

fn validateJoinMatchExpressionsAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    predicates: []const types.RelationalRowsExpressionCondition,
    any_groups: []const types.RelationalRowsExpressionPredicateGroup,
    not_groups: []const types.RelationalRowsExpressionPredicateGroup,
    array_contains: []const types.RelationalRowsExpressionArrayContainsPredicate,
) !void {
    for (predicates) |condition| try validateJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, condition);
    for (any_groups) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, condition);
    }
    for (not_groups) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, condition);
    }
    for (array_contains) |predicate| try validateJoinMatchExpressionAgainstOutputFields(left_output, right_output, predicate.expression);
}

fn validateJoinMatchExpressionConditionAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateJoinMatchExpressionAgainstOutputFields(left_output, right_output, condition.lhs);
    for (condition.rhs) |expression| try validateJoinMatchExpressionAgainstOutputFields(left_output, right_output, expression);
}

fn validateJoinMatchExpressionAgainstOutputFields(
    left_output: ?[]const []const u8,
    right_output: ?[]const []const u8,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        switch (expression.field_source) {
            .source => if (right_output) |fields| try validateOutputField(fields, expression.field),
            .row => if (left_output) |fields| try validateOutputField(fields, expression.field),
            .existing, .proposed => return error.InvalidQueryRequest,
        }
    }
    for (expression.operands) |operand| try validateJoinMatchExpressionAgainstOutputFields(left_output, right_output, operand);
    for (expression.case_branches) |branch| {
        try validateJoinMatchExpressionConditionAgainstOutputFields(left_output, right_output, branch.when);
        try validateJoinMatchExpressionAgainstOutputFields(left_output, right_output, branch.then);
    }
    for (expression.case_else) |fallback| try validateJoinMatchExpressionAgainstOutputFields(left_output, right_output, fallback);
}

pub fn joinSideSource(source_req: types.RelationalRowsQueryRequest) types.RelationalRowsQueryRequest {
    var source = source_req;
    source.select = &.{};
    source.select_all = true;
    return source;
}

pub fn lateralLeftSource(source_req: types.RelationalRowsQueryRequest) types.RelationalRowsQueryRequest {
    return joinSideSource(source_req);
}

pub fn aggregateSourceQuery(req: types.RelationalRowsAggregateRequest) types.RelationalRowsQueryRequest {
    var source = req.source;
    source.select = &.{};
    source.select_all = true;
    source.order_by = &.{};
    source.limit = null;
    source.offset = 0;
    return source;
}

pub fn windowSourceQuery(
    req: types.RelationalRowsWindowRequest,
    source_order: []const types.RelationalRowsQueryOrder,
) types.RelationalRowsQueryRequest {
    var source = req.source;
    source.select = &.{};
    source.json_extract = &.{};
    source.array_length = &.{};
    source.coalesce = &.{};
    source.field_aliases = &.{};
    source.select_all = true;
    source.order_by = source_order;
    source.limit = null;
    source.offset = 0;
    source.row_claim = null;
    return source;
}

pub fn joinedMutationTargetQuery(req: types.RelationalRowsJoinedMutationSourceRequest) types.RelationalRowsQueryRequest {
    return switch (req.target_side) {
        .left => req.join.left,
        .right => req.join.right,
    };
}

pub fn joinedMutationSourceQuery(req: types.RelationalRowsJoinedMutationSourceRequest) types.RelationalRowsQueryRequest {
    return switch (req.target_side) {
        .left => req.join.right,
        .right => req.join.left,
    };
}

pub fn joinedMutationTargetSource(req: types.RelationalRowsJoinedMutationSourceRequest) types.RelationalRowsQueryRequest {
    var source = joinedMutationTargetQuery(req);
    source.select = &.{};
    source.select_all = true;
    source.row_claim = null;
    source.limit = null;
    source.offset = 0;
    return source;
}

pub fn joinedMutationSourceSide(req: types.RelationalRowsJoinedMutationSourceRequest) types.RelationalRowsQueryRequest {
    var source = joinedMutationSourceQuery(req);
    source.select = &.{};
    source.select_all = true;
    source.row_claim = null;
    return source;
}

pub fn joinedMutationClaim(req: types.RelationalRowsJoinedMutationSourceRequest) ?types.RowClaimRequest {
    return joinedMutationTargetQuery(req).row_claim;
}

pub fn joinedMutationTargetJoinSide(req: types.RelationalRowsJoinedMutationSourceRequest) JoinSide {
    return switch (req.target_side) {
        .left => .left,
        .right => .right,
    };
}

pub fn joinedMutationSourceJoinSide(req: types.RelationalRowsJoinedMutationSourceRequest) JoinSide {
    return switch (req.target_side) {
        .left => .right,
        .right => .left,
    };
}

pub fn joinHasMatchPredicates(req: types.RelationalRowsJoinRequest) bool {
    return req.match_expression_predicates.len != 0 or
        req.match_expression_or_predicates.len != 0 or
        req.match_expression_not_predicates.len != 0 or
        req.match_expression_array_contains.len != 0;
}

pub fn joinHasOnExpressionPredicates(req: types.RelationalRowsJoinRequest) bool {
    return req.on_expression_predicates.len != 0 or
        req.on_expression_or_predicates.len != 0 or
        req.on_expression_not_predicates.len != 0 or
        req.on_expression_array_contains.len != 0;
}

pub fn lateralHasMatchPredicates(req: types.RelationalRowsLateralRequest) bool {
    return req.match_expression_predicates.len != 0 or
        req.match_expression_or_predicates.len != 0 or
        req.match_expression_not_predicates.len != 0 or
        req.match_expression_array_contains.len != 0;
}

pub fn validateJoinedMutationCteReferences(req: types.RelationalRowsJoinedMutationSourceRequest) !void {
    const target = joinedMutationTargetQuery(req);
    const source = joinedMutationSourceQuery(req);
    if (target.source_cte.len != 0) return error.UnsupportedQueryRequest;
    if (req.ctes.len == 0) {
        if (source.source_cte.len != 0) return error.InvalidQueryRequest;
        return;
    }
    if (source.source_cte.len == 0) return error.InvalidQueryRequest;
    try validateMaterializedCtes(req.ctes, &.{});
    try validateFinalCteReference(req.ctes, source.source_cte);
}

pub fn validateJoinedMutationJoinFieldsForSide(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsJoinedMutationSourceRequest,
    side: JoinSide,
) !void {
    for (req.join.on) |predicate| {
        const field = switch (side) {
            .left => predicate.left_field,
            .right => predicate.right_field,
        };
        _ = findColumn(runtime_schema.relational_columns, field) orelse return error.InvalidQueryRequest;
    }
}

pub fn validateJoinedMutationJoinFieldsForOutputFields(
    output_fields: []const []const u8,
    req: types.RelationalRowsJoinedMutationSourceRequest,
    side: JoinSide,
) !void {
    for (req.join.on) |predicate| {
        const field = switch (side) {
            .left => predicate.left_field,
            .right => predicate.right_field,
        };
        try validateOutputField(output_fields, field);
    }
}

pub fn validateJoinedMutationMatchExpressions(
    target_schema: schema_mod.TableSchema,
    source_schema: schema_mod.TableSchema,
    req: types.RelationalRowsJoinedMutationSourceRequest,
) !void {
    for (req.match_expression_predicates) |condition| {
        try validateJoinedMutationMatchExpressionCondition(target_schema, source_schema, condition);
    }
    for (req.match_expression_or_predicates) |group| {
        for (group.conditions) |condition| {
            try validateJoinedMutationMatchExpressionCondition(target_schema, source_schema, condition);
        }
    }
    for (req.match_expression_not_predicates) |group| {
        for (group.conditions) |condition| {
            try validateJoinedMutationMatchExpressionCondition(target_schema, source_schema, condition);
        }
    }
    for (req.match_expression_array_contains) |predicate| {
        try validateJoinedMutationMatchExpression(target_schema, source_schema, predicate.expression);
    }
}

fn validateJoinedMutationMatchExpressionCondition(
    target_schema: schema_mod.TableSchema,
    source_schema: schema_mod.TableSchema,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateJoinedMutationMatchExpression(target_schema, source_schema, condition.lhs);
    for (condition.rhs) |rhs| try validateJoinedMutationMatchExpression(target_schema, source_schema, rhs);
}

fn validateJoinedMutationMatchExpression(
    target_schema: schema_mod.TableSchema,
    source_schema: schema_mod.TableSchema,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        if (expression.field_source == .existing or expression.field_source == .proposed) return error.InvalidQueryRequest;
        const schema = if (expression.field_source == .source) source_schema else target_schema;
        _ = findColumn(schema.relational_columns, expression.field) orelse return error.InvalidQueryRequest;
    }
    for (expression.operands) |operand| try validateJoinedMutationMatchExpression(target_schema, source_schema, operand);
    for (expression.case_branches) |branch| {
        try validateJoinedMutationMatchExpressionCondition(target_schema, source_schema, branch.when);
        try validateJoinedMutationMatchExpression(target_schema, source_schema, branch.then);
    }
    for (expression.case_else) |case_else| try validateJoinedMutationMatchExpression(target_schema, source_schema, case_else);
}

pub fn validateMutationReturningRequestOutputs(
    runtime_schema: schema_mod.TableSchema,
    fields: []const []const u8,
    returning_all: bool,
    expressions: []const types.RelationalRowsExpressionProjection,
) !void {
    if (returning_all and fields.len != 0) return error.InvalidQueryRequest;
    for (fields) |field| {
        if (field.len == 0) return error.InvalidQueryRequest;
        if (findColumn(runtime_schema.relational_columns, field) == null and
            !jsonColumnSubpathIsValid(runtime_schema.relational_columns, field))
        {
            return error.InvalidQueryRequest;
        }
        if (mutationReturningOutputCount(fields, expressions, field) > 1) return error.InvalidQueryRequest;
    }
    for (expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (returning_all and findColumn(runtime_schema.relational_columns, projection.output) != null) return error.InvalidQueryRequest;
        if (mutationReturningOutputCount(fields, expressions, projection.output) > 1) return error.InvalidQueryRequest;
    }
}

pub fn validateMutationReturningTargetExpressions(
    runtime_schema: schema_mod.TableSchema,
    expressions: []const types.RelationalRowsExpressionProjection,
) !void {
    for (expressions) |projection| try validateMutationReturningTargetExpression(runtime_schema, projection.expression);
}

fn validateMutationReturningTargetExpressionCondition(
    runtime_schema: schema_mod.TableSchema,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateMutationReturningTargetExpression(runtime_schema, condition.lhs);
    for (condition.rhs) |rhs| try validateMutationReturningTargetExpression(runtime_schema, rhs);
}

fn validateMutationReturningTargetExpression(
    runtime_schema: schema_mod.TableSchema,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        if (expression.field_source == .source or expression.field_source == .proposed) return error.InvalidQueryRequest;
        _ = findColumn(runtime_schema.relational_columns, expression.field) orelse return error.InvalidQueryRequest;
    }
    for (expression.operands) |operand| try validateMutationReturningTargetExpression(runtime_schema, operand);
    for (expression.case_branches) |branch| {
        try validateMutationReturningTargetExpressionCondition(runtime_schema, branch.when);
        try validateMutationReturningTargetExpression(runtime_schema, branch.then);
    }
    for (expression.case_else) |case_else| try validateMutationReturningTargetExpression(runtime_schema, case_else);
}

pub fn validateJoinedMutationReturningExpressions(
    target_schema: schema_mod.TableSchema,
    source_schema: schema_mod.TableSchema,
    expressions: []const types.RelationalRowsExpressionProjection,
) !void {
    for (expressions) |projection| {
        try validateJoinedMutationReturningExpression(target_schema, source_schema, projection.expression);
    }
}

pub fn validateJoinedMutationReturningExpression(
    target_schema: schema_mod.TableSchema,
    source_schema: schema_mod.TableSchema,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        if (expression.field_source == .existing or expression.field_source == .proposed) return error.InvalidQueryRequest;
        const schema = if (expression.field_source == .source) source_schema else target_schema;
        _ = findColumn(schema.relational_columns, expression.field) orelse return error.InvalidQueryRequest;
    }
    for (expression.operands) |operand| try validateJoinedMutationReturningExpression(target_schema, source_schema, operand);
    for (expression.case_branches) |branch| {
        try validateJoinedMutationReturningExpressionCondition(target_schema, source_schema, branch.when);
        try validateJoinedMutationReturningExpression(target_schema, source_schema, branch.then);
    }
    for (expression.case_else) |case_else| try validateJoinedMutationReturningExpression(target_schema, source_schema, case_else);
}

fn validateJoinedMutationReturningExpressionCondition(
    target_schema: schema_mod.TableSchema,
    source_schema: schema_mod.TableSchema,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateJoinedMutationReturningExpression(target_schema, source_schema, condition.lhs);
    for (condition.rhs) |rhs| try validateJoinedMutationReturningExpression(target_schema, source_schema, rhs);
}

fn findColumn(columns: []const schema_mod.RelationalColumn, name: []const u8) ?schema_mod.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.path, name) or std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn jsonColumnSubpathIsValid(columns: []const schema_mod.RelationalColumn, field: []const u8) bool {
    const dot = std.mem.indexOfScalar(u8, field, '.') orelse return false;
    if (dot == 0 or dot + 1 >= field.len) return false;
    const root = field[0..dot];
    const column = findColumn(columns, root) orelse return false;
    return column.field_type == .json;
}

pub fn validateAggregateAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsAggregateRequest,
) !void {
    for (req.group_by) |field| try validateSchemaField(runtime_schema, field);
    for (req.group_expressions) |projection| try validateExpressionAgainstSchema(runtime_schema, projection.expression);
    for (req.aggregations) |aggregation| {
        if (aggregation.field) |field| try validateSchemaField(runtime_schema, field);
        if (aggregation.expression) |expression| try validateExpressionAgainstSchema(runtime_schema, expression);
        for (aggregation.array_order_by) |order| try validateQueryOrderAgainstSchema(runtime_schema, order);
        for (aggregation.filter_predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_array_any) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_array_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_array_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_in_predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_json_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_json_path_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_json_path_exists) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_text_patterns) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (aggregation.filter_expressions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
        for (aggregation.filter_expression_array_contains) |predicate| try validateExpressionAgainstSchema(runtime_schema, predicate.expression);
        for (aggregation.filter_any) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
        }
        for (aggregation.filter_not) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
        }
    }
}

pub fn validateWindowAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsWindowRequest,
) !void {
    for (req.windows) |window| {
        for (window.partition_by) |field| try validateSchemaField(runtime_schema, field);
        for (window.order_by) |order| try validateQueryOrderAgainstSchema(runtime_schema, order);
        if (window.value_expression) |expression| try validateExpressionAgainstSchema(runtime_schema, expression);
        for (window.filter_predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_array_any) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_array_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_array_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_in_predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_json_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_json_path_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_json_path_exists) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_text_patterns) |predicate| try validateSchemaField(runtime_schema, predicate.field);
        for (window.filter_expressions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
        for (window.filter_expression_array_contains) |predicate| try validateExpressionAgainstSchema(runtime_schema, predicate.expression);
        for (window.filter_any) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
        }
        for (window.filter_not) |group| {
            for (group.conditions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
        }
    }
    if (!req.select_all) {
        for (req.select) |field| try validateSchemaField(runtime_schema, field);
    }
}

pub fn validateQueryOrderAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    order: types.RelationalRowsQueryOrder,
) !void {
    if (order.field.len > 0) try validateSchemaField(runtime_schema, order.field);
    if (order.expression) |expression| try validateExpressionAgainstSchema(runtime_schema, expression);
}

pub fn validateJoinAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsJoinRequest,
) !void {
    const validate_left = req.left.source_cte.len == 0;
    const validate_right = req.right.source_cte.len == 0;
    if (validate_left) try validateQueryAgainstSchema(runtime_schema, joinSideSource(req.left));
    if (validate_right) try validateQueryAgainstSchema(runtime_schema, joinSideSource(req.right));
    for (req.on) |join_on| {
        if (validate_left) try validateSchemaField(runtime_schema, join_on.left_field);
        if (validate_right) try validateSchemaField(runtime_schema, join_on.right_field);
    }
    for (req.select) |projection| switch (projection.side) {
        .left => if (validate_left) try validateSchemaField(runtime_schema, projection.field),
        .right => if (validate_right) try validateSchemaField(runtime_schema, projection.field),
    };
    try validateJoinMatchExpressionsAgainstSchema(runtime_schema, validate_left, validate_right, req.on_expression_predicates, req.on_expression_or_predicates, req.on_expression_not_predicates, req.on_expression_array_contains);
    try validateJoinMatchExpressionsAgainstSchema(runtime_schema, validate_left, validate_right, req.match_expression_predicates, req.match_expression_or_predicates, req.match_expression_not_predicates, req.match_expression_array_contains);
}

pub fn validateLateralAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsLateralRequest,
) !void {
    const validate_left = req.left.source_cte.len == 0;
    const validate_right = req.right.source_cte.len == 0;
    if (validate_left) try validateQueryAgainstSchema(runtime_schema, joinSideSource(req.left));
    if (validate_right) try validateQueryAgainstSchema(runtime_schema, joinSideSource(req.right));
    for (req.correlations) |correlation| {
        if (validate_left) try validateSchemaField(runtime_schema, correlation.left_field);
        if (validate_right) try validateSchemaField(runtime_schema, correlation.right_field);
    }
    for (req.select) |projection| switch (projection.side) {
        .left => if (validate_left) try validateSchemaField(runtime_schema, projection.field),
        .right => if (validate_right) try validateSchemaField(runtime_schema, projection.field),
    };
    try validateJoinMatchExpressionsAgainstSchema(runtime_schema, validate_left, validate_right, req.match_expression_predicates, req.match_expression_or_predicates, req.match_expression_not_predicates, req.match_expression_array_contains);
}

fn validateJoinMatchExpressionsAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    validate_left: bool,
    validate_right: bool,
    predicates: []const types.RelationalRowsExpressionCondition,
    any_groups: []const types.RelationalRowsExpressionPredicateGroup,
    not_groups: []const types.RelationalRowsExpressionPredicateGroup,
    array_contains: []const types.RelationalRowsExpressionArrayContainsPredicate,
) !void {
    for (predicates) |condition| try validateJoinMatchExpressionConditionAgainstSchema(runtime_schema, validate_left, validate_right, condition);
    for (any_groups) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionAgainstSchema(runtime_schema, validate_left, validate_right, condition);
    }
    for (not_groups) |group| {
        for (group.conditions) |condition| try validateJoinMatchExpressionConditionAgainstSchema(runtime_schema, validate_left, validate_right, condition);
    }
    for (array_contains) |predicate| try validateJoinMatchExpressionAgainstSchema(runtime_schema, validate_left, validate_right, predicate.expression);
}

fn validateJoinMatchExpressionConditionAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    validate_left: bool,
    validate_right: bool,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateJoinMatchExpressionAgainstSchema(runtime_schema, validate_left, validate_right, condition.lhs);
    for (condition.rhs) |expression| try validateJoinMatchExpressionAgainstSchema(runtime_schema, validate_left, validate_right, expression);
}

fn validateJoinMatchExpressionAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    validate_left: bool,
    validate_right: bool,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        switch (expression.field_source) {
            .row => if (validate_left) try validateSchemaField(runtime_schema, expression.field),
            .source => if (validate_right) try validateSchemaField(runtime_schema, expression.field),
            .existing, .proposed => return error.InvalidQueryRequest,
        }
    }
    for (expression.operands) |operand| try validateJoinMatchExpressionAgainstSchema(runtime_schema, validate_left, validate_right, operand);
    for (expression.case_branches) |branch| {
        try validateJoinMatchExpressionConditionAgainstSchema(runtime_schema, validate_left, validate_right, branch.when);
        try validateJoinMatchExpressionAgainstSchema(runtime_schema, validate_left, validate_right, branch.then);
    }
    for (expression.case_else) |fallback| try validateJoinMatchExpressionAgainstSchema(runtime_schema, validate_left, validate_right, fallback);
}

pub fn validateBaseQueryRequestAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsQueryRequest,
) !void {
    try validateBaseQueryRequest(req);
    try validateQueryAgainstSchema(runtime_schema, req);
    if (!req.select_all) return;
    for (runtime_schema.relational_columns) |column| {
        try validateQueryProjectionOutputDoesNotCollide(req, column.name);
        if (!std.mem.eql(u8, column.path, column.name)) {
            try validateQueryProjectionOutputDoesNotCollide(req, column.path);
        }
    }
}

pub fn validateQueryAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    req: types.RelationalRowsQueryRequest,
) !void {
    for (req.predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.array_any) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.array_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.array_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.in_predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.json_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.json_path_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.json_path_exists) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.text_patterns) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (req.or_predicates) |group| {
        for (group.predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    }
    for (req.not_predicates) |group| {
        for (group.predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    }
    for (req.access_or_predicates) |group| try validateAccessPredicateGroupAgainstSchema(runtime_schema, group);
    for (req.access_not_predicates) |group| try validateAccessPredicateGroupAgainstSchema(runtime_schema, group);
    for (req.expression_predicates) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
    for (req.expression_or_predicates) |group| {
        for (group.conditions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
    }
    for (req.expression_not_predicates) |group| {
        for (group.conditions) |condition| try validateExpressionConditionAgainstSchema(runtime_schema, condition);
    }
    for (req.expression_array_contains) |predicate| try validateExpressionAgainstSchema(runtime_schema, predicate.expression);
    if (!req.select_all) {
        for (req.select) |field| try validateSchemaField(runtime_schema, field);
    }
    for (req.distinct_on) |field| try validateSchemaField(runtime_schema, field);
    for (req.distinct_on_expressions) |expression| try validateExpressionAgainstSchema(runtime_schema, expression);
    for (req.order_by) |order| {
        if (order.field.len > 0) try validateSchemaField(runtime_schema, order.field);
        if (order.expression) |expression| try validateExpressionAgainstSchema(runtime_schema, expression);
    }
    for (req.json_extract) |projection| try validateSchemaField(runtime_schema, projection.field);
    for (req.array_length) |projection| try validateSchemaField(runtime_schema, projection.field);
    for (req.coalesce) |projection| {
        for (projection.operands) |operand| {
            if (operand.kind == .field) try validateSchemaField(runtime_schema, operand.field);
        }
    }
    for (req.field_aliases) |projection| try validateSchemaField(runtime_schema, projection.field);
    for (req.expressions) |projection| try validateExpressionAgainstSchema(runtime_schema, projection.expression);
}

fn validateAccessPredicateGroupAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    group: types.RelationalRowsAccessPredicateGroup,
) !void {
    for (group.predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.array_any) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.array_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.array_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.in_predicates) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.json_contains) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.json_path_eq) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.json_path_exists) |predicate| try validateSchemaField(runtime_schema, predicate.field);
    for (group.text_patterns) |predicate| try validateSchemaField(runtime_schema, predicate.field);
}

pub fn validateExpressionConditionAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    condition: types.RelationalRowsExpressionCondition,
) anyerror!void {
    try validateExpressionAgainstSchema(runtime_schema, condition.lhs);
    for (condition.rhs) |expression| try validateExpressionAgainstSchema(runtime_schema, expression);
}

pub fn validateExpressionAgainstSchema(
    runtime_schema: schema_mod.TableSchema,
    expression: types.RelationalRowsExpression,
) anyerror!void {
    if (expression.kind == .field) {
        if (expression.field_source != .row) return error.InvalidQueryRequest;
        try validateSchemaField(runtime_schema, expression.field);
    }
    for (expression.operands) |operand| try validateExpressionAgainstSchema(runtime_schema, operand);
    for (expression.case_branches) |branch| {
        try validateExpressionConditionAgainstSchema(runtime_schema, branch.when);
        try validateExpressionAgainstSchema(runtime_schema, branch.then);
    }
    for (expression.case_else) |fallback| try validateExpressionAgainstSchema(runtime_schema, fallback);
}

pub fn validateSchemaField(runtime_schema: schema_mod.TableSchema, field: []const u8) !void {
    if (field.len == 0 or !schemaCoversField(runtime_schema, field)) return error.InvalidQueryRequest;
}

pub fn validateDocKeyRanges(ranges: []const types.RelationalRowsDocKeyRange) !void {
    var previous_end: ?[]const u8 = null;
    for (ranges, 0..) |range, i| {
        if (range.start.len == 0 and range.end.len == 0) return error.InvalidQueryRequest;
        if (range.start.len > 0 and range.end.len > 0 and std.mem.order(u8, range.start, range.end) != .lt) return error.InvalidQueryRequest;
        if (i > 0 and range.start.len == 0) return error.InvalidQueryRequest;
        if (previous_end) |end| {
            if (end.len == 0) return error.InvalidQueryRequest;
            if (std.mem.order(u8, range.start, end) == .lt) return error.InvalidQueryRequest;
        }
        previous_end = range.end;
    }
}

pub fn planRangesForJoinAlloc(
    alloc: Allocator,
    left_ranges: []const types.RelationalRowsDocKeyRange,
    right_ranges: []const types.RelationalRowsDocKeyRange,
) ![]const types.RelationalRowsDocKeyRange {
    if ((left_ranges.len == 0) != (right_ranges.len == 0)) return error.InvalidQueryRequest;
    if (left_ranges.len == 0) return &.{};
    try validateDocKeyRanges(left_ranges);
    try validateDocKeyRanges(right_ranges);
    const sorted = try alloc.alloc(types.RelationalRowsDocKeyRange, left_ranges.len + right_ranges.len);
    defer alloc.free(sorted);
    @memcpy(sorted[0..left_ranges.len], left_ranges);
    @memcpy(sorted[left_ranges.len..], right_ranges);
    std.sort.pdq(types.RelationalRowsDocKeyRange, sorted, {}, docKeyRangeLessThan);

    var out = std.ArrayListUnmanaged(types.RelationalRowsDocKeyRange).empty;
    errdefer out.deinit(alloc);
    for (sorted) |range| {
        if (out.items.len == 0) {
            try out.append(alloc, range);
            continue;
        }
        const last = &out.items[out.items.len - 1];
        if (docKeyRangesOverlapOrTouch(last.*, range)) {
            last.end = docKeyRangeMaxEnd(last.end, range.end);
        } else {
            try out.append(alloc, range);
        }
    }
    return try out.toOwnedSlice(alloc);
}

fn docKeyRangesOverlapOrTouch(lhs: types.RelationalRowsDocKeyRange, rhs: types.RelationalRowsDocKeyRange) bool {
    if (lhs.end.len == 0) return true;
    if (rhs.start.len == 0) return true;
    return std.mem.order(u8, rhs.start, lhs.end) != .gt;
}

fn docKeyRangeMaxEnd(lhs: []const u8, rhs: []const u8) []const u8 {
    if (lhs.len == 0 or rhs.len == 0) return "";
    return if (std.mem.order(u8, lhs, rhs) == .lt) rhs else lhs;
}

fn docKeyRangeLessThan(_: void, lhs: types.RelationalRowsDocKeyRange, rhs: types.RelationalRowsDocKeyRange) bool {
    if (lhs.start.len == 0) return rhs.start.len != 0;
    if (rhs.start.len == 0) return false;
    const start_order = std.mem.order(u8, lhs.start, rhs.start);
    if (start_order != .eq) return start_order == .lt;
    if (lhs.end.len == 0) return false;
    if (rhs.end.len == 0) return true;
    return std.mem.order(u8, lhs.end, rhs.end) == .lt;
}

fn schemaCoversField(runtime_schema: schema_mod.TableSchema, field: []const u8) bool {
    for (runtime_schema.relational_columns) |column| {
        if (fieldCoversName(column.name, field)) return true;
        if (!std.mem.eql(u8, column.path, column.name) and fieldCoversName(column.path, field)) return true;
    }
    return false;
}

fn fieldCoversName(source: []const u8, field: []const u8) bool {
    if (std.mem.eql(u8, source, field)) return true;
    return field.len > source.len and std.mem.startsWith(u8, field, source) and field[source.len] == '.';
}

pub fn validateMutationUpdateTargetPaths(
    operations: []const types.TransformOp,
    patch_expressions: []const types.RelationalRowsExpressionAssignment,
    increment_expressions: []const types.RelationalRowsExpressionAssignment,
    json_set_expressions: []const types.RelationalRowsJsonSetExpressionAssignment,
    source_assignments: []const types.RelationalRowsJoinedMutationFieldAssignment,
) !void {
    for (operations, 0..) |lhs, i| {
        for (operations[i + 1 ..]) |rhs| {
            if (mutationOperationPathsConflict(lhs, rhs)) return error.InvalidQueryRequest;
        }
        for (patch_expressions) |assignment| {
            if (dottedPathsConflict(lhs.path, assignment.field)) return error.InvalidQueryRequest;
        }
        for (increment_expressions) |assignment| {
            if (dottedPathsConflict(lhs.path, assignment.field)) return error.InvalidQueryRequest;
        }
        for (json_set_expressions) |assignment| {
            if (dottedPathConflictsJsonSetPath(lhs.path, assignment.field, assignment.path)) return error.InvalidQueryRequest;
        }
        for (source_assignments) |assignment| {
            if (dottedPathsConflict(lhs.path, assignment.field)) return error.InvalidQueryRequest;
        }
    }
    for (patch_expressions, 0..) |lhs, i| {
        for (patch_expressions[i + 1 ..]) |rhs| {
            if (dottedPathsConflict(lhs.field, rhs.field)) return error.InvalidQueryRequest;
        }
        for (increment_expressions) |assignment| {
            if (dottedPathsConflict(lhs.field, assignment.field)) return error.InvalidQueryRequest;
        }
        for (json_set_expressions) |assignment| {
            if (dottedPathConflictsJsonSetPath(lhs.field, assignment.field, assignment.path)) return error.InvalidQueryRequest;
        }
        for (source_assignments) |assignment| {
            if (dottedPathsConflict(lhs.field, assignment.field)) return error.InvalidQueryRequest;
        }
    }
    for (increment_expressions, 0..) |lhs, i| {
        for (increment_expressions[i + 1 ..]) |rhs| {
            if (dottedPathsConflict(lhs.field, rhs.field)) return error.InvalidQueryRequest;
        }
        for (json_set_expressions) |assignment| {
            if (dottedPathConflictsJsonSetPath(lhs.field, assignment.field, assignment.path)) return error.InvalidQueryRequest;
        }
        for (source_assignments) |assignment| {
            if (dottedPathsConflict(lhs.field, assignment.field)) return error.InvalidQueryRequest;
        }
    }
    for (json_set_expressions, 0..) |lhs, i| {
        for (json_set_expressions[i + 1 ..]) |rhs| {
            if (jsonSetPathsConflict(lhs.field, lhs.path, rhs.field, rhs.path)) return error.InvalidQueryRequest;
        }
        for (source_assignments) |assignment| {
            if (dottedPathConflictsJsonSetPath(assignment.field, lhs.field, lhs.path)) return error.InvalidQueryRequest;
        }
    }
    for (source_assignments, 0..) |lhs, i| {
        for (source_assignments[i + 1 ..]) |rhs| {
            if (dottedPathsConflict(lhs.field, rhs.field)) return error.InvalidQueryRequest;
        }
    }
}

fn mutationOperationPathsConflict(lhs: types.TransformOp, rhs: types.TransformOp) bool {
    if (!dottedPathsConflict(lhs.path, rhs.path)) return false;
    return !(transformOpIsArrayUpdate(lhs.op) and transformOpIsArrayUpdate(rhs.op) and std.mem.eql(u8, lhs.path, rhs.path));
}

fn transformOpIsArrayUpdate(op: types.TransformOpType) bool {
    return switch (op) {
        .push, .pull, .add_to_set, .pop => true,
        else => false,
    };
}

fn dottedPathsConflict(lhs: []const u8, rhs: []const u8) bool {
    if (std.mem.eql(u8, lhs, rhs)) return true;
    return dottedPathIsAncestor(lhs, rhs) or dottedPathIsAncestor(rhs, lhs);
}

fn dottedPathIsAncestor(parent: []const u8, child: []const u8) bool {
    return parent.len < child.len and
        std.mem.startsWith(u8, child, parent) and
        child[parent.len] == '.';
}

fn dottedPathConflictsJsonSetPath(path: []const u8, json_field: []const u8, json_path: []const []const u8) bool {
    if (dottedPathsConflict(path, json_field)) return true;
    if (path.len <= json_field.len + 1) return false;
    if (!std.mem.startsWith(u8, path, json_field) or path[json_field.len] != '.') return false;
    return jsonSegmentsConflictDottedPath(json_path, path[json_field.len + 1 ..]);
}

fn jsonSetPathsConflict(
    lhs_field: []const u8,
    lhs_path: []const []const u8,
    rhs_field: []const u8,
    rhs_path: []const []const u8,
) bool {
    if (!std.mem.eql(u8, lhs_field, rhs_field)) return dottedPathsConflict(lhs_field, rhs_field);
    const shared = @min(lhs_path.len, rhs_path.len);
    for (lhs_path[0..shared], rhs_path[0..shared]) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs, rhs)) return false;
    }
    return true;
}

fn jsonSegmentsConflictDottedPath(json_path: []const []const u8, dotted_path: []const u8) bool {
    if (json_path.len == 0 or dotted_path.len == 0) return false;
    var offset: usize = 0;
    for (json_path, 0..) |segment, i| {
        if (offset >= dotted_path.len) return true;
        if (!std.mem.startsWith(u8, dotted_path[offset..], segment)) return false;
        offset += segment.len;
        const dotted_done = offset == dotted_path.len;
        const json_done = i + 1 == json_path.len;
        if (!dotted_done and dotted_path[offset] != '.') return false;
        if (dotted_done or json_done) return true;
        offset += 1;
    }
    return offset == dotted_path.len;
}

pub fn validateMutationReturningRowOutputs(
    row: std.json.Value,
    fields: []const []const u8,
    returning_all: bool,
    expressions: []const types.RelationalRowsExpressionProjection,
) !void {
    if (row != .object) return error.InvalidQueryRequest;
    if (returning_all and fields.len != 0) return error.InvalidQueryRequest;
    for (fields) |field| {
        if (field.len == 0) return error.InvalidQueryRequest;
        if (mutationReturningOutputCount(fields, expressions, field) > 1) return error.InvalidQueryRequest;
    }
    for (expressions) |projection| {
        if (projection.output.len == 0) return error.InvalidQueryRequest;
        if (returning_all and row.object.get(projection.output) != null) return error.InvalidQueryRequest;
        if (mutationReturningOutputCount(fields, expressions, projection.output) > 1) return error.InvalidQueryRequest;
    }
}

pub fn mutationReturningOutputCount(
    fields: []const []const u8,
    expressions: []const types.RelationalRowsExpressionProjection,
    output: []const u8,
) usize {
    var count: usize = 0;
    for (fields) |field| {
        if (std.mem.eql(u8, field, output)) count += 1;
    }
    for (expressions) |projection| {
        if (std.mem.eql(u8, projection.output, output)) count += 1;
    }
    return count;
}

pub fn validateWindowRequestAlloc(
    alloc: Allocator,
    req: types.RelationalRowsWindowRequest,
) !void {
    const source_order = try validateWindowRequestAndSourceOrderAlloc(alloc, req);
    alloc.free(source_order);
}

pub fn validateWindowRequestAndSourceOrderAlloc(
    alloc: Allocator,
    req: types.RelationalRowsWindowRequest,
) ![]types.RelationalRowsQueryOrder {
    if (req.windows.len == 0) return error.InvalidQueryRequest;
    if (req.source.row_claim != null) return error.UnsupportedQueryRequest;
    if (req.source.doc_key_range != null and req.source.source_cte.len != 0) return error.InvalidQueryRequest;
    try validateWindowOutputNames(req);
    const first_window = req.windows[0];
    for (req.windows) |window| {
        if (window.output.len == 0) return error.InvalidQueryRequest;
        try validateWindowFrameSpec(window);
        switch (window.function) {
            .row_number, .rank, .dense_rank, .percent_rank, .cume_dist => {
                if (window.value_expression != null or window.offset != 1 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .ntile => {
                if (window.value_expression != null or window.offset == 0 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .lag, .lead => {
                if (window.value_expression == null or window.offset == 0) return error.InvalidQueryRequest;
            },
            .first_value => {
                if (window.value_expression == null or window.offset != 1 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .last_value => {
                if (window.value_expression == null or window.offset != 1 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .nth_value => {
                if (window.value_expression == null or window.offset == 0 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .count => {
                if (window.offset != 1 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .sum, .avg, .min, .max => {
                if (window.value_expression == null or window.offset != 1 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
            .bool_or, .bool_and => {
                if (window.value_expression == null or window.offset != 1 or window.default_json.len > 0) return error.InvalidQueryRequest;
            },
        }
        if (window.order_by.len == 0 and windowFunctionRequiresOrder(window.function)) return error.InvalidQueryRequest;
        if (!windowFunctionSupportsFilter(window.function) and windowHasFilters(window)) return error.InvalidQueryRequest;
    }

    return try windowSourceOrderAlloc(alloc, first_window);
}

fn validateWindowFrameSpec(window: types.RelationalRowsWindowSpec) !void {
    const frame = window.frame orelse return;
    if (window.order_by.len == 0) return error.InvalidQueryRequest;
    if (frame.start == .unbounded_following or frame.end == .unbounded_preceding) return error.InvalidQueryRequest;
    try validateWindowFrameBoundOffset(frame.start, frame.start_offset);
    try validateWindowFrameBoundOffset(frame.end, frame.end_offset);
    if (windowFrameBoundOrdinal(frame.start, frame.start_offset) > windowFrameBoundOrdinal(frame.end, frame.end_offset)) return error.InvalidQueryRequest;
    if (frame.unit != .range or !windowFrameHasOffset(frame)) return;
    const order = window.order_by[0];
    if (order.null_test != null) return error.InvalidQueryRequest;
    if (order.field.len == 0 and order.expression == null) return error.InvalidQueryRequest;
}

fn validateWindowFrameBoundOffset(
    bound: types.RelationalRowsWindowFrameBound,
    offset: u32,
) !void {
    switch (bound) {
        .offset_preceding, .offset_following => {
            if (offset == 0) return error.InvalidQueryRequest;
        },
        else => if (offset != 0) return error.InvalidQueryRequest,
    }
}

fn windowFrameHasOffset(frame: types.RelationalRowsWindowFrame) bool {
    return frame.start == .offset_preceding or
        frame.start == .offset_following or
        frame.end == .offset_preceding or
        frame.end == .offset_following;
}

fn windowFrameBoundOrdinal(bound: types.RelationalRowsWindowFrameBound, offset: u32) i64 {
    return switch (bound) {
        .unbounded_preceding => std.math.minInt(i64),
        .offset_preceding => -@as(i64, @intCast(offset)),
        .current_row => 0,
        .offset_following => @as(i64, @intCast(offset)),
        .unbounded_following => std.math.maxInt(i64),
    };
}

fn windowFunctionSupportsFilter(function: types.RelationalRowsWindowFunction) bool {
    return switch (function) {
        .count, .sum, .avg, .min, .max, .bool_or, .bool_and => true,
        else => false,
    };
}

fn windowFunctionRequiresOrder(function: types.RelationalRowsWindowFunction) bool {
    return switch (function) {
        .count, .sum, .avg, .min, .max, .bool_or, .bool_and => false,
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .lag, .lead, .first_value, .last_value, .nth_value => true,
    };
}

pub fn windowHasFilters(window: types.RelationalRowsWindowSpec) bool {
    return window.filter_predicates.len > 0 or
        window.filter_array_any.len > 0 or
        window.filter_array_contains.len > 0 or
        window.filter_array_eq.len > 0 or
        window.filter_in_predicates.len > 0 or
        window.filter_json_contains.len > 0 or
        window.filter_json_path_eq.len > 0 or
        window.filter_json_path_exists.len > 0 or
        window.filter_text_patterns.len > 0 or
        window.filter_expressions.len > 0 or
        window.filter_expression_array_contains.len > 0 or
        window.filter_any.len > 0 or
        window.filter_not.len > 0;
}

fn validateWindowOutputNames(req: types.RelationalRowsWindowRequest) !void {
    if (!req.select_all) {
        for (req.select) |field| {
            if (field.len == 0) return error.InvalidQueryRequest;
            if (windowExplicitOutputNameCount(req, field) > 1) return error.InvalidQueryRequest;
        }
    }
    for (req.windows) |window| {
        if (window.output.len == 0) return error.InvalidQueryRequest;
        if (windowExplicitOutputNameCount(req, window.output) > 1) return error.InvalidQueryRequest;
    }
}

fn windowExplicitOutputNameCount(
    req: types.RelationalRowsWindowRequest,
    name: []const u8,
) usize {
    var count: usize = 0;
    if (!req.select_all) {
        for (req.select) |field| {
            if (std.mem.eql(u8, field, name)) count += 1;
        }
    }
    for (req.windows) |window| {
        if (std.mem.eql(u8, window.output, name)) count += 1;
    }
    return count;
}

fn windowSourceOrderAlloc(
    alloc: Allocator,
    window: types.RelationalRowsWindowSpec,
) ![]types.RelationalRowsQueryOrder {
    const out = try alloc.alloc(types.RelationalRowsQueryOrder, window.partition_by.len + window.order_by.len);
    for (window.partition_by, 0..) |field, i| out[i] = .{ .field = field, .direction = .asc };
    for (window.order_by, 0..) |order, i| out[window.partition_by.len + i] = order;
    return out;
}

pub fn validateFinalCteReference(
    ctes: []const types.RelationalRowsCte,
    name: []const u8,
) !void {
    if (name.len != 0 and !cteNameExists(ctes, name)) return error.InvalidQueryRequest;
}

fn cteNameExists(
    ctes: []const types.RelationalRowsCte,
    name: []const u8,
) bool {
    for (ctes) |cte| {
        if (std.mem.eql(u8, cte.name, name)) return true;
    }
    return false;
}

pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn queryRelationalRowsSetOperationPlan(
            self: *DB,
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            plan: types.RelationalRowsSetOperationPlan,
        ) !types.RelationalRowsQueryResult {
            if (runtime_schema.storage_mode != .relational or runtime_schema.primary_key == null) return error.InvalidArgument;

            var left = try self.queryRelationalRowsPlan(alloc, runtime_schema, plan.left);
            defer left.deinit(alloc);
            var right = try self.queryRelationalRowsPlan(alloc, runtime_schema, plan.right);
            defer right.deinit(alloc);

            const combined = try relationalRowsSetOperationRowsAlloc(alloc, plan.operation, left.rows, right.rows);
            defer freeOwnedConstStringSlice(alloc, combined);
            try admitRelationalRowsSetOperationRowsAllowSpill(plan, combined);

            const tail_query = types.RelationalRowsQueryRequest{
                .order_by = plan.order_by,
                .limit = plan.limit,
                .offset = plan.offset,
                .select_all = true,
            };
            return try self.queryRelationalRowsFromSourceRowsAlloc(alloc, "set_operation", combined, tail_query);
        }
    };
}

pub fn admitRelationalRowsSetOperationRows(
    plan: types.RelationalRowsSetOperationPlan,
    rows: []const []const u8,
) !void {
    try admitRelationalRowsSetOperationRowsWithPolicy(plan, rows, .strict);
}

pub fn admitRelationalRowsSetOperationRowsAllowSpill(
    plan: types.RelationalRowsSetOperationPlan,
    rows: []const []const u8,
) !void {
    try admitRelationalRowsSetOperationRowsWithPolicy(plan, rows, .allow_spill);
}

fn admitRelationalRowsSetOperationRowsWithPolicy(
    plan: types.RelationalRowsSetOperationPlan,
    rows: []const []const u8,
    policy: RelationalRowsCteMaterializationPolicy,
) !void {
    const materialized_bytes = types.relationalRowsCteMaterializedJsonBytes(rows) orelse return error.UnsupportedQueryRequest;
    const admission = types.RelationalRowsCte{
        .name = "set_operation",
        .query = .{},
        .max_rows = plan.max_rows,
        .max_bytes = plan.max_bytes,
        .spill_after_bytes = plan.spill_after_bytes,
    };
    try admitRelationalRowsCteMaterializationWithPolicy(admission, rows.len, materialized_bytes, policy);
}

pub fn admitRelationalRowsCteMaterialization(
    cte: types.RelationalRowsCte,
    observed_rows: usize,
    observed_bytes: u64,
) !void {
    try admitRelationalRowsCteMaterializationWithPolicy(cte, observed_rows, observed_bytes, .strict);
}

pub fn admitRelationalRowsCteMaterializationAllowSpill(
    cte: types.RelationalRowsCte,
    observed_rows: usize,
    observed_bytes: u64,
) !void {
    try admitRelationalRowsCteMaterializationWithPolicy(cte, observed_rows, observed_bytes, .allow_spill);
}

const RelationalRowsCteMaterializationPolicy = enum {
    strict,
    allow_spill,
};

fn admitRelationalRowsCteMaterializationWithPolicy(
    cte: types.RelationalRowsCte,
    observed_rows: usize,
    observed_bytes: u64,
    policy: RelationalRowsCteMaterializationPolicy,
) !void {
    switch (types.relationalRowsCteMaterializationDecision(cte, observed_rows, observed_bytes)) {
        .memory => {},
        .spill => switch (policy) {
            .strict => return error.RelationalRowsCteSpillRequired,
            .allow_spill => {},
        },
        .reject => return error.RelationalRowsCteMaterializationRejected,
    }
}

pub fn relationalRowsSetOperationRowsAlloc(
    alloc: Allocator,
    operation: types.RelationalRowsSetOperation,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    return switch (operation) {
        .union_all => try relationalRowsUnionAllRowsAlloc(alloc, left, right),
        .union_distinct => try relationalRowsUnionDistinctRowsAlloc(alloc, left, right),
        .intersect => try relationalRowsIntersectRowsAlloc(alloc, left, right),
        .except => try relationalRowsExceptRowsAlloc(alloc, left, right),
    };
}

fn relationalRowsUnionAllRowsAlloc(
    alloc: Allocator,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitRelationalRowsSetRowList(alloc, &rows);
    try rows.ensureUnusedCapacity(alloc, left.len + right.len);
    for (left) |row| rows.appendAssumeCapacity(try alloc.dupe(u8, row));
    for (right) |row| rows.appendAssumeCapacity(try alloc.dupe(u8, row));
    return try rows.toOwnedSlice(alloc);
}

fn relationalRowsUnionDistinctRowsAlloc(
    alloc: Allocator,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer freeRelationalRowsSetKeyMap(alloc, &seen);
    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitRelationalRowsSetRowList(alloc, &rows);
    for (left) |row| try appendDistinctRelationalRowsSetRowAlloc(alloc, &seen, &rows, row);
    for (right) |row| try appendDistinctRelationalRowsSetRowAlloc(alloc, &seen, &rows, row);
    return try rows.toOwnedSlice(alloc);
}

fn relationalRowsIntersectRowsAlloc(
    alloc: Allocator,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    var right_set = std.StringHashMapUnmanaged(void).empty;
    defer freeRelationalRowsSetKeyMap(alloc, &right_set);
    for (right) |row| _ = try putRelationalRowsSetKeyAlloc(alloc, &right_set, row);

    var emitted = std.StringHashMapUnmanaged(void).empty;
    defer freeRelationalRowsSetKeyMap(alloc, &emitted);
    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitRelationalRowsSetRowList(alloc, &rows);
    for (left) |row| {
        if (!right_set.contains(row)) continue;
        try appendDistinctRelationalRowsSetRowAlloc(alloc, &emitted, &rows, row);
    }
    return try rows.toOwnedSlice(alloc);
}

fn relationalRowsExceptRowsAlloc(
    alloc: Allocator,
    left: []const []const u8,
    right: []const []const u8,
) ![]const []const u8 {
    var right_set = std.StringHashMapUnmanaged(void).empty;
    defer freeRelationalRowsSetKeyMap(alloc, &right_set);
    for (right) |row| _ = try putRelationalRowsSetKeyAlloc(alloc, &right_set, row);

    var emitted = std.StringHashMapUnmanaged(void).empty;
    defer freeRelationalRowsSetKeyMap(alloc, &emitted);
    var rows = std.ArrayListUnmanaged([]const u8).empty;
    errdefer deinitRelationalRowsSetRowList(alloc, &rows);
    for (left) |row| {
        if (right_set.contains(row)) continue;
        try appendDistinctRelationalRowsSetRowAlloc(alloc, &emitted, &rows, row);
    }
    return try rows.toOwnedSlice(alloc);
}

fn appendDistinctRelationalRowsSetRowAlloc(
    alloc: Allocator,
    seen: *std.StringHashMapUnmanaged(void),
    rows: *std.ArrayListUnmanaged([]const u8),
    row: []const u8,
) !void {
    if (try putRelationalRowsSetKeyAlloc(alloc, seen, row)) {
        const row_copy = try alloc.dupe(u8, row);
        errdefer alloc.free(row_copy);
        try rows.append(alloc, row_copy);
    }
}

fn putRelationalRowsSetKeyAlloc(
    alloc: Allocator,
    set: *std.StringHashMapUnmanaged(void),
    row: []const u8,
) !bool {
    if (set.contains(row)) return false;
    const key = try alloc.dupe(u8, row);
    errdefer alloc.free(key);
    const gop = try set.getOrPut(alloc, key);
    if (gop.found_existing) {
        alloc.free(key);
        return false;
    }
    return true;
}

fn deinitRelationalRowsSetRowList(
    alloc: Allocator,
    rows: *std.ArrayListUnmanaged([]const u8),
) void {
    for (rows.items) |row| alloc.free(@constCast(row));
    rows.deinit(alloc);
}

fn freeRelationalRowsSetKeyMap(
    alloc: Allocator,
    set: *std.StringHashMapUnmanaged(void),
) void {
    var keys = set.keyIterator();
    while (keys.next()) |key| alloc.free(@constCast(key.*));
    set.deinit(alloc);
}

fn freeOwnedConstStringSlice(alloc: Allocator, keys: []const []const u8) void {
    for (keys) |key| alloc.free(@constCast(key));
    if (keys.len > 0) alloc.free(@constCast(keys));
}
