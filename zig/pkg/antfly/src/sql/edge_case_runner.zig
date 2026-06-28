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
const corpus = @import("corpus.zig");
const plan = @import("plan.zig");
const relational_rows = @import("relational_rows.zig");
const runtime_schema = @import("../storage/schema.zig");
const tokenized = @import("tokenized.zig");
const value_mod = @import("value.zig");

pub const SqlAdapterEdgeCaseLoweringCallbacks = struct {
    lower_select: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
    ) anyerror!plan.LoweredSelect,
    lower_update: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_delete: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredMutation,
    lower_insert: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredInsert,
    plan_ddl: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
    ) anyerror!binder.LogicalSqlPlan,
    lower_write_plan: *const fn (
        std.mem.Allocator,
        *const tokenized.ParsedSql,
        runtime_schema.TableSchema,
        []const value_mod.SqlValue,
        ?relational_rows.UniqueSelectorResolver,
    ) anyerror!plan.LoweredWritePlan,
};

fn expectSqlAdapterEdgeCaseError(expected: []const u8, actual: anyerror) !void {
    if (std.mem.eql(u8, expected, "unsupported_sql_shape")) {
        return std.testing.expectEqual(error.UnsupportedSqlShape, actual);
    }
    if (std.mem.eql(u8, expected, "invalid_sql_catalog")) {
        return std.testing.expectEqual(error.InvalidSqlCatalog, actual);
    }
    if (std.mem.eql(u8, expected, "unsupported_rows_selector")) {
        return std.testing.expectEqual(error.UnsupportedRowsSelector, actual);
    }
    if (std.mem.eql(u8, expected, "unsupported_rows_query")) {
        return std.testing.expectEqual(error.UnsupportedRowsQuery, actual);
    }
    return error.TestUnexpectedResult;
}

fn expectSqlAdapterEdgeCaseNoErrorExpected(expected_error: []const u8) !void {
    if (expected_error.len != 0) return error.TestUnexpectedResult;
}

fn expectSqlAdapterEdgeCaseSelect(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    var lowered = callbacks.lower_select(alloc, parsed_sql, schema, edge_case.params) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer lowered.deinit(alloc);
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
    if (edge_case.expected_table) |table_name| {
        try std.testing.expectEqualStrings(table_name, lowered.table_name);
    }
    if (edge_case.expected_predicates) |predicates| {
        try std.testing.expectEqual(predicates, lowered.query.predicates.len);
    }
    if (edge_case.expected_first_predicate_field) |field| {
        if (lowered.query.predicates.len == 0) return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(field, lowered.query.predicates[0].field);
    }
    if (edge_case.expected_first_predicate_value_json) |value_json| {
        if (lowered.query.predicates.len == 0) return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings(value_json, lowered.query.predicates[0].value_json orelse return error.TestUnexpectedResult);
    }
}

fn expectSqlAdapterEdgeCaseUpdate(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    resolver: relational_rows.UniqueSelectorResolver,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    var lowered = callbacks.lower_update(alloc, parsed_sql, schema, edge_case.params, resolver) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer lowered.deinit(alloc);
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
    if (edge_case.expected_table) |table_name| {
        try std.testing.expectEqualStrings(table_name, lowered.table_name);
    }
    if (edge_case.expected_transformed) |transformed| {
        try std.testing.expectEqual(transformed, lowered.batch.transformed);
    }
}

fn expectSqlAdapterEdgeCaseDelete(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    resolver: relational_rows.UniqueSelectorResolver,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    var lowered = callbacks.lower_delete(alloc, parsed_sql, schema, edge_case.params, resolver) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer lowered.deinit(alloc);
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
}

fn expectSqlAdapterEdgeCaseInsert(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    resolver: relational_rows.UniqueSelectorResolver,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    var lowered = callbacks.lower_insert(alloc, parsed_sql, schema, edge_case.params, resolver) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer lowered.deinit(alloc);
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
    if (edge_case.expected_table) |table_name| {
        try std.testing.expectEqualStrings(table_name, lowered.table_name);
    }
    if (edge_case.expected_inserted) |inserted| {
        try std.testing.expectEqual(inserted, lowered.batch.inserted);
    }
}

fn expectSqlAdapterEdgeCaseDdl(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    var logical = callbacks.plan_ddl(alloc, parsed_sql) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer logical.deinit(alloc);
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
    if (edge_case.expected_ddl_tag) |tag| switch (tag) {
        .create_table => switch (logical) {
            .table_ddl => |table_plan| switch (table_plan) {
                .create_table => |create_table_plan| {
                    if (edge_case.expected_table) |table_name| try std.testing.expectEqualStrings(table_name, create_table_plan.table_name);
                    if (edge_case.expected_if_not_exists) |if_not_exists| try std.testing.expectEqual(if_not_exists, create_table_plan.if_not_exists);
                },
                else => return error.TestUnexpectedResult,
            },
            else => return error.TestUnexpectedResult,
        },
    };
}

fn expectSqlAdapterEdgeCaseClassifyWrite(
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
) !void {
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
    const expected_kind = edge_case.expected_write_kind orelse return error.TestUnexpectedResult;
    const actual = parsed_sql.writeStatementKindIncludingGeneratedAst() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(expected_kind, actual);
}

fn expectSqlAdapterEdgeCaseWritePlan(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    parsed_sql: *const tokenized.ParsedSql,
    schema: runtime_schema.TableSchema,
    resolver: relational_rows.UniqueSelectorResolver,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    const effective_resolver: ?relational_rows.UniqueSelectorResolver = if (edge_case.omit_resolver) null else resolver;
    var lowered = callbacks.lower_write_plan(alloc, parsed_sql, schema, edge_case.params, effective_resolver) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer lowered.deinit(alloc);
    try expectSqlAdapterEdgeCaseNoErrorExpected(edge_case.expected_error);
}

pub fn expectSqlAdapterEdgeCase(
    alloc: std.mem.Allocator,
    edge_case: corpus.SqlAdapterEdgeCase,
    schema: runtime_schema.TableSchema,
    resolver: relational_rows.UniqueSelectorResolver,
    callbacks: SqlAdapterEdgeCaseLoweringCallbacks,
) !void {
    errdefer std.debug.print("sql adapter edge case failed: {s}\n", .{edge_case.name});
    var parsed_sql = tokenized.ParsedSql.initAlloc(alloc, edge_case.sql) catch |err| {
        try expectSqlAdapterEdgeCaseError(edge_case.expected_error, err);
        return;
    };
    defer parsed_sql.deinit(alloc);
    switch (edge_case.action) {
        .select => try expectSqlAdapterEdgeCaseSelect(alloc, edge_case, &parsed_sql, schema, callbacks),
        .update => try expectSqlAdapterEdgeCaseUpdate(alloc, edge_case, &parsed_sql, schema, resolver, callbacks),
        .delete => try expectSqlAdapterEdgeCaseDelete(alloc, edge_case, &parsed_sql, schema, resolver, callbacks),
        .insert => try expectSqlAdapterEdgeCaseInsert(alloc, edge_case, &parsed_sql, schema, resolver, callbacks),
        .ddl => try expectSqlAdapterEdgeCaseDdl(alloc, edge_case, &parsed_sql, callbacks),
        .classify_write => try expectSqlAdapterEdgeCaseClassifyWrite(edge_case, &parsed_sql),
        .write_plan => try expectSqlAdapterEdgeCaseWritePlan(alloc, edge_case, &parsed_sql, schema, resolver, callbacks),
    }
}
