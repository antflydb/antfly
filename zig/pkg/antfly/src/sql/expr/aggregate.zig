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

const binder = @import("../binder.zig");
const db_mod = @import("../../storage/db/mod.zig");
const ddl_plan = @import("../ddl_plan.zig");
const expr_equal = @import("equal.zig");
const expr_projection = @import("projection.zig");
const expr_type = @import("type.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");

pub const ProjectedColumnType = struct {
    field_type: runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType = null,
};

pub const Filter = struct {
    predicates: []const runtime_schema.RelationalCheck = &.{},
    array_any: []const db_mod.types.RelationalRowsArrayAnyPredicate = &.{},
    array_contains: []const db_mod.types.RelationalRowsArrayContainsPredicate = &.{},
    array_eq: []const db_mod.types.RelationalRowsArrayEqPredicate = &.{},
    in_predicates: []const db_mod.types.RelationalRowsInPredicate = &.{},
    json_contains: []const db_mod.types.RelationalRowsJsonContainsPredicate = &.{},
    json_path_eq: []const db_mod.types.RelationalRowsJsonPathEqPredicate = &.{},
    json_path_exists: []const db_mod.types.RelationalRowsJsonPathExistsPredicate = &.{},
    text_patterns: []const db_mod.types.RelationalRowsTextPatternPredicate = &.{},
    expressions: []const db_mod.types.RelationalRowsExpressionCondition = &.{},
    expression_array_contains: []const db_mod.types.RelationalRowsExpressionArrayContainsPredicate = &.{},
    any_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
    not_groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup = &.{},
};

pub const PercentileArgument = struct {
    percentile: ?f64 = null,
    percentiles: []const f64 = &.{},
};

pub fn opForName(name: []const u8) ?db_mod.types.RelationalRowsAggregateOp {
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "percentile_cont")) return .percentile_cont;
    if (std.ascii.eqlIgnoreCase(name, "percentile_disc")) return .percentile_disc;
    if (std.ascii.eqlIgnoreCase(name, "mode")) return .mode;
    if (std.ascii.eqlIgnoreCase(name, "array_agg")) return .array_agg;
    if (std.ascii.eqlIgnoreCase(name, "string_agg")) return .string_agg;
    if (std.ascii.eqlIgnoreCase(name, "bool_or")) return .bool_or;
    if (std.ascii.eqlIgnoreCase(name, "bool_and")) return .bool_and;
    return null;
}

pub fn opName(op: db_mod.types.RelationalRowsAggregateOp) []const u8 {
    return switch (op) {
        .count => "count",
        .sum => "sum",
        .min => "min",
        .max => "max",
        .avg => "avg",
        .percentile_cont => "percentile_cont",
        .percentile_disc => "percentile_disc",
        .mode => "mode",
        .array_agg => "array_agg",
        .string_agg => "string_agg",
        .bool_or => "bool_or",
        .bool_and => "bool_and",
    };
}

pub fn validatePercentile(percentile: f64) !void {
    if (!std.math.isFinite(percentile) or percentile < 0 or percentile > 1) return error.UnsupportedSqlShape;
}

pub fn aliasOrDefaultAlloc(
    alloc: std.mem.Allocator,
    explicit_alias: ?[]const u8,
    op: db_mod.types.RelationalRowsAggregateOp,
    field: ?[]const u8,
) ![]const u8 {
    if (explicit_alias) |alias| return try alloc.dupe(u8, alias);
    if (field) |field_name| return try std.fmt.allocPrint(alloc, "{s}_{s}", .{ opName(op), field_name });
    return try alloc.dupe(u8, opName(op));
}

pub fn isPercentileOp(op: db_mod.types.RelationalRowsAggregateOp) bool {
    return op == .percentile_cont or op == .percentile_disc;
}

pub fn sqlJsonNumberAsF64(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string => |number| std.fmt.parseFloat(f64, number) catch null,
        else => null,
    };
}

pub fn outputProjectedType(
    aggregation: db_mod.types.RelationalRowsAggregateSpec,
    input_type: ?runtime_schema.AntflyType,
) !ProjectedColumnType {
    return switch (aggregation.op) {
        .array_agg => .{
            .field_type = .array,
            .array_item_type = input_type orelse return error.UnsupportedSqlShape,
        },
        .string_agg => .{ .field_type = .keyword },
        .percentile_cont, .percentile_disc => .{
            .field_type = if (aggregation.percentiles.len > 0) .array else .numeric,
            .array_item_type = if (aggregation.percentiles.len > 0) .numeric else null,
        },
        .count, .sum, .avg => .{ .field_type = .numeric },
        .min, .max, .mode => .{ .field_type = input_type orelse return error.UnsupportedSqlShape },
        .bool_or, .bool_and => .{ .field_type = .boolean },
    };
}

pub fn filterIsEmpty(filter: Filter) bool {
    return filter.predicates.len == 0 and
        filter.array_any.len == 0 and
        filter.array_contains.len == 0 and
        filter.array_eq.len == 0 and
        filter.in_predicates.len == 0 and
        filter.json_contains.len == 0 and
        filter.json_path_eq.len == 0 and
        filter.json_path_exists.len == 0 and
        filter.text_patterns.len == 0 and
        filter.expressions.len == 0 and
        filter.expression_array_contains.len == 0 and
        filter.any_groups.len == 0 and
        filter.not_groups.len == 0;
}

pub fn freeFilter(alloc: std.mem.Allocator, filter: Filter) void {
    plan_mod.freeRelationalChecks(alloc, filter.predicates);
    if (filter.predicates.len > 0) alloc.free(filter.predicates);
    plan_mod.freeArrayAny(alloc, filter.array_any);
    if (filter.array_any.len > 0) alloc.free(filter.array_any);
    plan_mod.freeArrayContains(alloc, filter.array_contains);
    if (filter.array_contains.len > 0) alloc.free(filter.array_contains);
    plan_mod.freeArrayEq(alloc, filter.array_eq);
    if (filter.array_eq.len > 0) alloc.free(filter.array_eq);
    plan_mod.freeInPredicates(alloc, filter.in_predicates);
    if (filter.in_predicates.len > 0) alloc.free(filter.in_predicates);
    plan_mod.freeJsonContains(alloc, filter.json_contains);
    if (filter.json_contains.len > 0) alloc.free(filter.json_contains);
    plan_mod.freeJsonPathEq(alloc, filter.json_path_eq);
    if (filter.json_path_eq.len > 0) alloc.free(filter.json_path_eq);
    plan_mod.freeJsonPathExists(alloc, filter.json_path_exists);
    if (filter.json_path_exists.len > 0) alloc.free(filter.json_path_exists);
    plan_mod.freeTextPatterns(alloc, filter.text_patterns);
    if (filter.text_patterns.len > 0) alloc.free(filter.text_patterns);
    plan_mod.freeExpressionConditions(alloc, filter.expressions);
    if (filter.expressions.len > 0) alloc.free(filter.expressions);
    plan_mod.freeExpressionArrayContains(alloc, filter.expression_array_contains);
    if (filter.expression_array_contains.len > 0) alloc.free(filter.expression_array_contains);
    plan_mod.freeExpressionPredicateGroups(alloc, filter.any_groups);
    if (filter.any_groups.len > 0) alloc.free(filter.any_groups);
    plan_mod.freeExpressionPredicateGroups(alloc, filter.not_groups);
    if (filter.not_groups.len > 0) alloc.free(filter.not_groups);
}

pub fn filterExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expressions.len;
    }
    return count;
}

pub fn filterExpressionArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_expression_array_contains.len;
    }
    return count;
}

pub fn filterJsonAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_json_contains.len +
            aggregation.filter_json_path_eq.len +
            aggregation.filter_json_path_exists.len;
    }
    return count;
}

pub fn filterStructuredAccessCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_array_any.len +
            aggregation.filter_array_contains.len +
            aggregation.filter_array_eq.len +
            aggregation.filter_in_predicates.len +
            aggregation.filter_text_patterns.len;
    }
    return count;
}

pub fn filterGroupCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        count += aggregation.filter_any.len + aggregation.filter_not.len;
    }
    return count;
}

pub fn inputExpressionCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.expression != null) count += 1;
    }
    return count;
}

pub fn descendingPercentileCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isPercentileOp(aggregation.op) and aggregation.percentile_order == .desc) count += 1;
    }
    return count;
}

pub fn percentileArrayCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (isPercentileOp(aggregation.op) and aggregation.percentiles.len > 0) count += 1;
    }
    return count;
}

pub fn modeCount(aggregations: []const db_mod.types.RelationalRowsAggregateSpec) usize {
    var count: usize = 0;
    for (aggregations) |aggregation| {
        if (aggregation.op == .mode) count += 1;
    }
    return count;
}

pub fn validateGroupBy(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    group_by: []const []const u8,
    parsed_group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
) !void {
    if (!stringSlicesEqual(group_fields, group_by)) return error.UnsupportedSqlShape;
    if (group_expressions.len != parsed_group_expressions.len) return error.UnsupportedSqlShape;
    for (group_expressions, parsed_group_expressions) |selected, parsed| {
        if (!expr_equal.relationalRowsExpressionEqual(selected.expression, parsed.expression)) return error.UnsupportedSqlShape;
    }
}

fn stringSlicesEqual(left: []const []const u8, right: []const []const u8) bool {
    if (left.len != right.len) return false;
    for (left, right) |left_item, right_item| {
        if (!std.mem.eql(u8, left_item, right_item)) return false;
    }
    return true;
}

pub fn outputFieldIsUnique(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (group_fields) |group_field| {
        if (std.mem.eql(u8, group_field, field)) matches += 1;
    }
    for (group_expressions) |projection| {
        if (std.mem.eql(u8, projection.output, field)) matches += 1;
    }
    for (aggregations) |aggregation| {
        if (std.mem.eql(u8, aggregation.name, field)) matches += 1;
    }
    return matches == 1;
}

pub fn validateSelectListOutputs(
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) !void {
    for (group_fields, 0..) |field, i| {
        for (group_fields[i + 1 ..]) |other| {
            if (std.mem.eql(u8, field, other)) return error.UnsupportedSqlShape;
        }
        for (group_expressions) |projection| {
            if (std.mem.eql(u8, field, projection.output)) return error.UnsupportedSqlShape;
        }
        for (aggregations) |aggregation| {
            if (std.mem.eql(u8, field, aggregation.name)) return error.UnsupportedSqlShape;
        }
    }
    for (group_expressions, 0..) |projection, i| {
        for (group_expressions[i + 1 ..]) |other| {
            if (std.mem.eql(u8, projection.output, other.output)) return error.UnsupportedSqlShape;
        }
        for (aggregations) |aggregation| {
            if (std.mem.eql(u8, projection.output, aggregation.name)) return error.UnsupportedSqlShape;
        }
    }
    for (aggregations, 0..) |aggregation, i| {
        for (aggregations[i + 1 ..]) |other| {
            if (std.mem.eql(u8, aggregation.name, other.name)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn outputFieldByOrdinalAlloc(
    alloc: std.mem.Allocator,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    ordinal: u32,
) ![]const u8 {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    var index: usize = @intCast(ordinal - 1);
    if (index < group_fields.len) return try alloc.dupe(u8, group_fields[index]);
    index -= group_fields.len;
    if (index < group_expressions.len) return try alloc.dupe(u8, group_expressions[index].output);
    index -= group_expressions.len;
    if (index < aggregations.len) return try alloc.dupe(u8, aggregations[index].name);
    return error.UnsupportedSqlShape;
}

pub fn inputType(
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    aggregation: db_mod.types.RelationalRowsAggregateSpec,
) !runtime_schema.AntflyType {
    if (aggregation.field) |field| {
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        return column.field_type;
    }
    if (aggregation.expression) |expression| {
        return try type_context.rowExpressionOutputType(expression);
    }
    return error.UnsupportedSqlShape;
}

pub fn outputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
) ![]runtime_schema.RelationalColumn {
    const total = group_fields.len + group_expressions.len + aggregations.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer {
        ddl_plan.clearDdlRelationalColumns(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (group_fields) |field| {
        if (expr_projection.outputColumnExists(out[0..initialized], field)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = try expr_projection.projectedSourceColumnAlloc(alloc, field, column);
        initialized += 1;
    }
    for (group_expressions) |projection| {
        if (expr_projection.outputColumnExists(out[0..initialized], projection.output)) return error.UnsupportedSqlShape;
        out[initialized] = try expr_projection.projectedColumnAlloc(alloc, projection.output, try type_context.rowExpressionOutputType(projection.expression), null, true);
        initialized += 1;
    }
    for (aggregations) |aggregation| {
        if (expr_projection.outputColumnExists(out[0..initialized], aggregation.name)) return error.UnsupportedSqlShape;
        const aggregation_input_type = if (aggregation.field != null or aggregation.expression != null)
            try inputType(schema, type_context, aggregation)
        else
            null;
        const projected_type = try outputProjectedType(aggregation, aggregation_input_type);
        out[initialized] = try expr_projection.projectedColumnAlloc(alloc, aggregation.name, projected_type.field_type, projected_type.array_item_type, false);
        initialized += 1;
    }
    return out;
}

pub fn outputColumnForFieldAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    group_fields: []const []const u8,
    group_expressions: []const db_mod.types.RelationalRowsExpressionProjection,
    aggregations: []const db_mod.types.RelationalRowsAggregateSpec,
    field: []const u8,
) !runtime_schema.RelationalColumn {
    const output_columns = try outputColumnsAlloc(alloc, schema, type_context, group_fields, group_expressions, aggregations);
    defer ddl_plan.freeDdlRelationalColumns(alloc, output_columns);
    for (output_columns) |column| {
        if (std.mem.eql(u8, column.name, field)) return try expr_projection.projectedSourceColumnAlloc(alloc, column.name, column);
    }
    return error.UnsupportedSqlShape;
}

test "sql expr_aggregate validates output type and filter ownership" {
    const alloc = std.testing.allocator;

    try std.testing.expectEqual(db_mod.types.RelationalRowsAggregateOp.percentile_cont, opForName("PERCENTILE_CONT").?);
    try std.testing.expectEqualStrings("array_agg", opName(.array_agg));
    try validatePercentile(0);
    try validatePercentile(1);
    try std.testing.expectError(error.UnsupportedSqlShape, validatePercentile(-0.01));
    try std.testing.expectError(error.UnsupportedSqlShape, validatePercentile(1.01));
    try std.testing.expectError(error.UnsupportedSqlShape, validatePercentile(std.math.inf(f64)));
    const explicit_alias = try aliasOrDefaultAlloc(alloc, "total_amount", .sum, "amount");
    defer alloc.free(explicit_alias);
    try std.testing.expectEqualStrings("total_amount", explicit_alias);
    const field_alias = try aliasOrDefaultAlloc(alloc, null, .sum, "amount");
    defer alloc.free(field_alias);
    try std.testing.expectEqualStrings("sum_amount", field_alias);
    const op_alias = try aliasOrDefaultAlloc(alloc, null, .count, null);
    defer alloc.free(op_alias);
    try std.testing.expectEqualStrings("count", op_alias);
    try std.testing.expect(isPercentileOp(.percentile_disc));
    try std.testing.expect(!isPercentileOp(.array_agg));
    try std.testing.expectEqual(@as(?f64, 1.5), sqlJsonNumberAsF64(.{ .number_string = "1.5" }));
    try std.testing.expect(sqlJsonNumberAsF64(.{ .string = "1.5" }) == null);

    try std.testing.expectEqual(runtime_schema.AntflyType.array, (try outputProjectedType(.{
        .name = "items",
        .op = .array_agg,
    }, .keyword)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try outputProjectedType(.{
        .name = "items",
        .op = .array_agg,
    }, .keyword)).array_item_type.?);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, (try outputProjectedType(.{
        .name = "labels",
        .op = .string_agg,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try outputProjectedType(.{
        .name = "count",
        .op = .count,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try outputProjectedType(.{
        .name = "p95",
        .op = .percentile_cont,
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.array, (try outputProjectedType(.{
        .name = "percentiles",
        .op = .percentile_disc,
        .percentiles = &.{ 0.5, 0.95 },
    }, null)).field_type);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, (try outputProjectedType(.{
        .name = "percentiles",
        .op = .percentile_disc,
        .percentiles = &.{ 0.5, 0.95 },
    }, null)).array_item_type.?);
    try std.testing.expectEqual(runtime_schema.AntflyType.datetime, (try outputProjectedType(.{
        .name = "latest",
        .op = .max,
    }, .datetime)).field_type);
    try std.testing.expectError(error.UnsupportedSqlShape, outputProjectedType(.{
        .name = "missing",
        .op = .array_agg,
    }, null));
    try std.testing.expect(filterIsEmpty(.{}));

    const owned_filter_predicates = try alloc.alloc(runtime_schema.RelationalCheck, 1);
    const owned_filter_field = try alloc.dupe(u8, "status");
    const owned_filter_value = try alloc.dupe(u8, "\"open\"");
    owned_filter_predicates[0] = .{
        .name = "",
        .field = owned_filter_field,
        .op = .eq,
        .value_json = owned_filter_value,
    };
    const owned_filter: Filter = .{ .predicates = owned_filter_predicates };
    defer freeFilter(alloc, owned_filter);
    try std.testing.expect(!filterIsEmpty(owned_filter));

    const aggregate_specs = [_]db_mod.types.RelationalRowsAggregateSpec{.{
        .name = "statuses",
        .op = .array_agg,
        .field = "status",
        .filter_json_contains = &.{.{ .field = "metadata", .value_json = "{\"source\":\"sql\"}" }},
    }};
    try std.testing.expectEqual(@as(usize, 0), filterGroupCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), filterExpressionCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), filterExpressionArrayCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 1), filterJsonAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), filterStructuredAccessCount(&aggregate_specs));
    try std.testing.expectEqual(@as(usize, 0), inputExpressionCount(&aggregate_specs));

    const percentile_specs = [_]db_mod.types.RelationalRowsAggregateSpec{
        .{ .name = "p", .op = .percentile_cont, .field = "amount", .percentile_order = .desc, .percentiles = &.{ 0.5, 0.9 } },
        .{ .name = "m", .op = .mode, .field = "status" },
    };
    try std.testing.expectEqual(@as(usize, 1), descendingPercentileCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), percentileArrayCount(&percentile_specs));
    try std.testing.expectEqual(@as(usize, 1), modeCount(&percentile_specs));
}
