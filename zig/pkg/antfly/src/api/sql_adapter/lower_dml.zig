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
const db_mod = @import("../../storage/db/mod.zig");
const lower_expr = @import("lower_expr.zig");
const plan_mod = @import("plan.zig");
const relational_rows = @import("../relational_rows.zig");
const runtime_schema = @import("../../storage/schema.zig");

const LoweredMergeMutationPlan = plan_mod.LoweredMergeMutationPlan;
const MergeArmPredicate = plan_mod.MergeArmPredicate;
const MergeExpressionAssignment = plan_mod.MergeExpressionAssignment;
const MergeFieldMapping = plan_mod.MergeFieldMapping;
const MergeMatchedArm = plan_mod.MergeMatchedArm;
const MergeNotMatchedArm = plan_mod.MergeNotMatchedArm;
const ReturningProjection = plan_mod.ReturningProjection;

pub fn sqlCanonicalMutationFieldPath(
    schema: runtime_schema.TableSchema,
    field: []const u8,
) ![]const u8 {
    const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
    return column.path;
}

pub fn validateSqlInsertColumnsUnique(
    schema: runtime_schema.TableSchema,
    columns: []const []const u8,
) !void {
    for (columns, 0..) |lhs, i| {
        const lhs_path = try sqlCanonicalMutationFieldPath(schema, lhs);
        for (columns[i + 1 ..]) |rhs| {
            if (sqlDottedPathsConflict(lhs_path, try sqlCanonicalMutationFieldPath(schema, rhs))) return error.UnsupportedSqlShape;
        }
    }
}

pub fn mergeExpressionUsesTargetRow(expression: runtime_schema.RelationalRowsExpression) bool {
    if (expression.kind == .field and expression.field_source != .source) return true;
    for (expression.operands) |operand| {
        if (mergeExpressionUsesTargetRow(operand)) return true;
    }
    for (expression.case_branches) |branch| {
        if (mergeExpressionConditionUsesTargetRow(branch.when)) return true;
        if (mergeExpressionUsesTargetRow(branch.then)) return true;
    }
    for (expression.case_else) |case_else| {
        if (mergeExpressionUsesTargetRow(case_else)) return true;
    }
    return false;
}

pub fn mergeExpressionConditionUsesTargetRow(condition: runtime_schema.RelationalRowsExpressionCondition) bool {
    if (mergeExpressionUsesTargetRow(condition.lhs)) return true;
    for (condition.rhs) |rhs| {
        if (mergeExpressionUsesTargetRow(rhs)) return true;
    }
    return false;
}

pub fn mergeExpressionPredicateGroupsUseTargetRow(groups: []const runtime_schema.RelationalRowsExpressionPredicateGroup) bool {
    for (groups) |group| {
        for (group.conditions) |condition| {
            if (mergeExpressionConditionUsesTargetRow(condition)) return true;
        }
    }
    return false;
}

pub fn updateWillLookupExistingRow(schema: runtime_schema.TableSchema, returning: ReturningProjection) bool {
    if (returning.hasProjection() or schema.checks.len > 0) return true;
    for (schema.relational_columns) |column| {
        if (column.generated != null) return true;
    }
    return false;
}

pub fn validateMergeAssignmentsUnique(
    mappings: []const MergeFieldMapping,
    expressions: []const MergeExpressionAssignment,
) !void {
    for (mappings, 0..) |mapping, i| {
        for (mappings[i + 1 ..]) |other| {
            if (std.mem.eql(u8, mapping.target_field, other.target_field)) return error.UnsupportedSqlShape;
        }
        for (expressions) |expression| {
            if (std.mem.eql(u8, mapping.target_field, expression.target_field)) return error.UnsupportedSqlShape;
        }
    }
    for (expressions, 0..) |expression, i| {
        for (expressions[i + 1 ..]) |other| {
            if (std.mem.eql(u8, expression.target_field, other.target_field)) return error.UnsupportedSqlShape;
        }
    }
}

pub fn mergeSourceQueryIsDefault(req: db_mod.types.RelationalRowsQueryRequest) bool {
    return req.source_cte.len == 0 and
        req.predicates.len == 0 and
        req.array_any.len == 0 and
        req.array_contains.len == 0 and
        req.array_eq.len == 0 and
        req.in_predicates.len == 0 and
        req.json_contains.len == 0 and
        req.json_path_eq.len == 0 and
        req.json_path_exists.len == 0 and
        req.text_patterns.len == 0 and
        req.or_predicates.len == 0 and
        req.not_predicates.len == 0 and
        req.access_or_predicates.len == 0 and
        req.access_not_predicates.len == 0 and
        req.expression_predicates.len == 0 and
        req.expression_or_predicates.len == 0 and
        req.expression_not_predicates.len == 0 and
        req.expression_array_contains.len == 0 and
        req.select.len == 0 and
        req.json_extract.len == 0 and
        req.array_length.len == 0 and
        req.coalesce.len == 0 and
        req.field_aliases.len == 0 and
        req.expressions.len == 0 and
        req.select_all and
        req.distinct_on.len == 0 and
        req.distinct_on_expressions.len == 0 and
        req.order_by.len == 0 and
        req.row_claim == null and
        req.doc_key_range == null and
        req.limit == null and
        req.offset == 0;
}

pub const MergeExecutionTargetRow = struct {
    key: []const u8,
    json: []const u8,
    version: u64,
};

pub fn buildMergeMutationBatchAlloc(
    alloc: std.mem.Allocator,
    target_schema: runtime_schema.TableSchema,
    source_schema: runtime_schema.TableSchema,
    plan: LoweredMergeMutationPlan,
    target_rows: []const MergeExecutionTargetRow,
    source_rows: []const []const u8,
) !relational_rows.OwnedRowsBatchRequest {
    if (target_schema.storage_mode != .relational or target_schema.primary_key == null) return error.InvalidSqlCatalog;
    if (source_schema.storage_mode != .relational) return error.InvalidSqlCatalog;

    var target_parsed = std.ArrayListUnmanaged(std.json.Parsed(std.json.Value)).empty;
    defer {
        for (target_parsed.items) |*parsed| parsed.deinit();
        target_parsed.deinit(alloc);
    }
    try target_parsed.ensureUnusedCapacity(alloc, target_rows.len);
    for (target_rows) |row| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row.json, .{}) catch return error.InvalidRowsRequest;
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        target_parsed.appendAssumeCapacity(parsed);
    }

    var source_parsed = std.ArrayListUnmanaged(std.json.Parsed(std.json.Value)).empty;
    defer {
        for (source_parsed.items) |*parsed| parsed.deinit();
        source_parsed.deinit(alloc);
    }
    try source_parsed.ensureUnusedCapacity(alloc, source_rows.len);
    for (source_rows) |row| {
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, row, .{}) catch return error.InvalidRowsRequest;
        errdefer parsed.deinit();
        if (parsed.value != .object) return error.InvalidRowsRequest;
        source_parsed.appendAssumeCapacity(parsed);
    }

    var matched_target_keys = std.StringHashMapUnmanaged(void).empty;
    defer matched_target_keys.deinit(alloc);

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll("{\"operations\":[");
    var wrote_operation = false;

    for (source_parsed.items) |source| {
        var matched_index: ?usize = null;
        for (target_parsed.items, 0..) |target, target_index| {
            if (try mergeRowsMatch(source.value, target.value, plan.match_fields)) {
                if (matched_index != null) return error.InvalidRowsRequest;
                matched_index = target_index;
            }
        }

        if (matched_index) |target_index| {
            const target = target_parsed.items[target_index].value;
            const arm = (try selectMergeMatchedArm(alloc, target, source.value, plan.matched_arms)) orelse continue;
            if (arm.do_nothing) continue;
            if (arm.update.len == 0 and arm.update_expressions.len == 0 and !arm.delete) continue;
            const gop = try matched_target_keys.getOrPut(alloc, target_rows[target_index].key);
            if (gop.found_existing) return error.InvalidRowsRequest;
            if (wrote_operation) try writer.writeByte(',');
            wrote_operation = true;
            if (arm.delete) {
                try writer.writeAll("{\"op\":\"delete\",\"where\":");
                try writeMergePrimaryWhereJson(writer, target_schema, target);
                try writer.print(",\"expected_version\":{d}", .{target_rows[target_index].version});
                try writeMergeReturningProjectionJson(writer, plan.returning);
                try writer.writeByte('}');
            } else {
                try writer.writeAll("{\"op\":\"update\",\"where\":");
                try writeMergePrimaryWhereJson(writer, target_schema, target);
                try writer.print(",\"expected_version\":{d},\"patch\":{{", .{target_rows[target_index].version});
                var wrote_patch_field = false;
                for (arm.update) |mapping| {
                    if (wrote_patch_field) try writer.writeByte(',');
                    wrote_patch_field = true;
                    try writer.print("{f}:", .{std.json.fmt(mapping.target_field, .{})});
                    try std.json.Stringify.value(try mergeObjectField(source.value, mapping.source_field), .{}, writer);
                }
                for (arm.update_expressions) |assignment| {
                    if (wrote_patch_field) try writer.writeByte(',');
                    wrote_patch_field = true;
                    const value_json = try relational_rows.expressionValueJsonWithTargetSourceAlloc(alloc, target, source.value, assignment.expression);
                    defer alloc.free(value_json);
                    try writer.print("{f}:{s}", .{ std.json.fmt(assignment.target_field, .{}), value_json });
                }
                try writer.writeByte('}');
                try writeMergeReturningProjectionJson(writer, plan.returning);
                try writer.writeByte('}');
            }
        } else {
            const arm = (try selectMergeNotMatchedArm(alloc, source.value, plan.not_matched_arms)) orelse continue;
            if (arm.do_nothing or (arm.insert.len == 0 and arm.insert_expressions.len == 0)) continue;
            if (wrote_operation) try writer.writeByte(',');
            wrote_operation = true;
            try writer.writeAll("{\"op\":\"insert\",\"row\":{");
            var wrote_insert_field = false;
            for (arm.insert) |mapping| {
                if (wrote_insert_field) try writer.writeByte(',');
                wrote_insert_field = true;
                try writer.print("{f}:", .{std.json.fmt(mapping.target_field, .{})});
                try std.json.Stringify.value(try mergeObjectField(source.value, mapping.source_field), .{}, writer);
            }
            for (arm.insert_expressions) |assignment| {
                if (wrote_insert_field) try writer.writeByte(',');
                wrote_insert_field = true;
                const value_json = try relational_rows.expressionValueJsonWithTargetSourceAlloc(alloc, source.value, source.value, assignment.expression);
                defer alloc.free(value_json);
                try writer.print("{f}:{s}", .{ std.json.fmt(assignment.target_field, .{}), value_json });
            }
            try writer.writeByte('}');
            try writeMergeReturningProjectionJson(writer, plan.returning);
            try writer.writeByte('}');
        }
    }

    try writer.writeAll("]}");
    const body_json = try out.toOwnedSlice();
    defer alloc.free(body_json);

    var resolver_ctx = MergeBatchResolverContext{
        .table_name = plan.target_table_name,
        .schema = target_schema,
        .rows = target_rows,
    };
    return try relational_rows.parseRowsBatchRequestWithResolver(
        alloc,
        plan.target_table_name,
        body_json,
        target_schema,
        resolver_ctx.resolver(),
    );
}

const MergeBatchResolverContext = struct {
    table_name: []const u8,
    schema: runtime_schema.TableSchema,
    rows: []const MergeExecutionTargetRow,

    fn resolver(self: *@This()) relational_rows.UniqueSelectorResolver {
        return .{
            .ptr = self,
            .resolve = resolveUnique,
            .resolve_temporal = resolveTemporalUnique,
            .resolve_primary = primaryExists,
            .lookup_primary = lookupPrimary,
        };
    }

    fn resolveUnique(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !?[]u8 {
        return null;
    }

    fn resolveTemporalUnique(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
    ) !?[]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!std.mem.eql(u8, constraint_name, db_mod.relational_store.primary_key_constraint_name)) return null;
        var found: ?[]const u8 = null;
        for (self.rows) |row| {
            if (!try relational_rows.temporalPrimaryKeyRowContainsPointAlloc(alloc, self.schema, row.json, encoded_value, encoded_point)) continue;
            if (found) |existing| {
                if (!std.mem.eql(u8, existing, row.key)) return error.UniqueConstraintViolation;
                continue;
            }
            found = row.key;
        }
        return if (found) |key| try alloc.dupe(u8, key) else null;
    }

    fn primaryExists(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, physical_key: []const u8) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return false;
        for (self.rows) |row| {
            if (std.mem.eql(u8, row.key, physical_key)) return true;
        }
        return false;
    }

    fn lookupPrimary(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, physical_key: []const u8) !?relational_rows.ResolvedPrimaryRow {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        for (self.rows) |row| {
            if (!std.mem.eql(u8, row.key, physical_key)) continue;
            return .{
                .json = try alloc.dupe(u8, row.json),
                .version = row.version,
            };
        }
        return null;
    }
};

fn mergeRowsMatch(
    source: std.json.Value,
    target: std.json.Value,
    mappings: []const MergeFieldMapping,
) !bool {
    for (mappings) |mapping| {
        if (!mergeJsonValuesEqual(try mergeObjectField(target, mapping.target_field), try mergeObjectField(source, mapping.source_field))) return false;
    }
    return true;
}

fn mergePredicatesMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    predicates: []const MergeArmPredicate,
) !bool {
    for (predicates) |predicate| {
        const row = switch (predicate.side) {
            .target => target,
            .source => source,
        };
        if (!try mergePredicateMatches(alloc, row, predicate)) return false;
    }
    return true;
}

fn selectMergeMatchedArm(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    arms: []const MergeMatchedArm,
) !?MergeMatchedArm {
    for (arms) |arm| {
        if (!try mergePredicatesMatch(alloc, target, source, arm.predicates)) continue;
        if (!try mergeExpressionPredicatesMatch(
            alloc,
            target,
            source,
            arm.expression_predicates,
            arm.expression_or_predicates,
            arm.expression_not_predicates,
        )) continue;
        return arm;
    }
    return null;
}

fn selectMergeNotMatchedArm(
    alloc: std.mem.Allocator,
    source: std.json.Value,
    arms: []const MergeNotMatchedArm,
) !?MergeNotMatchedArm {
    for (arms) |arm| {
        if (!try mergePredicatesMatch(alloc, .{ .null = {} }, source, arm.predicates)) continue;
        if (!try mergeExpressionPredicatesMatch(
            alloc,
            source,
            source,
            arm.expression_predicates,
            arm.expression_or_predicates,
            arm.expression_not_predicates,
        )) continue;
        return arm;
    }
    return null;
}

fn mergeExpressionPredicatesMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    predicates: []const db_mod.types.RelationalRowsExpressionCondition,
    or_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
    not_predicates: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    for (predicates) |predicate| {
        if (!try relational_rows.expressionConditionMatchesWithTargetSource(alloc, target, source, predicate)) return false;
    }
    if (!try mergeExpressionOrPredicateGroupsMatch(alloc, target, source, or_predicates)) return false;
    if (!try mergeExpressionNotPredicateGroupsMatch(alloc, target, source, not_predicates)) return false;
    return true;
}

fn mergeExpressionOrPredicateGroupsMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    if (groups.len == 0) return true;
    for (groups) |group| {
        var group_matches = true;
        for (group.conditions) |condition| {
            if (!try relational_rows.expressionConditionMatchesWithTargetSource(alloc, target, source, condition)) {
                group_matches = false;
                break;
            }
        }
        if (group_matches) return true;
    }
    return false;
}

fn mergeExpressionNotPredicateGroupsMatch(
    alloc: std.mem.Allocator,
    target: std.json.Value,
    source: std.json.Value,
    groups: []const db_mod.types.RelationalRowsExpressionPredicateGroup,
) !bool {
    for (groups) |group| {
        var group_matches = true;
        for (group.conditions) |condition| {
            if (!try relational_rows.expressionConditionMatchesWithTargetSource(alloc, target, source, condition)) {
                group_matches = false;
                break;
            }
        }
        if (group_matches) return false;
    }
    return true;
}

fn mergePredicateMatches(alloc: std.mem.Allocator, row: std.json.Value, predicate: MergeArmPredicate) !bool {
    if (predicate.op == .is_null) return row == .object and mergeObjectFieldOrNull(row, predicate.field) == null;
    if (predicate.op == .is_not_null) return row == .object and mergeObjectFieldOrNull(row, predicate.field) != null;
    const actual = try mergeObjectField(row, predicate.field);
    const value_json = predicate.value_json orelse return error.InvalidRowsRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (predicate.op == .is_distinct or predicate.op == .is_not_distinct) {
        const equal = mergeJsonValuesNotDistinct(actual, parsed.value);
        return if (predicate.op == .is_distinct) !equal else equal;
    }
    const cmp = mergeJsonCompare(actual, parsed.value) orelse return false;
    return switch (predicate.op) {
        .eq => cmp == .eq,
        .ne => cmp != .eq,
        .gt => cmp == .gt,
        .gte => cmp == .gt or cmp == .eq,
        .lt => cmp == .lt,
        .lte => cmp == .lt or cmp == .eq,
        .is_distinct, .is_not_distinct => unreachable,
        .is_null, .is_not_null => unreachable,
    };
}

const MergeScalarComparison = enum { lt, eq, gt };

fn mergeJsonValuesEqual(left: std.json.Value, right: std.json.Value) bool {
    const cmp = mergeJsonCompare(left, right) orelse return false;
    return cmp == .eq;
}

fn mergeJsonValuesNotDistinct(left: std.json.Value, right: std.json.Value) bool {
    if (left == .null and right == .null) return true;
    if (left == .null or right == .null) return false;
    return mergeJsonValuesEqual(left, right);
}

fn mergeJsonCompare(left: std.json.Value, right: std.json.Value) ?MergeScalarComparison {
    if (mergeJsonNumericValue(left)) |left_num| {
        const right_num = mergeJsonNumericValue(right) orelse return null;
        if (left_num < right_num) return .lt;
        if (left_num > right_num) return .gt;
        return .eq;
    }
    if (left == .string and right == .string) {
        return switch (std.mem.order(u8, left.string, right.string)) {
            .lt => .lt,
            .eq => .eq,
            .gt => .gt,
        };
    }
    if (left == .bool and right == .bool) {
        if (left.bool == right.bool) return .eq;
        return if (!left.bool and right.bool) .lt else .gt;
    }
    return null;
}

fn mergeJsonNumericValue(value: std.json.Value) ?f64 {
    return switch (value) {
        .integer => |integer| @floatFromInt(integer),
        .float => |float| float,
        else => null,
    };
}

fn mergeObjectField(value: std.json.Value, field: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidRowsRequest;
    return mergeObjectFieldOrNull(value, field) orelse return error.InvalidRowsRequest;
}

fn mergeObjectFieldOrNull(value: std.json.Value, field: []const u8) ?std.json.Value {
    if (value != .object) return null;
    return value.object.get(field);
}

fn writeMergePrimaryWhereJson(
    writer: *std.Io.Writer,
    schema: runtime_schema.TableSchema,
    row: std.json.Value,
) !void {
    const primary_key = schema.primary_key orelse return error.InvalidRowsRequest;
    if (primary_key.without_overlaps_period) |period_name| {
        const period = binder.relationalPeriodForDdl(schema.periods, period_name) orelse return error.InvalidRowsRequest;
        try writer.writeAll("{\"primary\":{\"values\":{");
        for (primary_key.columns, 0..) |column, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}:", .{std.json.fmt(column, .{})});
            try std.json.Stringify.value(try mergeObjectField(row, column), .{}, writer);
        }
        try writer.print("}},\"period\":{{\"name\":{f},\"at\":", .{std.json.fmt(period.name, .{})});
        try std.json.Stringify.value(try mergeObjectField(row, period.start_column), .{}, writer);
        try writer.writeAll("}}}");
        return;
    }
    try writer.writeAll("{\"primary\":{");
    for (primary_key.columns, 0..) |column, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(column, .{})});
        try std.json.Stringify.value(try mergeObjectField(row, column), .{}, writer);
    }
    try writer.writeAll("}}");
}

fn writeMergeReturningProjectionJson(writer: *std.Io.Writer, returning: ReturningProjection) !void {
    if (returning.fields.len > 0) {
        try writer.writeAll(",\"returning\":[");
        for (returning.fields, 0..) |field, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{f}", .{std.json.fmt(field, .{})});
        }
        try writer.writeByte(']');
    }
    if (returning.expressions.len == 0) return;
    try writer.writeAll(",\"returning_expressions\":[");
    for (returning.expressions, 0..) |projection, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{{\"as\":{f},\"expr\":", .{std.json.fmt(projection.output, .{})});
        try lower_expr.writeRowExpressionJson(writer, projection.expression);
        try writer.writeByte('}');
    }
    try writer.writeByte(']');
}

pub fn sqlJsonSetPathsConflict(
    lhs_field_path: []const u8,
    lhs_json_path: []const []const u8,
    rhs_field_path: []const u8,
    rhs_json_path: []const []const u8,
) bool {
    if (!std.mem.eql(u8, lhs_field_path, rhs_field_path)) return sqlDottedPathsConflict(lhs_field_path, rhs_field_path);
    const shared = @min(lhs_json_path.len, rhs_json_path.len);
    for (lhs_json_path[0..shared], rhs_json_path[0..shared]) |lhs_segment, rhs_segment| {
        if (!std.mem.eql(u8, lhs_segment, rhs_segment)) return false;
    }
    return true;
}

pub fn sqlDottedPathConflictsJsonSetPath(
    path: []const u8,
    json_field_path: []const u8,
    json_path: []const []const u8,
) bool {
    if (sqlDottedPathsConflict(path, json_field_path)) return true;
    if (path.len <= json_field_path.len + 1) return false;
    if (!std.mem.startsWith(u8, path, json_field_path) or path[json_field_path.len] != '.') return false;
    return sqlJsonSegmentsConflictDottedPath(json_path, path[json_field_path.len + 1 ..]);
}

pub fn jsonSetTypedTransformPathAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    path_segments: []const []const u8,
) ![]u8 {
    if (field.len == 0 or path_segments.len == 0) return error.UnsupportedSqlShape;
    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;
    try writer.writeAll(field);
    for (path_segments) |segment| {
        if (segment.len == 0 or std.mem.indexOfScalar(u8, segment, '.') != null) return error.UnsupportedSqlShape;
        try writer.writeByte('.');
        try writer.writeAll(segment);
    }
    return try out.toOwnedSlice();
}

pub fn sqlDottedPathsConflict(lhs: []const u8, rhs: []const u8) bool {
    if (std.mem.eql(u8, lhs, rhs)) return true;
    return sqlDottedPathIsAncestor(lhs, rhs) or sqlDottedPathIsAncestor(rhs, lhs);
}

pub fn sqlDottedPathIsAncestor(parent: []const u8, child: []const u8) bool {
    return parent.len < child.len and
        std.mem.startsWith(u8, child, parent) and
        child[parent.len] == '.';
}

pub fn sqlJsonSegmentsConflictDottedPath(json_path: []const []const u8, dotted_path: []const u8) bool {
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

test "sql adapter lower dml detects dotted path conflicts" {
    try std.testing.expect(sqlDottedPathsConflict("metadata", "metadata.status"));
    try std.testing.expect(sqlDottedPathsConflict("metadata.status", "metadata"));
    try std.testing.expect(sqlDottedPathsConflict("metadata.status", "metadata.status"));
    try std.testing.expect(!sqlDottedPathsConflict("metadata.status", "metadata_status"));
    try std.testing.expect(!sqlDottedPathsConflict("metadata.status", "metadata.state"));
}

test "sql adapter lower dml detects json set path conflicts" {
    const status_path = [_][]const u8{"status"};
    const nested_status_path = [_][]const u8{ "profile", "status" };
    const profile_path = [_][]const u8{"profile"};
    const settings_path = [_][]const u8{"settings"};

    try std.testing.expect(sqlJsonSetPathsConflict("metadata", &status_path, "metadata", &status_path));
    try std.testing.expect(sqlJsonSetPathsConflict("metadata", &profile_path, "metadata", &nested_status_path));
    try std.testing.expect(!sqlJsonSetPathsConflict("metadata", &status_path, "metadata", &settings_path));
    try std.testing.expect(sqlJsonSetPathsConflict("metadata", &status_path, "metadata.status", &settings_path));
    try std.testing.expect(sqlDottedPathConflictsJsonSetPath("metadata.profile.status", "metadata", &profile_path));
    try std.testing.expect(sqlDottedPathConflictsJsonSetPath("metadata.profile.status", "metadata", &settings_path));
    try std.testing.expect(!sqlDottedPathConflictsJsonSetPath("profile.status", "metadata", &settings_path));

    const alloc = std.testing.allocator;
    const typed_path = try jsonSetTypedTransformPathAlloc(alloc, "metadata", &nested_status_path);
    defer alloc.free(typed_path);
    try std.testing.expectEqualStrings("metadata.profile.status", typed_path);
    try std.testing.expectError(error.UnsupportedSqlShape, jsonSetTypedTransformPathAlloc(alloc, "metadata", &.{""}));
    try std.testing.expectError(error.UnsupportedSqlShape, jsonSetTypedTransformPathAlloc(alloc, "metadata", &.{"bad.segment"}));
}

test "sql adapter lower dml detects merge target row usage" {
    const source_field = runtime_schema.RelationalRowsExpression{
        .kind = .field,
        .field = "status",
        .field_source = .source,
    };
    const target_field = runtime_schema.RelationalRowsExpression{
        .kind = .field,
        .field = "status",
        .field_source = .row,
    };
    const source_condition = runtime_schema.RelationalRowsExpressionCondition{
        .lhs = source_field,
        .op = .eq,
        .rhs = &.{source_field},
    };
    const target_condition = runtime_schema.RelationalRowsExpressionCondition{
        .lhs = source_field,
        .op = .eq,
        .rhs = &.{target_field},
    };
    const source_groups = [_]runtime_schema.RelationalRowsExpressionPredicateGroup{.{ .conditions = &.{source_condition} }};
    const target_groups = [_]runtime_schema.RelationalRowsExpressionPredicateGroup{.{ .conditions = &.{target_condition} }};

    try std.testing.expect(!mergeExpressionUsesTargetRow(source_field));
    try std.testing.expect(mergeExpressionUsesTargetRow(target_field));
    try std.testing.expect(!mergeExpressionPredicateGroupsUseTargetRow(&source_groups));
    try std.testing.expect(mergeExpressionPredicateGroupsUseTargetRow(&target_groups));
}
