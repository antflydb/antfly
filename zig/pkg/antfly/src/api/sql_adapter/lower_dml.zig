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
const runtime_schema = @import("../../storage/schema.zig");

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
