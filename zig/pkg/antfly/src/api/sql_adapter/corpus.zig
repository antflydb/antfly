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

const diagnostics = @import("diagnostics.zig");

pub const UnsupportedPlanFamily = enum {
    query,
    read,
    ddl,
    write,
    insert,
    update,
    update_source,
    delete,
    update_joined_source,
    delete_joined_source,
    merge_mutation,
};

pub fn unsupportedPlanFamilyToken(family: UnsupportedPlanFamily) []const u8 {
    return @tagName(family);
}

pub fn unsupportedFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "unsupported:{s}:requires={s}", .{
        unsupportedPlanFamilyToken(family),
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn adapterNoopFingerprintAlloc(
    alloc: std.mem.Allocator,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) ![]u8 {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return error.UnsupportedSqlShape;
    return try std.fmt.allocPrint(alloc, "adapter_noop:{s}:reason={s}", .{
        family,
        diagnostics.classificationReasonToken(reason),
    });
}

pub fn unsupportedPlanMatchesFamily(plan: []const u8, family: UnsupportedPlanFamily) bool {
    const prefix = "unsupported:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const family_token = unsupportedPlanFamilyToken(family);
    const rest = plan[prefix.len..];
    return std.mem.startsWith(u8, rest, family_token) and
        rest.len > family_token.len and
        rest[family_token.len] == ':';
}

pub fn unsupportedPlanMatchesReason(
    plan: []const u8,
    family: UnsupportedPlanFamily,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsUnsupportedRequirement(reason)) return false;
    if (!unsupportedPlanMatchesFamily(plan, family)) return false;
    return planHasExactStringToken(plan, ":requires=", diagnostics.classificationReasonToken(reason));
}

pub fn adapterNoopPlanMatchesReason(
    plan: []const u8,
    family: []const u8,
    reason: diagnostics.SqlAdapterClassificationReason,
) bool {
    if (!diagnostics.classificationReasonIsAdapterNoop(reason)) return false;
    const prefix = "adapter_noop:";
    if (!std.mem.startsWith(u8, plan, prefix)) return false;
    const rest = plan[prefix.len..];
    if (!std.mem.startsWith(u8, rest, family) or rest.len <= family.len or rest[family.len] != ':') return false;
    return planHasExactStringToken(plan, ":reason=", diagnostics.classificationReasonToken(reason));
}

pub const PlanStringTokenScan = union(enum) {
    absent,
    value: []const u8,
    invalid,
};

pub fn scanStringToken(plan: []const u8, token: []const u8) PlanStringTokenScan {
    var start: usize = 0;
    var found: ?[]const u8 = null;
    while (std.mem.indexOfPos(u8, plan, start, token)) |index| {
        const value_start = index + token.len;
        var value_end = value_start;
        while (value_end < plan.len and plan[value_end] != ':') : (value_end += 1) {}
        if (found != null) return .invalid;
        found = plan[value_start..value_end];
        start = index + token.len;
    }
    if (found) |value| return .{ .value = value };
    return .absent;
}

pub fn planHasExactStringToken(plan: []const u8, token: []const u8, expected: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| std.mem.eql(u8, value, expected),
        .absent, .invalid => false,
    };
}

pub fn planHasStringToken(plan: []const u8, token: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| value.len > 0,
        .absent, .invalid => false,
    };
}

pub fn planTokenAbsent(plan: []const u8, token: []const u8) bool {
    return switch (scanStringToken(plan, token)) {
        .absent => true,
        .value, .invalid => false,
    };
}

pub fn planHasAnyExactStringToken(plan: []const u8, token: []const u8, expected_values: []const []const u8) bool {
    for (expected_values) |expected| {
        if (planHasExactStringToken(plan, token, expected)) return true;
    }
    return false;
}

pub fn parseDelimitedUsizeToken(plan: []const u8, value_start: usize) ?usize {
    var pos = value_start;
    if (pos >= plan.len or plan[pos] < '0' or plan[pos] > '9') return null;
    var value: usize = 0;
    while (pos < plan.len and plan[pos] >= '0' and plan[pos] <= '9') : (pos += 1) {
        value = value * 10 + @as(usize, plan[pos] - '0');
    }
    if (pos != plan.len and plan[pos] != ':') return null;
    return value;
}

pub const PlanUsizeTokenScan = union(enum) {
    absent,
    value: usize,
    invalid,
};

pub fn scanUsizeToken(plan: []const u8, token: []const u8) PlanUsizeTokenScan {
    var start: usize = 0;
    var found: ?usize = null;
    while (std.mem.indexOfPos(u8, plan, start, token)) |index| {
        const parsed = parseDelimitedUsizeToken(plan, index + token.len) orelse return .invalid;
        if (found != null) return .invalid;
        found = parsed;
        start = index + token.len;
    }
    if (found) |value| return .{ .value = value };
    return .absent;
}

pub fn planUsizeTokenValue(plan: []const u8, token: []const u8) ?usize {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value,
        .absent, .invalid => null,
    };
}

pub fn planHasNonZeroToken(plan: []const u8, token: []const u8) bool {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value > 0,
        .absent, .invalid => false,
    };
}

pub fn planHasNonZeroUsizeTokenNamePrefix(plan: []const u8, name_prefix: []const u8) bool {
    var segment_start: usize = 0;
    var found_non_zero = false;
    while (segment_start < plan.len) {
        var segment_end = segment_start;
        while (segment_end < plan.len and plan[segment_end] != ':') : (segment_end += 1) {}
        const segment = plan[segment_start..segment_end];
        if (std.mem.indexOfScalar(u8, segment, '=')) |equals_index| {
            if (std.mem.startsWith(u8, segment[0..equals_index], name_prefix)) {
                const value = parseDelimitedUsizeToken(segment, equals_index + 1) orelse return false;
                found_non_zero = found_non_zero or value > 0;
            }
        }
        segment_start = segment_end + 1;
    }
    return found_non_zero;
}

pub fn planHasExactUsizeToken(plan: []const u8, token: []const u8, expected: usize) bool {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value == expected,
        .absent, .invalid => false,
    };
}

pub fn planUsizeOptionalTokenValue(plan: []const u8, token: []const u8) ?usize {
    return switch (scanUsizeToken(plan, token)) {
        .value => |value| value,
        .absent => 0,
        .invalid => null,
    };
}

pub fn planBoolTokenValue(plan: []const u8, token: []const u8) ?bool {
    return switch (scanStringToken(plan, token)) {
        .value => |value| blk: {
            if (std.mem.eql(u8, value, "true")) break :blk true;
            if (std.mem.eql(u8, value, "false")) break :blk false;
            break :blk null;
        },
        .absent, .invalid => null,
    };
}

pub fn planBoolTokenUsize(plan: []const u8, token: []const u8) ?usize {
    return switch (scanStringToken(plan, token)) {
        .absent => 0,
        .value => |value| blk: {
            if (std.mem.eql(u8, value, "true")) break :blk 1;
            if (std.mem.eql(u8, value, "false")) break :blk 0;
            break :blk null;
        },
        .invalid => null,
    };
}

pub fn planHasExactBoolToken(plan: []const u8, token: []const u8, expected: bool) bool {
    const value = planBoolTokenValue(plan, token) orelse return false;
    return value == expected;
}

pub const SqlParameterScan = union(enum) {
    absent,
    value: usize,
    invalid,
};

pub fn sqlHasParameterIndex(sql: []const u8, expected: usize) bool {
    var index: usize = 0;
    while (sqlNextParameter(sql, &index)) |scan| {
        switch (scan) {
            .value => |param_index| if (param_index == expected) return true,
            .absent, .invalid => {},
        }
    }
    return false;
}

pub fn sqlParameterCoverageMatches(sql: []const u8, param_count: usize) bool {
    var index: usize = 0;
    var saw_parameter = false;
    var max_index: usize = 0;
    while (sqlNextParameter(sql, &index)) |scan| {
        switch (scan) {
            .value => |param_index| {
                if (param_index == 0 or param_index > param_count) return false;
                saw_parameter = true;
                max_index = @max(max_index, param_index);
            },
            .invalid => return false,
            .absent => {},
        }
    }
    if (param_count == 0) return !saw_parameter;
    if (!saw_parameter or max_index != param_count) return false;

    for (1..param_count + 1) |param_index| {
        if (!sqlHasParameterIndex(sql, param_index)) return false;
    }
    return true;
}

pub fn sqlNextParameter(sql: []const u8, index: *usize) ?SqlParameterScan {
    while (index.* < sql.len) {
        switch (sql[index.*]) {
            '\'' => {
                index.* = sqlSingleQuotedEnd(sql, index.*);
                continue;
            },
            '"' => {
                index.* = sqlDoubleQuotedEnd(sql, index.*);
                continue;
            },
            '-' => {
                if (index.* + 1 < sql.len and sql[index.* + 1] == '-') {
                    index.* = sqlLineCommentEnd(sql, index.*);
                    continue;
                }
            },
            '/' => {
                if (index.* + 1 < sql.len and sql[index.* + 1] == '*') {
                    index.* = sqlBlockCommentEnd(sql, index.*);
                    continue;
                }
            },
            '$' => {
                const dollar = index.*;
                if (sqlParameterIndexAt(sql, dollar)) |scan| {
                    index.* = sqlParameterTokenEnd(sql, dollar);
                    return scan;
                }
                if (sqlDollarQuotedEnd(sql, dollar)) |end| {
                    index.* = end;
                    continue;
                }
            },
            else => {},
        }
        index.* += 1;
    }
    return null;
}

fn sqlParameterIndexAt(sql: []const u8, dollar: usize) ?SqlParameterScan {
    if (dollar + 1 >= sql.len) return null;
    if (sql[dollar] != '$') return null;
    if (sql[dollar + 1] < '0' or sql[dollar + 1] > '9') return null;

    var index = dollar + 1;
    var value: usize = 0;
    while (index < sql.len and sql[index] >= '0' and sql[index] <= '9') : (index += 1) {
        value = value * 10 + (sql[index] - '0');
    }
    if (index < sql.len and (std.ascii.isAlphanumeric(sql[index]) or sql[index] == '_')) return .invalid;
    return .{ .value = value };
}

fn sqlParameterTokenEnd(sql: []const u8, dollar: usize) usize {
    var index = dollar + 1;
    while (index < sql.len and sql[index] >= '0' and sql[index] <= '9') : (index += 1) {}
    return index;
}

fn sqlSingleQuotedEnd(sql: []const u8, quote: usize) usize {
    var index = quote + 1;
    while (index < sql.len) : (index += 1) {
        if (sql[index] != '\'') continue;
        if (index + 1 < sql.len and sql[index + 1] == '\'') {
            index += 1;
            continue;
        }
        return index + 1;
    }
    return sql.len;
}

fn sqlDoubleQuotedEnd(sql: []const u8, quote: usize) usize {
    var index = quote + 1;
    while (index < sql.len) : (index += 1) {
        if (sql[index] != '"') continue;
        if (index + 1 < sql.len and sql[index + 1] == '"') {
            index += 1;
            continue;
        }
        return index + 1;
    }
    return sql.len;
}

fn sqlLineCommentEnd(sql: []const u8, dash: usize) usize {
    var index = dash + 2;
    while (index < sql.len and sql[index] != '\n' and sql[index] != '\r') : (index += 1) {}
    return index;
}

fn sqlBlockCommentEnd(sql: []const u8, slash: usize) usize {
    var index = slash + 2;
    while (index + 1 < sql.len) : (index += 1) {
        if (sql[index] == '*' and sql[index + 1] == '/') return index + 2;
    }
    return sql.len;
}

fn sqlDollarQuotedEnd(sql: []const u8, dollar: usize) ?usize {
    if (dollar + 1 >= sql.len) return null;
    if (sql[dollar + 1] >= '0' and sql[dollar + 1] <= '9') return null;

    var delimiter_end = dollar + 1;
    if (sql[delimiter_end] == '$') {
        delimiter_end += 1;
    } else {
        if (!sqlDollarQuoteTagStart(sql[delimiter_end])) return null;
        delimiter_end += 1;
        while (delimiter_end < sql.len and sqlDollarQuoteTagContinue(sql[delimiter_end])) : (delimiter_end += 1) {}
        if (delimiter_end >= sql.len or sql[delimiter_end] != '$') return null;
        delimiter_end += 1;
    }

    const delimiter = sql[dollar..delimiter_end];
    const body_start = delimiter_end;
    if (std.mem.indexOfPos(u8, sql, body_start, delimiter)) |close| {
        return close + delimiter.len;
    }
    return sql.len;
}

fn sqlDollarQuoteTagStart(ch: u8) bool {
    return std.ascii.isAlphabetic(ch) or ch == '_';
}

fn sqlDollarQuoteTagContinue(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_';
}

test "sql adapter corpus fingerprints unsupported and adapter no-op reasons" {
    const alloc = std.testing.allocator;
    const unsupported = try unsupportedFingerprintAlloc(alloc, .write, .multi_table_generation_barrier);
    defer alloc.free(unsupported);
    try std.testing.expectEqualStrings("unsupported:write:requires=multi_table_generation_barrier", unsupported);
    try std.testing.expect(unsupportedPlanMatchesReason(unsupported, .write, .multi_table_generation_barrier));
    try std.testing.expect(!unsupportedPlanMatchesReason(unsupported, .write, .session_setting));
    try std.testing.expect(!unsupportedPlanMatchesReason("unsupported:write:requires=multi_table_generation_barrier_extra", .write, .multi_table_generation_barrier));

    const noop = try adapterNoopFingerprintAlloc(alloc, "ddl", .session_setting);
    defer alloc.free(noop);
    try std.testing.expectEqualStrings("adapter_noop:ddl:reason=session_setting", noop);
    try std.testing.expect(adapterNoopPlanMatchesReason(noop, "ddl", .session_setting));
    try std.testing.expect(!adapterNoopPlanMatchesReason(noop, "ddl", .set_operation_plan));
    try std.testing.expect(!adapterNoopPlanMatchesReason("adapter_noop:ddl:reason=session_setting_extra", "ddl", .session_setting));

    try std.testing.expectError(error.UnsupportedSqlShape, unsupportedFingerprintAlloc(alloc, .write, .session_setting));
    try std.testing.expectError(error.UnsupportedSqlShape, adapterNoopFingerprintAlloc(alloc, "ddl", .set_operation_plan));
}

test "sql adapter corpus string token matching is exact and unique" {
    const plan = "query:table=usage_records:claim=no_key_update_nowait:limit=none";
    try std.testing.expect(planHasExactStringToken(plan, ":claim=", "no_key_update_nowait"));
    try std.testing.expect(!planHasExactStringToken(plan, ":claim=", "no_key_update"));
    try std.testing.expect(planHasStringToken(plan, ":limit="));
    try std.testing.expect(planTokenAbsent(plan, ":offset="));
    try std.testing.expect(planHasAnyExactStringToken(plan, ":claim=", &.{
        "no_key_update",
        "no_key_update_nowait",
    }));

    const duplicate = "read:query:table=usage_records:table=usage_records";
    try std.testing.expect(!planHasExactStringToken(duplicate, ":table=", "usage_records"));
    try std.testing.expect(!planHasStringToken(duplicate, ":table="));
}

test "sql adapter corpus numeric and bool token matching is exact and unique" {
    const plan = "applied:rebuild=true:validation=false:rewrite=false:unvalidated_unique=10:unvalidated_fk=1";
    try std.testing.expect(planHasExactBoolToken(plan, "rebuild=", true));
    try std.testing.expect(planHasExactBoolToken(plan, "validation=", false));
    try std.testing.expectEqual(@as(?usize, 10), planUsizeTokenValue(plan, "unvalidated_unique="));
    try std.testing.expect(planHasExactUsizeToken(plan, "unvalidated_fk=", 1));
    try std.testing.expect(planHasNonZeroToken(plan, "unvalidated_unique="));
    try std.testing.expect(planHasNonZeroUsizeTokenNamePrefix(plan, "unvalidated_"));
    try std.testing.expectEqual(@as(?usize, 0), planUsizeOptionalTokenValue(plan, "missing="));
    try std.testing.expect(!planHasExactUsizeToken("query:pred=10x", "pred=", 10));
    try std.testing.expect(!planHasExactUsizeToken("query:pred=1:pred=1", "pred=", 1));
    try std.testing.expect(!planHasExactBoolToken("ddl:replace=true_extra", "replace=", true));
}

test "sql adapter corpus placeholder coverage ignores literals and comments" {
    try std.testing.expect(sqlParameterCoverageMatches(
        "SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $2",
        2,
    ));
    try std.testing.expect(!sqlParameterCoverageMatches(
        "SELECT id FROM usage_records WHERE tenant_id = $1 AND user_id = $3",
        3,
    ));
    try std.testing.expect(!sqlParameterCoverageMatches(
        "SELECT id FROM usage_records WHERE tenant_id = $1abc",
        1,
    ));
    try std.testing.expect(sqlParameterCoverageMatches(
        "SELECT '$1', $$ $2 $$, id FROM usage_records -- $3abc\nWHERE tenant_id = $1",
        1,
    ));
}
