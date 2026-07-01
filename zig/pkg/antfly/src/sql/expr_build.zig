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

const db_mod = @import("../storage/db/mod.zig");
const expr_limits = @import("expr_limits.zig");
const plan_mod = @import("plan.zig");
const platform_time = @import("../platform/time.zig");
const token_mod = @import("token.zig");
const value_mod = @import("value.zig");

const Token = token_mod.Token;
const freeExpression = plan_mod.freeExpression;
const max_scalar_or_expanded_branches = expr_limits.max_scalar_or_expanded_branches;

pub fn buildNowRowExpressionAlloc(alloc: std.mem.Allocator) !db_mod.types.RelationalRowsExpression {
    return .{
        .kind = .now,
        .value_json = try std.fmt.allocPrint(alloc, "{d}", .{platform_time.realtimeNs()}),
    };
}

pub fn parseSqlNowRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    try value_mod.parseSqlNowCall(tokens, pos);
    return try buildNowRowExpressionAlloc(alloc);
}

pub fn parseSqlCurrentDateRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    try value_mod.parseSqlCurrentDateKeyword(tokens, pos);
    return try buildCurrentDateExpressionAlloc(alloc);
}

pub fn parseSqlTypedDatetimeLiteralRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    return .{
        .kind = .value,
        .value_json = try value_mod.parseSqlTypedDatetimeLiteralValueJsonAlloc(alloc, tokens, pos),
    };
}

pub fn parseSqlUuidV4RowExpression(tokens: []const Token, pos: *usize) !db_mod.types.RelationalRowsExpression {
    try value_mod.parseSqlUuidV4Call(tokens, pos);
    return .{ .kind = .uuid_v4 };
}

pub fn parseSqlIntervalRowExpressionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsExpression {
    const interval = try value_mod.parseSqlIntervalLiteral(tokens, pos);
    return try buildSingleIntervalLiteralExpressionAlloc(alloc, interval);
}

pub fn buildSingleIntervalLiteralExpressionAlloc(
    alloc: std.mem.Allocator,
    interval: value_mod.SqlIntervalLiteral,
) !db_mod.types.RelationalRowsExpression {
    const has_calendar = interval.calendar_months != 0 or (interval.saw_calendar and interval.fixed_ns == 0);
    const has_fixed = interval.fixed_ns != 0 or (interval.saw_fixed and !has_calendar);
    if (has_calendar and has_fixed) return error.UnsupportedSqlShape;
    if (has_calendar) return try buildIntervalComponentExpressionAlloc(alloc, .interval_months, interval.calendar_months);
    if (has_fixed) return try buildIntervalComponentExpressionAlloc(alloc, .interval_ns, interval.fixed_ns);
    return error.UnsupportedSqlShape;
}

pub fn buildIntervalComponentExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    value: u64,
) !db_mod.types.RelationalRowsExpression {
    if (kind != .interval_ns and kind != .interval_months) return error.UnsupportedSqlShape;
    const value_json = try std.fmt.allocPrint(alloc, "{d}", .{value});
    var value_transferred = false;
    errdefer if (!value_transferred) alloc.free(value_json);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = .{ .kind = .value, .value_json = value_json };
    value_transferred = true;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildIntervalLiteralArithmeticAlloc(
    alloc: std.mem.Allocator,
    lhs: db_mod.types.RelationalRowsExpression,
    op_kind: db_mod.types.RelationalRowsExpressionKind,
    interval: value_mod.SqlIntervalLiteral,
) !db_mod.types.RelationalRowsExpression {
    if (op_kind != .add and op_kind != .sub) return error.UnsupportedSqlShape;

    const has_calendar = interval.calendar_months != 0;
    const has_fixed = interval.fixed_ns != 0;
    if (!has_calendar and !has_fixed) {
        const rhs = try buildSingleIntervalLiteralExpressionAlloc(alloc, interval);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        const expression = try buildBinaryExpressionAlloc(alloc, op_kind, lhs, rhs);
        rhs_owned = false;
        return expression;
    }
    if (has_calendar and !has_fixed) {
        const rhs = try buildIntervalComponentExpressionAlloc(alloc, .interval_months, interval.calendar_months);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        const expression = try buildBinaryExpressionAlloc(alloc, op_kind, lhs, rhs);
        rhs_owned = false;
        return expression;
    }
    if (!has_calendar and has_fixed) {
        const rhs = try buildIntervalComponentExpressionAlloc(alloc, .interval_ns, interval.fixed_ns);
        var rhs_owned = true;
        errdefer if (rhs_owned) freeExpression(alloc, rhs);
        const expression = try buildBinaryExpressionAlloc(alloc, op_kind, lhs, rhs);
        rhs_owned = false;
        return expression;
    }

    const calendar = try buildIntervalComponentExpressionAlloc(alloc, .interval_months, interval.calendar_months);
    var calendar_owned = true;
    errdefer if (calendar_owned) freeExpression(alloc, calendar);
    const fixed = try buildIntervalComponentExpressionAlloc(alloc, .interval_ns, interval.fixed_ns);
    var fixed_owned = true;
    errdefer if (fixed_owned) freeExpression(alloc, fixed);

    const inner_operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var inner_operands_owned = true;
    errdefer if (inner_operands_owned) alloc.free(inner_operands);
    const outer_operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var outer_operands_owned = true;
    errdefer if (outer_operands_owned) alloc.free(outer_operands);

    inner_operands[0] = lhs;
    inner_operands[1] = calendar;
    calendar_owned = false;
    inner_operands_owned = false;
    outer_operands[0] = .{
        .kind = op_kind,
        .operands = inner_operands,
    };
    outer_operands[1] = fixed;
    fixed_owned = false;
    outer_operands_owned = false;
    return .{
        .kind = op_kind,
        .operands = outer_operands,
    };
}

pub fn buildBinaryExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    lhs: db_mod.types.RelationalRowsExpression,
    rhs: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_owned = true;
    errdefer if (operands_owned) alloc.free(operands);
    operands[0] = lhs;
    operands[1] = rhs;
    operands_owned = false;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildUnaryNegativeExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const negative_one = try alloc.dupe(u8, "-1");
    var negative_one_transferred = false;
    errdefer if (!negative_one_transferred) alloc.free(negative_one);
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = .{ .kind = .value, .value_json = negative_one };
    operands[1] = operand;
    negative_one_transferred = true;
    operands_transferred = true;
    return .{
        .kind = .mul,
        .operands = operands,
    };
}

pub fn buildCurrentDateExpressionAlloc(
    alloc: std.mem.Allocator,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var initialized: usize = 0;
    errdefer {
        for (operands[0..initialized]) |operand| freeExpression(alloc, operand);
        alloc.free(operands);
    }

    operands[0] = .{
        .kind = .value,
        .value_json = try std.json.Stringify.valueAlloc(alloc, "day", .{}),
    };
    initialized += 1;
    operands[1] = try buildNowRowExpressionAlloc(alloc);
    initialized += 1;

    return .{
        .kind = .date_trunc,
        .operands = operands,
    };
}

pub fn buildCastExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
    cast_type: db_mod.types.RelationalRowsExpressionCastType,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;

    operands_transferred = true;
    return .{
        .kind = .cast,
        .operands = operands,
        .cast_type = cast_type,
    };
}

pub fn buildUnaryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    operand: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildBinaryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    lhs: db_mod.types.RelationalRowsExpression,
    rhs: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 2);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = lhs;
    operands[1] = rhs;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildTernaryFunctionExpressionAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    first: db_mod.types.RelationalRowsExpression,
    second: db_mod.types.RelationalRowsExpression,
    third: db_mod.types.RelationalRowsExpression,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 3);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = first;
    operands[1] = second;
    operands[2] = third;
    operands_transferred = true;
    return .{
        .kind = kind,
        .operands = operands,
    };
}

pub fn buildFunctionExpressionFromOperandListAlloc(
    alloc: std.mem.Allocator,
    kind: db_mod.types.RelationalRowsExpressionKind,
    operands: *std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression),
) !db_mod.types.RelationalRowsExpression {
    const owned_operands = try operands.toOwnedSlice(alloc);
    operands.* = .empty;
    return .{
        .kind = kind,
        .operands = owned_operands,
    };
}

pub fn buildExpressionProjection(
    output: []const u8,
    expression: db_mod.types.RelationalRowsExpression,
) db_mod.types.RelationalRowsExpressionProjection {
    return .{
        .output = output,
        .expression = expression,
    };
}

pub fn buildJsonExtractExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
    path: []const u8,
    as_text: bool,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operands_transferred = true;
    return .{
        .kind = .json_extract,
        .json_path = path,
        .json_as_text = as_text,
        .operands = operands,
    };
}

pub fn buildJsonPathExistsExpressionAlloc(
    alloc: std.mem.Allocator,
    operand: db_mod.types.RelationalRowsExpression,
    path: []const u8,
) !db_mod.types.RelationalRowsExpression {
    const operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
    var operands_transferred = false;
    errdefer if (!operands_transferred) alloc.free(operands);
    operands[0] = operand;
    operands_transferred = true;
    return .{
        .kind = .json_path_exists,
        .json_path = path,
        .operands = operands,
    };
}

pub fn buildJsonKeySetExistsExpressionAlloc(
    alloc: std.mem.Allocator,
    field: []const u8,
    source: db_mod.types.RelationalRowsExpressionFieldSource,
    match_all: bool,
    values_json: []const u8,
) !db_mod.types.RelationalRowsExpression {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, values_json, .{}) catch return error.UnsupportedSqlShape;
    defer parsed.deinit();
    if (parsed.value != .array or parsed.value.array.items.len == 0 or parsed.value.array.items.len > max_scalar_or_expanded_branches) return error.UnsupportedSqlShape;

    var operands = std.ArrayListUnmanaged(db_mod.types.RelationalRowsExpression).empty;
    var operands_transferred = false;
    errdefer {
        if (!operands_transferred) {
            for (operands.items) |operand| freeExpression(alloc, operand);
            operands.deinit(alloc);
        }
    }

    for (parsed.value.array.items) |value| {
        if (value != .string or value.string.len == 0 or std.mem.indexOfScalar(u8, value.string, '.') != null) return error.UnsupportedSqlShape;
        const owned_field = try alloc.dupe(u8, field);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(owned_field);
        const owned_path = try alloc.dupe(u8, value.string);
        var path_transferred = false;
        errdefer if (!path_transferred) alloc.free(owned_path);
        const exists_operands = try alloc.alloc(db_mod.types.RelationalRowsExpression, 1);
        var exists_operands_transferred = false;
        errdefer if (!exists_operands_transferred) alloc.free(exists_operands);
        exists_operands[0] = .{ .kind = .field, .field = owned_field, .field_source = source };
        try operands.append(alloc, .{
            .kind = .json_path_exists,
            .json_path = owned_path,
            .operands = exists_operands,
        });
        field_transferred = true;
        path_transferred = true;
        exists_operands_transferred = true;
    }

    if (operands.items.len == 1) {
        const out = operands.items[0];
        operands.clearRetainingCapacity();
        operands.deinit(alloc);
        operands_transferred = true;
        return out;
    }

    const owned_operands = try operands.toOwnedSlice(alloc);
    operands = .empty;
    operands_transferred = true;
    return .{
        .kind = if (match_all) .bool_and else .bool_or,
        .operands = owned_operands,
    };
}
