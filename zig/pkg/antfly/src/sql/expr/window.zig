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
const expr_projection = @import("projection.zig");
const expr_type = @import("type.zig");
const parser = @import("../parser.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;

pub fn functionRequiresOrder(function: db_mod.types.RelationalRowsWindowFunction) bool {
    return switch (function) {
        .count, .sum, .avg, .min, .max, .bool_or, .bool_and => false,
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .lag, .lead, .first_value, .last_value, .nth_value => true,
    };
}

pub fn functionName(function: db_mod.types.RelationalRowsWindowFunction) []const u8 {
    return switch (function) {
        .row_number => "row_number",
        .rank => "rank",
        .dense_rank => "dense_rank",
        .percent_rank => "percent_rank",
        .cume_dist => "cume_dist",
        .ntile => "ntile",
        .lag => "lag",
        .lead => "lead",
        .first_value => "first_value",
        .last_value => "last_value",
        .nth_value => "nth_value",
        .count => "count",
        .sum => "sum",
        .avg => "avg",
        .min => "min",
        .max => "max",
        .bool_or => "bool_or",
        .bool_and => "bool_and",
    };
}

pub fn functionForName(name: []const u8) ?db_mod.types.RelationalRowsWindowFunction {
    if (std.ascii.eqlIgnoreCase(name, "row_number")) return .row_number;
    if (std.ascii.eqlIgnoreCase(name, "rank")) return .rank;
    if (std.ascii.eqlIgnoreCase(name, "dense_rank")) return .dense_rank;
    if (std.ascii.eqlIgnoreCase(name, "percent_rank")) return .percent_rank;
    if (std.ascii.eqlIgnoreCase(name, "cume_dist")) return .cume_dist;
    if (std.ascii.eqlIgnoreCase(name, "ntile")) return .ntile;
    if (std.ascii.eqlIgnoreCase(name, "lag")) return .lag;
    if (std.ascii.eqlIgnoreCase(name, "lead")) return .lead;
    if (std.ascii.eqlIgnoreCase(name, "first_value")) return .first_value;
    if (std.ascii.eqlIgnoreCase(name, "last_value")) return .last_value;
    if (std.ascii.eqlIgnoreCase(name, "nth_value")) return .nth_value;
    if (std.ascii.eqlIgnoreCase(name, "count")) return .count;
    if (std.ascii.eqlIgnoreCase(name, "sum")) return .sum;
    if (std.ascii.eqlIgnoreCase(name, "avg")) return .avg;
    if (std.ascii.eqlIgnoreCase(name, "min")) return .min;
    if (std.ascii.eqlIgnoreCase(name, "max")) return .max;
    if (std.ascii.eqlIgnoreCase(name, "bool_or")) return .bool_or;
    if (std.ascii.eqlIgnoreCase(name, "bool_and")) return .bool_and;
    return null;
}

pub fn functionSupportsFilter(function: db_mod.types.RelationalRowsWindowFunction) bool {
    return switch (function) {
        .count, .sum, .avg, .min, .max, .bool_or, .bool_and => true,
        else => false,
    };
}

pub fn parseFunction(
    tokens: []const Token,
    pos: *usize,
) !db_mod.types.RelationalRowsWindowFunction {
    if (parser.matchKeyword(tokens, pos, "row_number")) return .row_number;
    if (parser.matchKeyword(tokens, pos, "rank")) return .rank;
    if (parser.matchKeyword(tokens, pos, "dense_rank")) return .dense_rank;
    if (parser.matchKeyword(tokens, pos, "percent_rank")) return .percent_rank;
    if (parser.matchKeyword(tokens, pos, "cume_dist")) return .cume_dist;
    if (parser.matchKeyword(tokens, pos, "ntile")) return .ntile;
    if (parser.matchKeyword(tokens, pos, "lag")) return .lag;
    if (parser.matchKeyword(tokens, pos, "lead")) return .lead;
    if (parser.matchKeyword(tokens, pos, "first_value")) return .first_value;
    if (parser.matchKeyword(tokens, pos, "last_value")) return .last_value;
    if (parser.matchKeyword(tokens, pos, "nth_value")) return .nth_value;
    if (parser.matchKeyword(tokens, pos, "count")) return .count;
    if (parser.matchKeyword(tokens, pos, "sum")) return .sum;
    if (parser.matchKeyword(tokens, pos, "avg")) return .avg;
    if (parser.matchKeyword(tokens, pos, "min")) return .min;
    if (parser.matchKeyword(tokens, pos, "max")) return .max;
    if (parser.matchKeyword(tokens, pos, "bool_or")) return .bool_or;
    if (parser.matchKeyword(tokens, pos, "bool_and")) return .bool_and;
    return error.UnsupportedSqlShape;
}

pub fn parseOptionalFrame(
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
) !?db_mod.types.RelationalRowsWindowFrame {
    const unit: db_mod.types.RelationalRowsWindowFrameUnit = if (parser.matchKeyword(tokens, pos, "rows"))
        .rows
    else if (parser.matchKeyword(tokens, pos, "range"))
        .range
    else
        return null;
    const start, const end = if (parser.matchKeyword(tokens, pos, "between")) blk: {
        const parsed_start = try parseFrameBound(tokens, pos, params);
        try parser.expectKeyword(tokens, pos, "and");
        const parsed_end = try parseFrameBound(tokens, pos, params);
        break :blk .{ parsed_start, parsed_end };
    } else blk: {
        const parsed_start = try parseFrameBound(tokens, pos, params);
        break :blk .{ parsed_start, plan_mod.ParsedWindowFrameBound{ .bound = .current_row } };
    };
    const frame = db_mod.types.RelationalRowsWindowFrame{
        .unit = unit,
        .start = start.bound,
        .start_offset = start.offset,
        .end = end.bound,
        .end_offset = end.offset,
    };
    try validateFrame(frame);
    return frame;
}

pub fn parseFrameBound(
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
) !plan_mod.ParsedWindowFrameBound {
    if (parser.matchKeyword(tokens, pos, "unbounded")) {
        if (parser.matchKeyword(tokens, pos, "preceding")) return .{ .bound = .unbounded_preceding };
        if (parser.matchKeyword(tokens, pos, "following")) return .{ .bound = .unbounded_following };
        return error.UnsupportedSqlShape;
    }
    if (parser.matchKeyword(tokens, pos, "current")) {
        try parser.expectKeyword(tokens, pos, "row");
        return .{ .bound = .current_row };
    }
    const offset = try value_mod.parseSqlU32Value(tokens, pos, params);
    if (offset == 0) return error.UnsupportedSqlShape;
    if (parser.matchKeyword(tokens, pos, "preceding")) return .{ .bound = .offset_preceding, .offset = offset };
    if (parser.matchKeyword(tokens, pos, "following")) return .{ .bound = .offset_following, .offset = offset };
    return error.UnsupportedSqlShape;
}

pub fn outputFieldIsUnique(
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
    field: []const u8,
) bool {
    var matches: usize = 0;
    for (fields) |candidate| {
        if (std.mem.eql(u8, candidate, field)) matches += 1;
    }
    for (windows) |window| {
        if (std.mem.eql(u8, window.output, field)) matches += 1;
    }
    return matches == 1;
}

pub fn validateFrame(frame: db_mod.types.RelationalRowsWindowFrame) !void {
    try validateFrameBoundOffset(frame.start, frame.start_offset);
    try validateFrameBoundOffset(frame.end, frame.end_offset);
    if (frame.start == .unbounded_following or frame.end == .unbounded_preceding) return error.UnsupportedSqlShape;
    if (frameBoundOrdinal(frame.start, frame.start_offset) > frameBoundOrdinal(frame.end, frame.end_offset)) return error.UnsupportedSqlShape;
}

pub fn validateFrameForOrder(
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    frame: db_mod.types.RelationalRowsWindowFrame,
    order_by: []const db_mod.types.RelationalRowsQueryOrder,
) !void {
    if (frame.unit != .range) return;
    if (frame.start != .offset_preceding and frame.start != .offset_following and frame.end != .offset_preceding and frame.end != .offset_following) return;
    if (order_by.len == 0) return error.UnsupportedSqlShape;
    const order = order_by[0];
    if (order.null_test != null) return error.UnsupportedSqlShape;
    if (order.expression) |expression| {
        try type_context.validateNumericOrDatetimeRowExpression(expression);
        return;
    }
    if (order.field.len == 0) return error.UnsupportedSqlShape;
    const column = binder.relationalColumnForField(schema, order.field, null) orelse return error.InvalidSqlCatalog;
    if (column.field_type != .numeric and column.field_type != .datetime) return error.UnsupportedSqlShape;
}

pub fn validateFrameBoundOffset(
    bound: db_mod.types.RelationalRowsWindowFrameBound,
    offset: u32,
) !void {
    switch (bound) {
        .offset_preceding, .offset_following => {
            if (offset == 0) return error.UnsupportedSqlShape;
        },
        else => if (offset != 0) return error.UnsupportedSqlShape,
    }
}

pub fn frameBoundOrdinal(bound: db_mod.types.RelationalRowsWindowFrameBound, offset: u32) i64 {
    return switch (bound) {
        .unbounded_preceding => std.math.minInt(i64),
        .offset_preceding => -@as(i64, @intCast(offset)),
        .current_row => 0,
        .offset_following => @as(i64, @intCast(offset)),
        .unbounded_following => std.math.maxInt(i64),
    };
}

pub fn valueExpressionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.value_expression != null) count += 1;
    }
    return count;
}

pub fn defaultCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        if (window.default_json.len > 0) count += 1;
    }
    return count;
}

pub fn filterPredicateCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_predicates.len;
    }
    return count;
}

pub fn filterExpressionCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_expressions.len;
        count += window.filter_expression_array_contains.len;
    }
    return count;
}

pub fn filterAccessCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_array_any.len;
        count += window.filter_array_contains.len;
        count += window.filter_array_eq.len;
        count += window.filter_in_predicates.len;
        count += window.filter_json_contains.len;
        count += window.filter_json_path_eq.len;
        count += window.filter_json_path_exists.len;
        count += window.filter_text_patterns.len;
    }
    return count;
}

pub fn filterGroupCount(windows: []const db_mod.types.RelationalRowsWindowSpec) usize {
    var count: usize = 0;
    for (windows) |window| {
        count += window.filter_any.len;
        count += window.filter_not.len;
    }
    return count;
}

pub fn frameSignature(windows: []const db_mod.types.RelationalRowsWindowSpec) u64 {
    var signature: u64 = 0;
    for (windows) |window| {
        const contribution = windowFrameSignature(window.frame orelse continue);
        signature +%= contribution *% 11400714819323198485;
        signature ^= std.math.rotl(u64, contribution, @as(u6, @intCast(contribution & 63)));
    }
    return signature;
}

pub fn outputFieldByOrdinalAlloc(
    alloc: std.mem.Allocator,
    select: plan_mod.WindowSelectList,
    ordinal: u32,
) ![]const u8 {
    if (ordinal == 0) return error.UnsupportedSqlShape;
    const index: usize = @intCast(ordinal - 1);
    if (index >= select.outputs.len) return error.UnsupportedSqlShape;
    const output = select.outputs[index];
    return switch (output.kind) {
        .field => try alloc.dupe(u8, select.fields[output.index]),
        .window => try alloc.dupe(u8, select.windows[output.index].output),
    };
}

pub fn outputColumnsAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    select: plan_mod.WindowSelectList,
) ![]runtime_schema.RelationalColumn {
    const total = select.fields.len + select.windows.len;
    if (total == 0) return &.{};
    const out = try alloc.alloc(runtime_schema.RelationalColumn, total);
    var initialized: usize = 0;
    errdefer {
        ddl_plan.clearDdlRelationalColumns(alloc, out[0..initialized]);
        alloc.free(out);
    }
    for (select.fields) |field| {
        if (expr_projection.outputColumnExists(out[0..initialized], field)) return error.UnsupportedSqlShape;
        const column = binder.relationalColumnForField(schema, field, null) orelse return error.InvalidSqlCatalog;
        out[initialized] = try expr_projection.projectedSourceColumnAlloc(alloc, field, column);
        initialized += 1;
    }
    for (select.windows) |window| {
        if (expr_projection.outputColumnExists(out[0..initialized], window.output)) return error.UnsupportedSqlShape;
        const value_type = if (window.value_expression) |expression| try type_context.rowExpressionOutputType(expression) else null;
        out[initialized] = try expr_projection.projectedColumnAlloc(alloc, window.output, try expr_type.windowOutputType(window.function, value_type), null, true);
        initialized += 1;
    }
    return out;
}

pub fn validateSelectListOutputs(
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
) !void {
    for (fields, 0..) |field, i| {
        for (fields[i + 1 ..]) |other| {
            if (std.mem.eql(u8, field, other)) return error.UnsupportedSqlShape;
        }
        for (windows) |window| {
            if (std.mem.eql(u8, field, window.output)) return error.UnsupportedSqlShape;
        }
    }
    for (windows, 0..) |window, i| {
        for (windows[i + 1 ..]) |other| {
            if (std.mem.eql(u8, window.output, other.output)) return error.UnsupportedSqlShape;
        }
    }
}

fn windowFrameSignature(frame: db_mod.types.RelationalRowsWindowFrame) u64 {
    var signature: u64 = 17;
    signature = signature *% 131 +% frameUnitCode(frame.unit);
    signature = signature *% 131 +% frameBoundCode(frame.start);
    signature = signature *% 131 +% @as(u64, @intCast(frame.start_offset));
    signature = signature *% 131 +% frameBoundCode(frame.end);
    signature = signature *% 131 +% @as(u64, @intCast(frame.end_offset));
    return signature;
}

fn frameUnitCode(unit: db_mod.types.RelationalRowsWindowFrameUnit) u64 {
    return switch (unit) {
        .rows => 1,
        .range => 2,
    };
}

fn frameBoundCode(bound: db_mod.types.RelationalRowsWindowFrameBound) u64 {
    return switch (bound) {
        .unbounded_preceding => 1,
        .offset_preceding => 2,
        .current_row => 3,
        .offset_following => 4,
        .unbounded_following => 5,
    };
}

test "sql expr_window validates frame helpers" {
    try std.testing.expect(functionRequiresOrder(.lag));
    try std.testing.expect(!functionRequiresOrder(.count));
    try std.testing.expectEqualStrings("row_number", functionName(.row_number));
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.rank, functionForName("RANK").?);
    try std.testing.expect(functionSupportsFilter(.count));
    try std.testing.expect(!functionSupportsFilter(.lag));

    try validateFrame(.{
        .unit = .rows,
        .start = .unbounded_preceding,
        .end = .current_row,
    });
    try std.testing.expectError(error.UnsupportedSqlShape, validateFrame(.{
        .unit = .rows,
        .start = .unbounded_following,
        .end = .current_row,
    }));
    try std.testing.expectError(error.UnsupportedSqlShape, validateFrameBoundOffset(.current_row, 1));
    try std.testing.expect(frameBoundOrdinal(.unbounded_preceding, 0) < frameBoundOrdinal(.current_row, 0));

    const schema = runtime_schema.TableSchema{
        .storage_mode = .relational,
        .relational_columns = &.{
            .{ .name = "amount", .path = "amount", .field_type = .numeric },
            .{ .name = "status", .path = "status", .field_type = .keyword },
        },
    };
    const type_context = expr_type.RowExpressionTypeContext{ .alloc = std.testing.allocator, .schema = schema };
    const range_frame = db_mod.types.RelationalRowsWindowFrame{
        .unit = .range,
        .start = .offset_preceding,
        .start_offset = 1,
        .end = .current_row,
    };
    try validateFrameForOrder(schema, type_context, range_frame, &.{.{ .field = "amount" }});
    try std.testing.expectError(error.UnsupportedSqlShape, validateFrameForOrder(schema, type_context, range_frame, &.{.{ .field = "status" }}));
    try std.testing.expectError(error.UnsupportedSqlShape, validateFrameForOrder(schema, type_context, range_frame, &.{.{ .field = "amount", .null_test = .is_null }}));

    const windows = [_]db_mod.types.RelationalRowsWindowSpec{.{
        .output = "row_num",
        .function = .lag,
        .value_expression = .{ .kind = .field, .field = "amount" },
        .default_json = "0",
        .frame = .{
            .unit = .rows,
            .start = .unbounded_preceding,
            .end = .current_row,
        },
        .filter_predicates = &.{.{ .name = "status_open", .field = "status", .op = .eq, .value_json = "\"open\"" }},
        .filter_expressions = &.{.{ .lhs = .{ .kind = .field, .field = "active" }, .op = .eq, .rhs = &.{.{ .kind = .value, .value_json = "true" }} }},
        .filter_array_any = &.{.{ .field = "tags", .values_json = "[\"new\"]" }},
    }};
    try std.testing.expectEqual(@as(usize, 1), valueExpressionCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), defaultCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), filterPredicateCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), filterExpressionCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), filterAccessCount(&windows));
    try std.testing.expect(frameSignature(&windows) != 0);
}
