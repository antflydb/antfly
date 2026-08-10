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
const db_mod = struct {
    pub const types = @import("../../storage/db/types.zig");
};
const ddl_plan = @import("../ddl_plan.zig");
const expr_aggregate = @import("aggregate.zig");
const expr_projection = @import("projection.zig");
const expr_generated = @import("generated.zig");
const expr_generated_validate = @import("generated_validate.zig");
const expr_order = @import("order.zig");
const expr_row_parse = @import("row_parse.zig");
const expr_where_condition = @import("where_condition.zig");
const expr_token = @import("token.zig");
const expr_type = @import("type.zig");
const generated_parser = @import("../generated_parser.zig");
const generated_read_validate = @import("../generated_read_validate.zig");
const grammar = @import("../grammar.zig");
const parser = @import("../parser.zig");
const strings = @import("../strings.zig");
const plan_mod = @import("../plan.zig");
const runtime_schema = @import("../../storage/schema.zig");
const token_mod = @import("../token.zig");
const value_mod = @import("../value.zig");

pub const Token = token_mod.Token;

const freeExpression = plan_mod.freeExpression;
const freeOrderBy = plan_mod.freeOrderBy;

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

pub fn peekFunction(tokens: []const Token, pos: usize) bool {
    return parser.peekKeyword(tokens, pos, "row_number") or
        parser.peekKeyword(tokens, pos, "rank") or
        parser.peekKeyword(tokens, pos, "dense_rank") or
        parser.peekKeyword(tokens, pos, "percent_rank") or
        parser.peekKeyword(tokens, pos, "cume_dist") or
        parser.peekKeyword(tokens, pos, "ntile") or
        parser.peekKeyword(tokens, pos, "lag") or
        parser.peekKeyword(tokens, pos, "lead") or
        parser.peekKeyword(tokens, pos, "first_value") or
        parser.peekKeyword(tokens, pos, "last_value") or
        parser.peekKeyword(tokens, pos, "nth_value") or
        parser.peekKeyword(tokens, pos, "count") or
        parser.peekKeyword(tokens, pos, "sum") or
        parser.peekKeyword(tokens, pos, "avg") or
        parser.peekKeyword(tokens, pos, "min") or
        parser.peekKeyword(tokens, pos, "max") or
        parser.peekKeyword(tokens, pos, "bool_or") or
        parser.peekKeyword(tokens, pos, "bool_and");
}

pub fn peekOutputOrderExpression(tokens: []const Token, pos: usize) bool {
    return expr_aggregate.peekOutputOrderExpression(tokens, pos);
}

pub fn topLevelClauseStart(tokens: []const Token, pos: usize) ?usize {
    var depth: usize = 0;
    var i = pos;
    while (i < tokens.len) : (i += 1) {
        const token = tokens[i];
        switch (token.kind) {
            .lparen => depth += 1,
            .rparen => if (depth > 0) {
                depth -= 1;
            },
            .identifier => if (depth == 0 and token.matchesKeywordTag(.window)) return i,
            .semicolon => if (depth == 0) return null,
            else => {},
        }
    }
    return null;
}

pub fn topLevelClauseEnd(tokens: []const Token, start: usize) usize {
    return parser.findTopLevelTailIndexToken(tokens, start, expr_token.sqlWindowTailClauseKeywordToken);
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

pub fn generatedFrameUnit(
    unit: generated_parser.GeneratedSqlWindowFrameUnit,
) db_mod.types.RelationalRowsWindowFrameUnit {
    return switch (unit) {
        .rows => .rows,
        .range => .range,
    };
}

pub fn generatedFrameBound(
    bound: generated_parser.GeneratedSqlWindowFrameBound,
) db_mod.types.RelationalRowsWindowFrameBound {
    return switch (bound) {
        .unbounded_preceding => .unbounded_preceding,
        .unbounded_following => .unbounded_following,
        .current_row => .current_row,
        .offset_preceding => .offset_preceding,
        .offset_following => .offset_following,
    };
}

pub fn generatedFrameBoundOffset(
    tokens: []const Token,
    bound: generated_parser.GeneratedSqlWindowFrameBound,
    expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    params: []const value_mod.SqlValue,
) !u32 {
    return switch (bound) {
        .offset_preceding, .offset_following => blk: {
            const range = expression_tokens orelse return error.UnsupportedSqlShape;
            if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
            var cursor = range.start;
            const offset = try value_mod.parseSqlU32Value(tokens, &cursor, params);
            if (cursor != range.end or offset == 0) return error.UnsupportedSqlShape;
            break :blk offset;
        },
        else => {
            if (expression_tokens != null) return error.UnsupportedSqlShape;
            return 0;
        },
    };
}

pub fn validateGeneratedFrameSemantics(
    tokens: []const Token,
    params: []const value_mod.SqlValue,
    unit: generated_parser.GeneratedSqlWindowFrameUnit,
    start_bound: generated_parser.GeneratedSqlWindowFrameBound,
    end_bound: generated_parser.GeneratedSqlWindowFrameBound,
    start_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    end_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    parsed_frame: db_mod.types.RelationalRowsWindowFrame,
) !void {
    const start_offset = try generatedFrameBoundOffset(tokens, start_bound, start_expression_tokens, params);
    const end_offset = try generatedFrameBoundOffset(tokens, end_bound, end_expression_tokens, params);
    if (generatedFrameUnit(unit) != parsed_frame.unit or
        generatedFrameBound(start_bound) != parsed_frame.start or
        start_offset != parsed_frame.start_offset or
        generatedFrameBound(end_bound) != parsed_frame.end or
        end_offset != parsed_frame.end_offset)
        return error.UnsupportedSqlShape;
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

pub const WindowDefinitionParserOptions = struct {
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8 = &.{},
    returning_expression_qualifiers: []const []const u8 = &.{},
    defer_row_expression_field_validation: bool = false,
    type_context: expr_type.RowExpressionTypeContext,
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    order_expression_hooks: expr_order.OrderExpressionParserOptions,
    generated_window_items: []const generated_parser.GeneratedSqlWindowAst = &.{},
    generated_window_ast: ?*const generated_parser.GeneratedSqlWindowAst = null,
    generated_over_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub const WindowSpecParserOptions = struct {
    row_expression_hooks: expr_row_parse.RowExpressionParserHooks,
    arithmetic_hooks: expr_row_parse.ArithmeticExpressionParserHooks,
    variadic_hooks: expr_row_parse.VariadicRowExpressionParserHooks,
    boolean_hooks: expr_row_parse.BooleanRowExpressionParserHooks,
    expression_alternatives: expr_where_condition.ExpressionWhereConditionAlternativesParserOptions,
    expression_conditions: expr_where_condition.ExpressionWhereConditionsParserOptions,
    fixed_binary: expr_row_parse.FixedBinaryRowExpressionParserOptions,
    realtime_ns: u64,
    generated_expression_ast: ?*const generated_parser.GeneratedSqlExpressionAst = null,
};

pub fn validateGeneratedOrderListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0 or list.items.len != list.count or list.expression_items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != list.count or list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != list.count or list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != list.count or list.nulls_order_items.len != list.count or list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;
    if (list.first_tokens == null or !expr_generated.generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !expr_generated.generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start >= item.end or item.start < range.start or item.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (item.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = list.items[index - 1];
            if (previous.end + 1 != item.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == list.count and item.end != range.end) return error.UnsupportedSqlShape;
        if (list.alias_items[index] != null or list.alias_name_items[index] != null) return error.UnsupportedSqlShape;

        const expression_range = list.expression_items[index];
        if (expression_range.start < item.start or expression_range.end > item.end or expression_range.start >= expression_range.end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, list.expressions[index]);

        const direction_end = if (list.direction_items[index]) |direction_range| blk: {
            if (direction_range.start != expression_range.end or direction_range.end > item.end or direction_range.start >= direction_range.end) return error.UnsupportedSqlShape;
            if (tokens[direction_range.start].matchesKeywordTag(.using)) {
                const using_operator = list.order_using_operator_items[index] orelse return error.UnsupportedSqlShape;
                if (list.directions[index] != null) return error.UnsupportedSqlShape;
                if (using_operator.start != direction_range.start + 1 or using_operator.end != direction_range.end) return error.UnsupportedSqlShape;
                if (using_operator.end != using_operator.start + 1) return error.UnsupportedSqlShape;
                switch (tokens[using_operator.start].kind) {
                    .lt, .lte, .gt, .gte => {},
                    else => return error.UnsupportedSqlShape,
                }
            } else {
                if (list.order_using_operator_items[index] != null) return error.UnsupportedSqlShape;
                const direction = list.directions[index] orelse return error.UnsupportedSqlShape;
                if (direction_range.end != direction_range.start + 1) return error.UnsupportedSqlShape;
                switch (direction) {
                    .asc => if (!tokens[direction_range.start].matchesKeywordTag(.asc)) return error.UnsupportedSqlShape,
                    .desc => if (!tokens[direction_range.start].matchesKeywordTag(.desc)) return error.UnsupportedSqlShape,
                }
            }
            break :blk direction_range.end;
        } else blk: {
            if (list.directions[index] != null or list.order_using_operator_items[index] != null) return error.UnsupportedSqlShape;
            break :blk expression_range.end;
        };

        const item_end = if (list.nulls_order_items[index]) |nulls_range| blk: {
            if (nulls_range.start != direction_end or nulls_range.end != nulls_range.start + 2 or nulls_range.end > item.end) return error.UnsupportedSqlShape;
            if (!tokens[nulls_range.start].matchesKeywordTag(.nulls)) return error.UnsupportedSqlShape;
            const nulls_order = list.nulls_orders[index] orelse return error.UnsupportedSqlShape;
            switch (nulls_order) {
                .first => if (!tokens[nulls_range.start + 1].matchesKeywordTag(.first)) return error.UnsupportedSqlShape,
                .last => if (!tokens[nulls_range.start + 1].matchesKeywordTag(.last)) return error.UnsupportedSqlShape,
            }
            break :blk nulls_range.end;
        } else blk: {
            if (list.nulls_orders[index] != null) return error.UnsupportedSqlShape;
            break :blk direction_end;
        };
        if (item_end != item.end) return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedExpressionListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    list: generated_parser.GeneratedSqlListAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len) return error.UnsupportedSqlShape;
    if (list.count == 0 or list.items.len != list.count or list.expression_items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    if (list.alias_items.len != list.count or list.alias_name_items.len != list.count) return error.UnsupportedSqlShape;
    if (list.direction_items.len != list.count or list.directions.len != list.count) return error.UnsupportedSqlShape;
    if (list.order_using_operator_items.len != list.count or list.nulls_order_items.len != list.count or list.nulls_orders.len != list.count) return error.UnsupportedSqlShape;
    if (list.first_tokens == null or !expr_generated.generatedTokenRangeEqual(list.first_tokens.?, list.items[0])) return error.UnsupportedSqlShape;
    if (list.last_tokens == null or !expr_generated.generatedTokenRangeEqual(list.last_tokens.?, list.items[list.count - 1])) return error.UnsupportedSqlShape;

    for (list.items, 0..) |item, index| {
        if (item.start >= item.end or item.start < range.start or item.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (item.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = list.items[index - 1];
            if (previous.end + 1 != item.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == list.count and item.end != range.end) return error.UnsupportedSqlShape;
        if (list.alias_items[index] != null or list.alias_name_items[index] != null) return error.UnsupportedSqlShape;
        if (list.direction_items[index] != null or list.directions[index] != null) return error.UnsupportedSqlShape;
        if (list.order_using_operator_items[index] != null or list.nulls_order_items[index] != null or list.nulls_orders[index] != null) return error.UnsupportedSqlShape;

        const expression_range = list.expression_items[index];
        if (!expr_generated.generatedTokenRangeEqual(expression_range, item)) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(list.expressions[index].tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
    }
}

fn validateGeneratedChildExpressionPayloads(
    tokens: []const Token,
    expected_range: generated_parser.GeneratedSqlTokenRange,
    expected_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) anyerror!void {
    const child = expression orelse return error.UnsupportedSqlShape;
    const child_tokens = child.tokens orelse return error.UnsupportedSqlShape;
    if (!expr_generated.generatedTokenRangeEqual(child_tokens, expected_range)) return error.UnsupportedSqlShape;
    if (expected_kind) |kind| {
        if (child.kind != kind) return error.UnsupportedSqlShape;
    }
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, child.*);
}

fn validateGeneratedEmptyList(list: generated_parser.GeneratedSqlListAst) !void {
    if (list.count != 0 or
        list.first_tokens != null or
        list.last_tokens != null or
        list.items.len != 0 or
        list.expression_items.len != 0 or
        list.alias_items.len != 0 or
        list.alias_name_items.len != 0 or
        list.direction_items.len != 0 or
        list.directions.len != 0 or
        list.order_using_operator_items.len != 0 or
        list.nulls_order_items.len != 0 or
        list.nulls_orders.len != 0 or
        list.expressions.len != 0)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedWindowListForClause(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    windows: []const generated_parser.GeneratedSqlWindowAst,
) !void {
    if (range.start >= range.end or range.end > tokens.len or windows.len == 0) return error.UnsupportedSqlShape;
    for (windows, 0..) |window, index| {
        if (window.tokens.start >= window.tokens.end or window.tokens.start < range.start or window.tokens.end > range.end) return error.UnsupportedSqlShape;
        if (index == 0) {
            if (window.tokens.start != range.start) return error.UnsupportedSqlShape;
        } else {
            const previous = windows[index - 1].tokens;
            if (previous.end + 1 != window.tokens.start or previous.end >= tokens.len or tokens[previous.end].kind != .comma) return error.UnsupportedSqlShape;
        }
        if (index + 1 == windows.len and window.tokens.end != range.end) return error.UnsupportedSqlShape;
        if (window.name_tokens.start != window.tokens.start or window.name_tokens.start >= window.name_tokens.end or window.name_tokens.end + 2 > window.tokens.end) return error.UnsupportedSqlShape;
        if (!tokens[window.name_tokens.end].matchesKeywordTag(.as) or tokens[window.name_tokens.end + 1].kind != .lparen or tokens[window.tokens.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
        if (window.definition_tokens.start != window.name_tokens.end + 2 or window.definition_tokens.end != window.tokens.end - 1) return error.UnsupportedSqlShape;

        if (window.partition_tokens) |partition_range| {
            if (partition_range.start < window.definition_tokens.start or partition_range.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedExpressionListForClause(tokens, partition_range, window.partition_items);
        } else {
            try validateGeneratedEmptyList(window.partition_items);
        }

        if (window.order_tokens) |order_range| {
            if (order_range.start < window.definition_tokens.start or order_range.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedOrderListForClause(tokens, order_range, window.order_items);
        } else {
            try validateGeneratedEmptyList(window.order_items);
        }

        if (window.frame_tokens) |frame_range| {
            if (window.frame_unit == null or window.frame_start_bound == null or window.frame_end_bound == null) return error.UnsupportedSqlShape;
            if (frame_range.start < window.definition_tokens.start or frame_range.end > window.definition_tokens.end) return error.UnsupportedSqlShape;
            try validateGeneratedWindowFrameClauseLayout(
                tokens,
                frame_range,
                window.frame_start_expression_kind,
                window.frame_start_expression_tokens,
                window.frame_start_expression,
                window.frame_end_expression_kind,
                window.frame_end_expression_tokens,
                window.frame_end_expression,
            );
            if (window.frame_start_expression_tokens) |expression_range| {
                if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual((window.frame_start_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
            } else if (window.frame_start_expression != null or window.frame_start_expression_kind != null) {
                return error.UnsupportedSqlShape;
            }
            if (window.frame_end_expression_tokens) |expression_range| {
                if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
                if (!expr_generated.generatedTokenRangeEqual((window.frame_end_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
            } else if (window.frame_end_expression != null or window.frame_end_expression_kind != null) {
                return error.UnsupportedSqlShape;
            }
        } else if (window.frame_unit != null or window.frame_start_bound != null or window.frame_end_bound != null or
            window.frame_start_expression_tokens != null or window.frame_start_expression != null or window.frame_start_expression_kind != null or
            window.frame_end_expression_tokens != null or window.frame_end_expression != null or window.frame_end_expression_kind != null)
        {
            return error.UnsupportedSqlShape;
        }
    }
}

fn generatedWindowClauseEnd(
    tokens: []const Token,
    keyword_index: usize,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?usize {
    const read = generated_read_ast orelse return null;
    if (keyword_index >= tokens.len or !tokens[keyword_index].matchesKeywordTag(.window)) return null;
    const range = read.window_tokens orelse return error.UnsupportedSqlShape;
    if (range.start != pos or range.end > tokens.len or read.window_items.len != read.window_count) return error.UnsupportedSqlShape;
    try validateGeneratedWindowListForClause(tokens, range, read.window_items);
    return range.end;
}

const GeneratedExpressionItem = struct {
    tokens: generated_parser.GeneratedSqlTokenRange,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
};

fn generatedExpressionListItemAtStart(
    tokens: []const Token,
    pos: usize,
    items: *const generated_parser.GeneratedSqlListAst,
) !?GeneratedExpressionItem {
    if (items.items.len != items.count or items.expressions.len != items.count) return error.UnsupportedSqlShape;
    for (items.items, 0..) |item, index| {
        if (item.start == pos) {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &items.expressions[index],
            };
        }
    }
    return null;
}

fn generatedProjectionItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.projection_items.items.len != read.projection_items.count or read.projection_items.expressions.len != read.projection_items.count) return error.UnsupportedSqlShape;
    for (read.projection_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, read.projection_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.projection_items.expressions[index],
            };
        }
    }
    if (read.set_operation_tokens != null) {
        if (read.set_operation.right_projection_items.items.len != read.set_operation.right_projection_items.count or
            read.set_operation.right_projection_items.expressions.len != read.set_operation.right_projection_items.count)
        {
            return error.UnsupportedSqlShape;
        }
        for (read.set_operation.right_projection_items.items, 0..) |item, index| {
            if (item.start == pos) {
                try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, read.set_operation.right_projection_items.expressions[index]);
                return .{
                    .tokens = item,
                    .expression = &read.set_operation.right_projection_items.expressions[index],
                };
            }
        }
    }
    return null;
}

fn generatedProjectionExpressionAtItemStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const item = try generatedProjectionItemAtStart(tokens, pos, generated_read_ast);
    return if (item) |generated_item| generated_item.expression else null;
}

fn generatedGroupItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.group_items.items.len != read.group_items.count or read.group_items.expressions.len != read.group_items.count) return error.UnsupportedSqlShape;
    for (read.group_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, read.group_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.group_items.expressions[index],
            };
        }
    }
    return null;
}

fn generatedGroupExpressionAtItemStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const item = try generatedGroupItemAtStart(tokens, pos, generated_read_ast);
    return if (item) |generated_item| generated_item.expression else null;
}

fn generatedOrderItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?GeneratedExpressionItem {
    const read = generated_read_ast orelse return null;
    if (read.order_items.items.len != read.order_items.count or read.order_items.expressions.len != read.order_items.count) return error.UnsupportedSqlShape;
    for (read.order_items.items, 0..) |item, index| {
        if (item.start == pos) {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, read.order_items.expressions[index]);
            return .{
                .tokens = item,
                .expression = &read.order_items.expressions[index],
            };
        }
    }
    return null;
}

fn generatedOrderExpressionAtItemStart(
    tokens: []const Token,
    pos: usize,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) !?*const generated_parser.GeneratedSqlExpressionAst {
    const item = try generatedOrderItemAtStart(tokens, pos, generated_read_ast);
    return if (item) |generated_item| generated_item.expression else null;
}

fn generatedReturningItemAtStart(
    tokens: []const Token,
    pos: usize,
    generated_returning_items: ?*const generated_parser.GeneratedSqlListAst,
) !?GeneratedExpressionItem {
    const list = generated_returning_items orelse return null;
    if (list.count == 0 or list.items.len != list.count or list.expressions.len != list.count) return error.UnsupportedSqlShape;
    const first = list.first_tokens orelse return error.UnsupportedSqlShape;
    const last = list.last_tokens orelse return error.UnsupportedSqlShape;
    try expr_generated_validate.validateGeneratedProjectionListForClause(tokens, .{ .start = first.start, .end = last.end }, list.*);
    for (list.items, 0..) |item, index| {
        if (item.start == pos) {
            try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, list.expressions[index]);
            return .{
                .tokens = item,
                .expression = &list.expressions[index],
            };
        }
    }
    return error.UnsupportedSqlShape;
}

fn validateGeneratedExpressionItemEnd(generated_item: ?GeneratedExpressionItem, pos: usize) !void {
    if (generated_item) |item| {
        if (pos != item.tokens.end) return error.UnsupportedSqlShape;
    }
}

fn generatedSelectItemStartAllowsExpressionKind(
    start: expr_projection.SelectItemStart,
    kind: generated_parser.GeneratedSqlExpressionKind,
) bool {
    return switch (start) {
        .pipe_concat => kind == .string_concat,
        .unary_negative => kind == .unary_negative,
        .boolean_not => kind == .logical_not,
        .extension_function,
        .routine_expression,
        .uuid_v4,
        .json_extract_path,
        .json_typeof,
        .json_array_length,
        .json_build_object,
        .convert_from,
        .to_jsonb,
        .array_length,
        .array_position,
        .array_element_transform,
        .array_to_string,
        .string_to_array,
        .coalesce,
        .case_fold,
        .replace,
        .regexp_replace,
        .regexp_substr,
        .regexp_match,
        .regexp_count,
        .regexp_instr,
        .translate,
        .concat,
        .nullif,
        .text_length,
        .ascii,
        .chr,
        .substring,
        .overlay,
        .split_part,
        .strpos,
        .left_right,
        .pad,
        .repeat,
        .reverse,
        .md5,
        .soundex,
        .starts_with,
        .ends_with,
        .date_trunc,
        .date_bin,
        .abs,
        .round,
        .trunc,
        .floor,
        .ceil,
        .sqrt,
        .sign,
        .mod,
        .power,
        .greatest_least,
        => kind == .function_call,
        .date_part => kind == .function_call or kind == .extract_expression,
        .now => kind == .function_call or kind == .current_timestamp,
        .current_date => kind == .current_date,
        .typed_datetime_literal => kind == .timestamp_literal,
        .case => kind == .case_expression,
        .cast => kind == .cast,
        .parenthesized => kind == .grouped,
    };
}

fn generatedExpressionFunctionNameToken(
    tokens: []const Token,
    expression: generated_parser.GeneratedSqlExpressionAst,
) !Token {
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.end != name_tokens.start + 1 or name_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    return tokens[name_tokens.start];
}

fn windowFunctionForGeneratedKind(
    kind: generated_parser.GeneratedSqlWindowFunctionKind,
) db_mod.types.RelationalRowsWindowFunction {
    return switch (kind) {
        .row_number => .row_number,
        .rank => .rank,
        .dense_rank => .dense_rank,
        .percent_rank => .percent_rank,
        .cume_dist => .cume_dist,
        .ntile => .ntile,
        .lag => .lag,
        .lead => .lead,
        .first_value => .first_value,
        .last_value => .last_value,
        .nth_value => .nth_value,
        .count => .count,
        .sum => .sum,
        .avg => .avg,
        .min => .min,
        .max => .max,
        .bool_or => .bool_or,
        .bool_and => .bool_and,
    };
}

fn generatedWindowFunctionForExpression(
    tokens: []const Token,
    expression: *const generated_parser.GeneratedSqlExpressionAst,
) !?db_mod.types.RelationalRowsWindowFunction {
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .function_call) return null;
    const function = windowFunctionForGeneratedKind(expression.window_function_kind orelse return null);
    if (expression.over_tokens == null) return null;
    return function;
}

fn parseIdentifierOwnedAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const token = parser.matchToken(tokens, pos, .identifier) orelse return error.UnsupportedSqlShape;
    return try alloc.dupe(u8, token.text);
}

fn validateGeneratedSimpleGroupExpression(
    tokens: []const Token,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .token_range) return error.UnsupportedSqlShape;
}

fn validateGeneratedWindowOverClauseForSpec(
    tokens: []const Token,
    function: ?db_mod.types.RelationalRowsWindowFunction,
    over_start: usize,
    over_end: usize,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, expression.*);
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    if (function) |expected_function| {
        const generated_function = windowFunctionForGeneratedKind(expression.window_function_kind orelse return error.UnsupportedSqlShape);
        if (generated_function != expected_function) return error.UnsupportedSqlShape;
    }
    const over_range = expression.over_tokens orelse return error.UnsupportedSqlShape;
    if (over_range.start != over_start or over_range.end != over_end or over_range.end > tokens.len) return error.UnsupportedSqlShape;
    if (over_range.start >= over_range.end or !tokens[over_range.start].matchesKeywordTag(.over)) return error.UnsupportedSqlShape;
    if (expression.over_name_tokens) |name_range| {
        if (expression.over_definition_tokens != null or
            expression.over_partition_tokens != null or
            expression.over_order_tokens != null or
            expression.over_frame_tokens != null or
            expression.over_frame_unit != null or
            expression.over_frame_start_bound != null or
            expression.over_frame_end_bound != null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedEmptyList(expression.over_partition_items);
        try validateGeneratedEmptyList(expression.over_order_items);
        if (name_range.start != over_range.start + 1 or name_range.end != over_range.end or name_range.start >= name_range.end) return error.UnsupportedSqlShape;
        return;
    }
    if (over_range.start + 2 > over_range.end or tokens[over_range.start + 1].kind != .lparen or tokens[over_range.end - 1].kind != .rparen) return error.UnsupportedSqlShape;
    const definition_range = expression.over_definition_tokens orelse {
        if (over_range.end != over_range.start + 3 or
            expression.over_partition_tokens != null or
            expression.over_order_tokens != null or
            expression.over_frame_tokens != null or
            expression.over_frame_unit != null or
            expression.over_frame_start_bound != null or
            expression.over_frame_end_bound != null)
        {
            return error.UnsupportedSqlShape;
        }
        try validateGeneratedEmptyList(expression.over_partition_items);
        try validateGeneratedEmptyList(expression.over_order_items);
        return;
    };
    if (definition_range.start != over_range.start + 2 or definition_range.end != over_range.end - 1) return error.UnsupportedSqlShape;

    if (expression.over_partition_tokens) |partition_range| {
        if (partition_range.start < definition_range.start or partition_range.end > definition_range.end) return error.UnsupportedSqlShape;
        try validateGeneratedExpressionListForClause(tokens, partition_range, expression.over_partition_items);
    } else {
        try validateGeneratedEmptyList(expression.over_partition_items);
    }

    if (expression.over_order_tokens) |order_range| {
        if (order_range.start < definition_range.start or order_range.end > definition_range.end) return error.UnsupportedSqlShape;
        try validateGeneratedOrderListForClause(tokens, order_range, expression.over_order_items);
    } else {
        try validateGeneratedEmptyList(expression.over_order_items);
    }

    if (expression.over_frame_tokens) |frame_range| {
        if (expression.over_frame_unit == null or expression.over_frame_start_bound == null or expression.over_frame_end_bound == null) return error.UnsupportedSqlShape;
        if (frame_range.start < definition_range.start or frame_range.end > definition_range.end) return error.UnsupportedSqlShape;
        try validateGeneratedWindowFrameClauseLayout(
            tokens,
            frame_range,
            expression.over_frame_start_expression_kind,
            expression.over_frame_start_expression_tokens,
            expression.over_frame_start_expression,
            expression.over_frame_end_expression_kind,
            expression.over_frame_end_expression_tokens,
            expression.over_frame_end_expression,
        );
        if (expression.over_frame_start_expression_tokens) |expression_range| {
            if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual((expression.over_frame_start_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        } else if (expression.over_frame_start_expression != null or expression.over_frame_start_expression_kind != null) {
            return error.UnsupportedSqlShape;
        }
        if (expression.over_frame_end_expression_tokens) |expression_range| {
            if (expression_range.start < frame_range.start or expression_range.end > frame_range.end) return error.UnsupportedSqlShape;
            if (!expr_generated.generatedTokenRangeEqual((expression.over_frame_end_expression orelse return error.UnsupportedSqlShape).tokens orelse return error.UnsupportedSqlShape, expression_range)) return error.UnsupportedSqlShape;
        } else if (expression.over_frame_end_expression != null or expression.over_frame_end_expression_kind != null) {
            return error.UnsupportedSqlShape;
        }
    } else if (expression.over_frame_unit != null or expression.over_frame_start_bound != null or expression.over_frame_end_bound != null or
        expression.over_frame_start_expression_tokens != null or expression.over_frame_start_expression != null or expression.over_frame_start_expression_kind != null or
        expression.over_frame_end_expression_tokens != null or expression.over_frame_end_expression != null or expression.over_frame_end_expression_kind != null)
    {
        return error.UnsupportedSqlShape;
    }
}

fn validateGeneratedWindowFrameClauseLayout(
    tokens: []const Token,
    frame_range: generated_parser.GeneratedSqlTokenRange,
    start_expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    start_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    start_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
    end_expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    end_expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    end_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    if (frame_range.start + 1 >= frame_range.end or frame_range.end > tokens.len) return error.UnsupportedSqlShape;
    if (!tokens[frame_range.start].matchesKeywordTag(.rows) and !tokens[frame_range.start].matchesKeywordTag(.range)) {
        return error.UnsupportedSqlShape;
    }

    var cursor = frame_range.start + 1;
    if (tokens[cursor].matchesKeywordTag(.between)) {
        cursor = try validateGeneratedWindowFrameBoundLayout(
            tokens,
            cursor + 1,
            frame_range.end,
            start_expression_kind,
            start_expression_tokens,
            start_expression,
        );
        if (cursor >= frame_range.end or !tokens[cursor].matchesKeywordTag(.@"and")) return error.UnsupportedSqlShape;
        cursor = try validateGeneratedWindowFrameBoundLayout(
            tokens,
            cursor + 1,
            frame_range.end,
            end_expression_kind,
            end_expression_tokens,
            end_expression,
        );
    } else {
        cursor = try validateGeneratedWindowFrameBoundLayout(
            tokens,
            cursor,
            frame_range.end,
            start_expression_kind,
            start_expression_tokens,
            start_expression,
        );
        if (end_expression_kind != null or end_expression_tokens != null or end_expression != null) return error.UnsupportedSqlShape;
    }
    if (cursor != frame_range.end) return error.UnsupportedSqlShape;
}

fn validateGeneratedWindowFrameBoundLayout(
    tokens: []const Token,
    start: usize,
    end: usize,
    expression_kind: ?generated_parser.GeneratedSqlExpressionKind,
    expression_tokens: ?generated_parser.GeneratedSqlTokenRange,
    expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !usize {
    if (start >= end or end > tokens.len) return error.UnsupportedSqlShape;

    if (tokens[start].matchesKeywordTag(.unbounded)) {
        if (expression_kind != null or expression_tokens != null or expression != null) return error.UnsupportedSqlShape;
        if (start + 1 >= end) return error.UnsupportedSqlShape;
        if (!tokens[start + 1].matchesKeywordTag(.preceding) and !tokens[start + 1].matchesKeywordTag(.following)) {
            return error.UnsupportedSqlShape;
        }
        return start + 2;
    }
    if (tokens[start].matchesKeywordTag(.current)) {
        if (expression_kind != null or expression_tokens != null or expression != null) return error.UnsupportedSqlShape;
        if (start + 1 >= end or !tokens[start + 1].matchesKeywordTag(.row)) return error.UnsupportedSqlShape;
        return start + 2;
    }

    const value_tokens = expression_tokens orelse return error.UnsupportedSqlShape;
    if (value_tokens.start != start or value_tokens.start >= value_tokens.end or value_tokens.end >= end) return error.UnsupportedSqlShape;
    const child = expression orelse return error.UnsupportedSqlShape;
    if (expression_kind) |expected_kind| {
        if (child.kind != expected_kind) return error.UnsupportedSqlShape;
    } else if (child.kind != .token_range) {
        return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(child.tokens orelse return error.UnsupportedSqlShape, value_tokens)) return error.UnsupportedSqlShape;
    try validateGeneratedChildExpressionPayloads(tokens, value_tokens, expression_kind, expression);
    if (!tokens[value_tokens.end].matchesKeywordTag(.preceding) and !tokens[value_tokens.end].matchesKeywordTag(.following)) {
        return error.UnsupportedSqlShape;
    }
    return value_tokens.end + 1;
}

fn generatedWindowFunctionArgumentCountBounds(function: db_mod.types.RelationalRowsWindowFunction) struct { min: usize, max: usize } {
    return switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist => .{ .min = 0, .max = 0 },
        .ntile, .first_value, .last_value, .sum, .avg, .min, .max, .bool_or, .bool_and => .{ .min = 1, .max = 1 },
        .nth_value => .{ .min = 2, .max = 2 },
        .lag, .lead => .{ .min = 1, .max = 3 },
        .count => .{ .min = 0, .max = 1 },
    };
}

fn validateGeneratedWindowFunctionArgumentPayloads(
    tokens: []const Token,
    function: db_mod.types.RelationalRowsWindowFunction,
    function_start: usize,
    argument_start: usize,
    argument_end: usize,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
    if (expression_tokens.start != function_start or expression_tokens.end > tokens.len) return error.UnsupportedSqlShape;
    const name_tokens = expression.function_name_tokens orelse return error.UnsupportedSqlShape;
    if (name_tokens.start != function_start or name_tokens.end >= expression_tokens.end) return error.UnsupportedSqlShape;
    const generated_function = windowFunctionForGeneratedKind(expression.window_function_kind orelse return error.UnsupportedSqlShape);
    if (generated_function != function) return error.UnsupportedSqlShape;
    if (expression.argument_distinct_tokens != null or
        expression.argument_order_tokens != null or
        expression.within_group_tokens != null or
        expression.within_group_order_tokens != null)
    {
        return error.UnsupportedSqlShape;
    }
    try validateGeneratedEmptyList(expression.argument_order_items);
    try validateGeneratedEmptyList(expression.within_group_order_items);

    const bounds = generatedWindowFunctionArgumentCountBounds(function);
    if (argument_start == argument_end) {
        if (bounds.min != 0) return error.UnsupportedSqlShape;
        if (expression.argument_tokens != null or expression.argument_value_tokens != null) return error.UnsupportedSqlShape;
        try validateGeneratedEmptyList(expression.argument_items);
        return;
    }

    const argument_tokens = expression.argument_tokens orelse return error.UnsupportedSqlShape;
    const value_tokens = expression.argument_value_tokens orelse return error.UnsupportedSqlShape;
    if (argument_tokens.start != argument_start or argument_tokens.end != argument_end) return error.UnsupportedSqlShape;
    if (value_tokens.start != argument_start or value_tokens.end != argument_end) return error.UnsupportedSqlShape;

    if (function == .count and argument_start + 1 == argument_end and tokens[argument_start].kind == .star) {
        if (expression.argument_items.count > 1) return error.UnsupportedSqlShape;
        if (expression.argument_items.count == 0) try validateGeneratedEmptyList(expression.argument_items);
        if (expression.argument_items.count == 1) try validateGeneratedCountStarArgumentList(tokens, expression.argument_items, value_tokens);
        return;
    }

    if (expression.argument_items.count < bounds.min or expression.argument_items.count > bounds.max) return error.UnsupportedSqlShape;
    if (expression.argument_items.count == 0) return error.UnsupportedSqlShape;
    if (expression.argument_items.items.len != expression.argument_items.count or expression.argument_items.expressions.len != expression.argument_items.count) return error.UnsupportedSqlShape;
    try validateGeneratedExpressionListForClause(tokens, value_tokens, expression.argument_items);
}

fn validateGeneratedCountStarArgumentList(
    tokens: []const Token,
    list: generated_parser.GeneratedSqlListAst,
    star_tokens: generated_parser.GeneratedSqlTokenRange,
) !void {
    if (star_tokens.end != star_tokens.start + 1 or star_tokens.end > tokens.len or tokens[star_tokens.start].kind != .star) return error.UnsupportedSqlShape;
    if (list.count != 1 or
        list.items.len != 1 or
        list.expression_items.len != 1 or
        list.expressions.len != 1 or
        list.alias_items.len != 1 or
        list.alias_name_items.len != 1 or
        list.direction_items.len != 1 or
        list.directions.len != 1 or
        list.order_using_operator_items.len != 1 or
        list.nulls_order_items.len != 1 or
        list.nulls_orders.len != 1)
    {
        return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(list.items[0], star_tokens) or
        !expr_generated.generatedTokenRangeEqual(list.expression_items[0], star_tokens) or
        !expr_generated.generatedTokenRangeEqual(list.first_tokens orelse return error.UnsupportedSqlShape, star_tokens) or
        !expr_generated.generatedTokenRangeEqual(list.last_tokens orelse return error.UnsupportedSqlShape, star_tokens) or
        list.alias_items[0] != null or
        list.alias_name_items[0] != null or
        list.direction_items[0] != null or
        list.directions[0] != null or
        list.order_using_operator_items[0] != null or
        list.nulls_order_items[0] != null or
        list.nulls_orders[0] != null)
    {
        return error.UnsupportedSqlShape;
    }
    if (!expr_generated.generatedTokenRangeEqual(list.expressions[0].tokens orelse return error.UnsupportedSqlShape, star_tokens)) return error.UnsupportedSqlShape;
    try expr_generated_validate.validateGeneratedExpressionPayloads(tokens, list.expressions[0]);
}

fn validateGeneratedWindowFunctionFilterPayloads(
    filter_start: ?usize,
    filter_predicate_start: ?usize,
    filter_predicate_end: ?usize,
    filter_end: ?usize,
    generated_expression: ?*const generated_parser.GeneratedSqlExpressionAst,
) !void {
    const expression = generated_expression orelse return;
    if (expression.kind != .function_call) return error.UnsupportedSqlShape;
    if (filter_start) |start| {
        const end = filter_end orelse return error.UnsupportedSqlShape;
        const predicate_start = filter_predicate_start orelse return error.UnsupportedSqlShape;
        const predicate_end = filter_predicate_end orelse return error.UnsupportedSqlShape;
        if (expression.filter_tokens == null or
            expression.filter_predicate_tokens == null or
            expression.filter_expression == null)
        {
            return error.UnsupportedSqlShape;
        }
        if (expression.filter_tokens.?.start != start or expression.filter_tokens.?.end != end) return error.UnsupportedSqlShape;
        if (expression.filter_predicate_tokens.?.start != predicate_start or expression.filter_predicate_tokens.?.end != predicate_end) return error.UnsupportedSqlShape;
        if (!expr_generated.generatedTokenRangeEqual(expression.filter_expression.?.tokens orelse return error.UnsupportedSqlShape, expression.filter_predicate_tokens.?)) return error.UnsupportedSqlShape;
    } else {
        if (filter_end != null or filter_predicate_start != null or filter_predicate_end != null) return error.UnsupportedSqlShape;
        if (expression.filter_tokens != null or
            expression.filter_predicate_tokens != null or
            expression.filter_expression_kind != null or
            expression.filter_expression != null)
        {
            return error.UnsupportedSqlShape;
        }
    }
}

fn generatedWindowDefinitionPartitionTokens(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlTokenRange {
    if (options.generated_window_ast) |window| return window.partition_tokens;
    if (options.generated_over_expression_ast) |expression| return expression.over_partition_tokens;
    return null;
}

fn generatedWindowDefinitionPartitionItems(options: WindowDefinitionParserOptions) ?*const generated_parser.GeneratedSqlListAst {
    if (options.generated_window_ast) |window| return &window.partition_items;
    if (options.generated_over_expression_ast) |expression| return &expression.over_partition_items;
    return null;
}

fn generatedWindowDefinitionOrderTokens(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlTokenRange {
    if (options.generated_window_ast) |window| return window.order_tokens;
    if (options.generated_over_expression_ast) |expression| return expression.over_order_tokens;
    return null;
}

fn generatedWindowDefinitionOrderItems(options: WindowDefinitionParserOptions) ?*const generated_parser.GeneratedSqlListAst {
    if (options.generated_window_ast) |window| return &window.order_items;
    if (options.generated_over_expression_ast) |expression| return &expression.over_order_items;
    return null;
}

fn generatedWindowDefinitionFrameTokens(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlTokenRange {
    if (options.generated_window_ast) |window| return window.frame_tokens;
    if (options.generated_over_expression_ast) |expression| return expression.over_frame_tokens;
    return null;
}

fn generatedWindowDefinitionFrameUnit(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlWindowFrameUnit {
    if (options.generated_window_ast) |window| return window.frame_unit;
    if (options.generated_over_expression_ast) |expression| return expression.over_frame_unit;
    return null;
}

fn generatedWindowDefinitionFrameStartBound(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlWindowFrameBound {
    if (options.generated_window_ast) |window| return window.frame_start_bound;
    if (options.generated_over_expression_ast) |expression| return expression.over_frame_start_bound;
    return null;
}

fn generatedWindowDefinitionFrameEndBound(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlWindowFrameBound {
    if (options.generated_window_ast) |window| return window.frame_end_bound;
    if (options.generated_over_expression_ast) |expression| return expression.over_frame_end_bound;
    return null;
}

fn generatedWindowDefinitionFrameStartExpressionTokens(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlTokenRange {
    if (options.generated_window_ast) |window| return window.frame_start_expression_tokens;
    if (options.generated_over_expression_ast) |expression| return expression.over_frame_start_expression_tokens;
    return null;
}

fn generatedWindowDefinitionFrameEndExpressionTokens(options: WindowDefinitionParserOptions) ?generated_parser.GeneratedSqlTokenRange {
    if (options.generated_window_ast) |window| return window.frame_end_expression_tokens;
    if (options.generated_over_expression_ast) |expression| return expression.over_frame_end_expression_tokens;
    return null;
}

fn validateGeneratedWindowDefinitionFrameLayout(
    tokens: []const Token,
    range: generated_parser.GeneratedSqlTokenRange,
    options: WindowDefinitionParserOptions,
) !void {
    if (options.generated_window_ast) |window| {
        return try validateGeneratedWindowFrameClauseLayout(
            tokens,
            range,
            window.frame_start_expression_kind,
            window.frame_start_expression_tokens,
            window.frame_start_expression,
            window.frame_end_expression_kind,
            window.frame_end_expression_tokens,
            window.frame_end_expression,
        );
    }
    const expression = options.generated_over_expression_ast orelse return error.UnsupportedSqlShape;
    return try validateGeneratedWindowFrameClauseLayout(
        tokens,
        range,
        expression.over_frame_start_expression_kind,
        expression.over_frame_start_expression_tokens,
        expression.over_frame_start_expression,
        expression.over_frame_end_expression_kind,
        expression.over_frame_end_expression_tokens,
        expression.over_frame_end_expression,
    );
}

fn validateGeneratedWindowDefinitionFrameSemantics(
    tokens: []const Token,
    params: []const value_mod.SqlValue,
    options: WindowDefinitionParserOptions,
    parsed_frame: db_mod.types.RelationalRowsWindowFrame,
) !void {
    const unit = generatedWindowDefinitionFrameUnit(options) orelse return error.UnsupportedSqlShape;
    const start_bound = generatedWindowDefinitionFrameStartBound(options) orelse return error.UnsupportedSqlShape;
    const end_bound = generatedWindowDefinitionFrameEndBound(options) orelse return error.UnsupportedSqlShape;
    try validateGeneratedFrameSemantics(
        tokens,
        params,
        unit,
        start_bound,
        end_bound,
        generatedWindowDefinitionFrameStartExpressionTokens(options),
        generatedWindowDefinitionFrameEndExpressionTokens(options),
        parsed_frame,
    );
}

fn parseWindowPartitionByAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    partition_by: *std.ArrayListUnmanaged([]const u8),
    options: WindowDefinitionParserOptions,
) !void {
    while (true) {
        const item_start = pos.*;
        const generated_items = generatedWindowDefinitionPartitionItems(options);
        const generated_item = if (generated_items) |items|
            try generatedExpressionListItemAtStart(tokens, item_start, items)
        else
            null;
        if (generated_item == null and generated_items != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        try validateGeneratedSimpleGroupExpression(tokens, generated_expression);
        const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
            alloc,
            tokens,
            pos,
            options.schema,
            options.field_expression_qualifiers,
            options.returning_expression_qualifiers,
            options.defer_row_expression_field_validation,
        );
        defer alloc.free(parsed_field);
        const field = try binder.normalizeRowExpressionFieldAlloc(alloc, options.schema, parsed_field, options.field_expression_qualifiers, options.returning_expression_qualifiers, options.defer_row_expression_field_validation);
        var field_transferred = false;
        errdefer if (!field_transferred) alloc.free(field);
        if (binder.relationalColumnForField(options.schema, field, null) == null) return error.InvalidSqlCatalog;
        try validateGeneratedExpressionItemEnd(generated_item, pos.*);
        try partition_by.append(alloc, field);
        field_transferred = true;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
        if (parser.peekKeyword(tokens, pos.*, "order")) return error.UnsupportedSqlShape;
    }
}

pub fn parseWindowDefinitionAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    options: WindowDefinitionParserOptions,
) !plan_mod.NamedWindowDefinition {
    var partition_by = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (partition_by.items) |field| alloc.free(field);
        partition_by.deinit(alloc);
    }
    if (parser.matchKeyword(tokens, pos, "partition")) {
        try parser.expectKeyword(tokens, pos, "by");
        const generated_partition_end = if (generatedWindowDefinitionPartitionTokens(options)) |range| blk: {
            if (range.start != pos.*) return error.UnsupportedSqlShape;
            const items = generatedWindowDefinitionPartitionItems(options) orelse return error.UnsupportedSqlShape;
            try validateGeneratedExpressionListForClause(tokens, range, items.*);
            break :blk range.end;
        } else null;
        try parseWindowPartitionByAlloc(
            alloc,
            tokens,
            pos,
            &partition_by,
            options,
        );
        if (generated_partition_end) |end| {
            if (pos.* != end) return error.UnsupportedSqlShape;
        }
    } else if (generatedWindowDefinitionPartitionTokens(options) != null) {
        return error.UnsupportedSqlShape;
    }

    var order_by = std.ArrayListUnmanaged(db_mod.types.RelationalRowsQueryOrder).empty;
    errdefer {
        freeOrderBy(alloc, order_by.items);
        order_by.deinit(alloc);
    }
    if (parser.matchKeyword(tokens, pos, "order")) {
        try parser.expectKeyword(tokens, pos, "by");
        const generated_order_items, const generated_order_end = if (generatedWindowDefinitionOrderTokens(options)) |range| blk: {
            if (range.start != pos.*) return error.UnsupportedSqlShape;
            const items = generatedWindowDefinitionOrderItems(options) orelse return error.UnsupportedSqlShape;
            try validateGeneratedOrderListForClause(tokens, range, items.*);
            break :blk .{ items, range.end };
        } else .{ null, null };
        try expr_order.parseByAlloc(
            alloc,
            tokens,
            pos,
            &order_by,
            .{
                .schema = options.schema,
                .function_bindings = options.function_bindings,
                .field_expression_qualifiers = options.field_expression_qualifiers,
                .returning_expression_qualifiers = options.returning_expression_qualifiers,
                .defer_row_expression_field_validation = options.defer_row_expression_field_validation,
                .type_context = options.type_context,
                .order_expression_hooks = options.order_expression_hooks,
                .generated_order_items = generated_order_items,
            },
        );
        if (generated_order_end) |end| {
            if (pos.* != end) return error.UnsupportedSqlShape;
        }
        if (order_by.items.len == 0) return error.UnsupportedSqlShape;
    } else if (generatedWindowDefinitionOrderTokens(options) != null) {
        return error.UnsupportedSqlShape;
    }

    const frame_start = pos.*;
    const frame = try parseOptionalFrame(tokens, pos, params);
    if (options.generated_window_ast != null or options.generated_over_expression_ast != null) {
        if (frame != null) {
            const range = generatedWindowDefinitionFrameTokens(options) orelse return error.UnsupportedSqlShape;
            if (range.start != frame_start or range.end != pos.*) return error.UnsupportedSqlShape;
            try validateGeneratedWindowDefinitionFrameLayout(tokens, range, options);
            try validateGeneratedWindowDefinitionFrameSemantics(tokens, params, options, frame.?);
        } else if (generatedWindowDefinitionFrameTokens(options) != null) {
            return error.UnsupportedSqlShape;
        }
    }
    if (frame != null and order_by.items.len == 0) return error.UnsupportedSqlShape;
    if (frame) |parsed_frame| try validateFrameForOrder(options.schema, options.type_context, parsed_frame, order_by.items);

    const owned_partition_by = try partition_by.toOwnedSlice(alloc);
    var partition_transferred = false;
    errdefer if (!partition_transferred) strings.freeStringSlice(alloc, owned_partition_by);
    const owned_order_by = try order_by.toOwnedSlice(alloc);
    var order_transferred = false;
    errdefer if (!order_transferred) {
        freeOrderBy(alloc, owned_order_by);
        if (owned_order_by.len > 0) alloc.free(owned_order_by);
    };

    partition_transferred = true;
    order_transferred = true;
    return .{
        .partition_by = owned_partition_by,
        .order_by = owned_order_by,
        .frame = frame,
    };
}

pub fn parseNamedWindowSpecsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    end: usize,
    params: []const value_mod.SqlValue,
    window_definition_options: WindowDefinitionParserOptions,
) ![]const plan_mod.NamedWindowSpec {
    var specs = std.ArrayListUnmanaged(plan_mod.NamedWindowSpec).empty;
    errdefer {
        for (specs.items) |spec| plan_mod.freeNamedWindowSpec(alloc, spec);
        specs.deinit(alloc);
    }

    var generated_index: usize = 0;
    while (true) {
        var spec_options = window_definition_options;
        if (window_definition_options.generated_window_items.len != 0) {
            if (generated_index >= window_definition_options.generated_window_items.len) return error.UnsupportedSqlShape;
            const generated_window = &window_definition_options.generated_window_items[generated_index];
            if (generated_window.tokens.start != pos.*) return error.UnsupportedSqlShape;
            spec_options.generated_window_ast = generated_window;
            spec_options.generated_window_items = &.{};
        }
        const spec_start = pos.*;
        const spec = try parseNamedWindowSpecAlloc(
            alloc,
            tokens,
            pos,
            params,
            specs.items,
            spec_options,
        );
        if (spec_options.generated_window_ast) |generated_window| {
            if (generated_window.tokens.start != spec_start or generated_window.tokens.end != pos.*) return error.UnsupportedSqlShape;
        }
        var spec_transferred = false;
        errdefer if (!spec_transferred) plan_mod.freeNamedWindowSpec(alloc, spec);
        try specs.append(alloc, spec);
        spec_transferred = true;
        generated_index += 1;
        if (parser.matchToken(tokens, pos, .comma) == null) break;
        if (pos.* >= end) return error.UnsupportedSqlShape;
    }
    if (window_definition_options.generated_window_items.len != 0 and generated_index != window_definition_options.generated_window_items.len) return error.UnsupportedSqlShape;
    if (pos.* != end) return error.UnsupportedSqlShape;
    return try specs.toOwnedSlice(alloc);
}

pub fn parseTopLevelNamedWindowSpecsAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: usize,
    params: []const value_mod.SqlValue,
    window_definition_options: WindowDefinitionParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst,
) ![]const plan_mod.NamedWindowSpec {
    const start = topLevelClauseStart(tokens, pos) orelse return &.{};
    const end = topLevelClauseEnd(tokens, start + 1);
    var window_pos: usize = start + 1;
    var generated_options = window_definition_options;
    if (generated_read_ast) |read| {
        const generated_end = try generatedWindowClauseEnd(tokens, start, window_pos, read);
        if (generated_end == null or generated_end.? != end) return error.UnsupportedSqlShape;
        generated_options.generated_window_items = read.window_items;
    }
    return try parseNamedWindowSpecsAlloc(
        alloc,
        tokens,
        &window_pos,
        end,
        params,
        generated_options,
    );
}

pub fn parseNamedWindowSpecAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    existing: []const plan_mod.NamedWindowSpec,
    window_definition_options: WindowDefinitionParserOptions,
) !plan_mod.NamedWindowSpec {
    const spec_start = pos.*;
    if (window_definition_options.generated_window_ast) |window| {
        if (window.tokens.start != spec_start or window.name_tokens.start != spec_start) return error.UnsupportedSqlShape;
    }
    const name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    if (window_definition_options.generated_window_ast) |window| {
        if (window.name_tokens.end != pos.*) return error.UnsupportedSqlShape;
    }
    var name_transferred = false;
    errdefer if (!name_transferred) alloc.free(name);
    for (existing) |spec| {
        if (std.ascii.eqlIgnoreCase(spec.name, name)) return error.UnsupportedSqlShape;
    }

    try parser.expectKeyword(tokens, pos, "as");
    try parser.expectToken(tokens, pos, .lparen);
    if (window_definition_options.generated_window_ast) |window| {
        if (window.definition_tokens.start != pos.*) return error.UnsupportedSqlShape;
    }
    const definition = try parseWindowDefinitionAlloc(
        alloc,
        tokens,
        pos,
        params,
        window_definition_options,
    );
    var definition_transferred = false;
    errdefer if (!definition_transferred) plan_mod.freeNamedWindowDefinition(alloc, definition);
    if (window_definition_options.generated_window_ast) |window| {
        if (window.definition_tokens.end != pos.*) return error.UnsupportedSqlShape;
    }
    try parser.expectToken(tokens, pos, .rparen);
    if (window_definition_options.generated_window_ast) |window| {
        if (window.tokens.end != pos.*) return error.UnsupportedSqlShape;
    }

    name_transferred = true;
    definition_transferred = true;
    return .{
        .name = name,
        .partition_by = definition.partition_by,
        .order_by = definition.order_by,
        .frame = definition.frame,
    };
}

pub fn parseWindowDefinitionReferenceAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    named_window_specs: []const plan_mod.NamedWindowSpec,
    window_definition_options: WindowDefinitionParserOptions,
) !plan_mod.NamedWindowDefinition {
    if (parser.matchToken(tokens, pos, .lparen) != null) {
        const definition_start = pos.*;
        if (window_definition_options.generated_over_expression_ast) |expression| {
            if (expression.over_name_tokens != null) return error.UnsupportedSqlShape;
            if (expression.over_definition_tokens) |range| {
                if (range.start != definition_start) return error.UnsupportedSqlShape;
            } else if (expression.over_partition_tokens != null or expression.over_order_tokens != null or expression.over_frame_tokens != null) {
                return error.UnsupportedSqlShape;
            }
        }
        const definition = try parseWindowDefinitionAlloc(
            alloc,
            tokens,
            pos,
            params,
            window_definition_options,
        );
        errdefer plan_mod.freeNamedWindowDefinition(alloc, definition);
        if (window_definition_options.generated_over_expression_ast) |expression| {
            if (expression.over_definition_tokens) |range| {
                if (range.end != pos.*) return error.UnsupportedSqlShape;
            } else if (pos.* != definition_start) {
                return error.UnsupportedSqlShape;
            }
        }
        try parser.expectToken(tokens, pos, .rparen);
        return definition;
    }

    const name_start = pos.*;
    const name = try parseIdentifierOwnedAlloc(alloc, tokens, pos);
    if (window_definition_options.generated_over_expression_ast) |expression| {
        const range = expression.over_name_tokens orelse return error.UnsupportedSqlShape;
        if (range.start != name_start or range.end != pos.*) return error.UnsupportedSqlShape;
    }
    defer alloc.free(name);
    for (named_window_specs) |spec| {
        if (std.ascii.eqlIgnoreCase(spec.name, name)) {
            return try plan_mod.cloneNamedWindowDefinitionAlloc(alloc, spec);
        }
    }
    return error.UnsupportedSqlShape;
}

pub fn parseWindowSpecAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    params: []const value_mod.SqlValue,
    schema: runtime_schema.TableSchema,
    field_expression_qualifiers: []const []const u8,
    returning_expression_qualifiers: []const []const u8,
    defer_row_expression_field_validation: bool,
    type_context: expr_type.RowExpressionTypeContext,
    named_window_specs: []const plan_mod.NamedWindowSpec,
    window_definition_options: WindowDefinitionParserOptions,
    options: WindowSpecParserOptions,
) !db_mod.types.RelationalRowsWindowSpec {
    const function_start = pos.*;
    const generated_function = if (options.generated_expression_ast) |expression| blk: {
        const function = try generatedWindowFunctionForExpression(tokens, expression) orelse return error.UnsupportedSqlShape;
        const expression_tokens = expression.tokens orelse return error.UnsupportedSqlShape;
        if (expression_tokens.start != function_start) return error.UnsupportedSqlShape;
        break :blk function;
    } else null;
    const parsed_function = try parseFunction(tokens, pos);
    const function = if (generated_function) |function| blk: {
        if (function != parsed_function) return error.UnsupportedSqlShape;
        break :blk function;
    } else parsed_function;
    try parser.expectToken(tokens, pos, .lparen);
    const argument_start = pos.*;
    const value_expression_start = pos.*;
    const value_expression: ?db_mod.types.RelationalRowsExpression = switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile => null,
        .lag, .lead, .first_value, .last_value, .nth_value => try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks),
        .count => if (parser.matchToken(tokens, pos, .star) != null) null else try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks),
        .sum, .avg, .min, .max => try expr_row_parse.parseRowExpressionAlloc(alloc, tokens, pos, type_context, options.row_expression_hooks, options.arithmetic_hooks, options.variadic_hooks),
        .bool_or, .bool_and => try expr_row_parse.parseBooleanRowExpressionAlloc(alloc, tokens, pos, type_context, options.boolean_hooks),
    };
    var value_expression_transferred = false;
    errdefer if (!value_expression_transferred) if (value_expression) |expression| freeExpression(alloc, expression);
    if (value_expression) |expression| {
        if (options.generated_expression_ast) |generated_expression| {
            if (generated_expression.argument_items.expressions.len == 0) return error.UnsupportedSqlShape;
            try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(
                tokens,
                value_expression_start,
                pos.*,
                expression,
                &generated_expression.argument_items.expressions[0],
            );
        }
        switch (function) {
            .sum, .avg => try type_context.validateNumericRowExpression(expression),
            .min, .max => try expr_type.validateAggregateMinMaxRowExpression(type_context, expression),
            .bool_or, .bool_and => try type_context.validateBooleanRowExpression(expression),
            else => {},
        }
    }

    const offset: u32 = switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .first_value, .last_value, .count, .sum, .avg, .min, .max, .bool_or, .bool_and => 1,
        .ntile => blk: {
            const parsed_offset = try value_mod.parseSqlU32Value(tokens, pos, params);
            if (parsed_offset == 0) return error.UnsupportedSqlShape;
            break :blk parsed_offset;
        },
        .nth_value => blk: {
            if (parser.matchToken(tokens, pos, .comma) == null) return error.UnsupportedSqlShape;
            const parsed_offset = try value_mod.parseSqlU32Value(tokens, pos, params);
            if (parsed_offset == 0) return error.UnsupportedSqlShape;
            break :blk parsed_offset;
        },
        .lag, .lead => if (parser.matchToken(tokens, pos, .comma) != null) blk: {
            const parsed_offset = try value_mod.parseSqlU32Value(tokens, pos, params);
            if (parsed_offset == 0) return error.UnsupportedSqlShape;
            break :blk parsed_offset;
        } else 1,
    };

    const default_json: []const u8 = switch (function) {
        .row_number, .rank, .dense_rank, .percent_rank, .cume_dist, .ntile, .first_value, .last_value, .nth_value, .count, .sum, .avg, .min, .max, .bool_or, .bool_and => &.{},
        .lag, .lead => if (parser.matchToken(tokens, pos, .comma) != null)
            try value_mod.parseJsonValueAlloc(alloc, tokens, pos, params)
        else
            &.{},
    };
    var default_transferred = false;
    errdefer if (!default_transferred) if (default_json.len > 0) alloc.free(default_json);
    const argument_end = pos.*;
    try parser.expectToken(tokens, pos, .rparen);
    try validateGeneratedWindowFunctionArgumentPayloads(
        tokens,
        function,
        function_start,
        argument_start,
        argument_end,
        options.generated_expression_ast,
    );
    const filter_start = if (parser.peekKeyword(tokens, pos.*, "filter")) pos.* else null;
    const filter_predicate_start = if (filter_start != null) pos.* + 3 else null;
    const generated_filter_expression: ?*const generated_parser.GeneratedSqlExpressionAst = if (filter_start != null) blk: {
        const generated_expression = options.generated_expression_ast orelse break :blk null;
        break :blk generated_expression.filter_expression;
    } else null;
    const filter = try expr_aggregate.parseFilterAlloc(
        alloc,
        tokens,
        pos,
        schema,
        field_expression_qualifiers,
        returning_expression_qualifiers,
        defer_row_expression_field_validation,
        params,
        type_context,
        options.expression_alternatives,
        options.expression_conditions,
        options.fixed_binary,
        options.realtime_ns,
        generated_filter_expression,
    );
    var filter_transferred = false;
    errdefer if (!filter_transferred) expr_aggregate.freeFilter(alloc, filter);
    const filter_end = if (filter_start != null) pos.* else null;
    const filter_predicate_end = if (filter_start != null) pos.* - 1 else null;
    try validateGeneratedWindowFunctionFilterPayloads(
        filter_start,
        filter_predicate_start,
        filter_predicate_end,
        filter_end,
        options.generated_expression_ast,
    );
    if (!functionSupportsFilter(function) and !expr_aggregate.filterIsEmpty(filter)) return error.UnsupportedSqlShape;
    try parser.expectKeyword(tokens, pos, "over");
    const over_start = pos.* - 1;
    var generated_window_definition_options = window_definition_options;
    generated_window_definition_options.generated_over_expression_ast = options.generated_expression_ast;

    const definition = try parseWindowDefinitionReferenceAlloc(
        alloc,
        tokens,
        pos,
        params,
        named_window_specs,
        generated_window_definition_options,
    );
    var definition_transferred = false;
    errdefer if (!definition_transferred) plan_mod.freeNamedWindowDefinition(alloc, definition);
    if (definition.order_by.len == 0 and functionRequiresOrder(function)) return error.UnsupportedSqlShape;
    try validateGeneratedWindowOverClauseForSpec(tokens, function, over_start, pos.*, options.generated_expression_ast);

    const output = try grammar.parseProjectionOutputOwnedAlloc(alloc, tokens, pos, functionName(function));
    var output_transferred = false;
    errdefer if (!output_transferred) alloc.free(output);

    output_transferred = true;
    definition_transferred = true;
    value_expression_transferred = true;
    default_transferred = true;
    filter_transferred = true;
    return .{
        .output = output,
        .function = function,
        .partition_by = definition.partition_by,
        .order_by = definition.order_by,
        .value_expression = value_expression,
        .offset = offset,
        .default_json = default_json,
        .frame = definition.frame,
        .filter_predicates = filter.predicates,
        .filter_array_any = filter.array_any,
        .filter_array_contains = filter.array_contains,
        .filter_array_eq = filter.array_eq,
        .filter_in_predicates = filter.in_predicates,
        .filter_json_contains = filter.json_contains,
        .filter_json_path_eq = filter.json_path_eq,
        .filter_json_path_exists = filter.json_path_exists,
        .filter_text_patterns = filter.text_patterns,
        .filter_expressions = filter.expressions,
        .filter_expression_array_contains = filter.expression_array_contains,
        .filter_any = filter.any_groups,
        .filter_not = filter.not_groups,
    };
}

pub const SelectListParserOptions = struct {
    params: []const value_mod.SqlValue = &.{},
    schema: runtime_schema.TableSchema,
    type_context: expr_type.RowExpressionTypeContext,
    field_expression_qualifiers: []const []const u8 = &.{},
    returning_expression_qualifiers: []const []const u8 = &.{},
    defer_row_expression_field_validation: bool = false,
    named_window_specs: []const plan_mod.NamedWindowSpec = &.{},
    function_bindings: expr_row_parse.SqlFunctionBindings = .{},
    order_expression_hooks: expr_order.OrderExpressionParserOptions,
    window_definition_options: WindowDefinitionParserOptions,
    window_spec_options: WindowSpecParserOptions,
    generated_read_ast: ?*const generated_parser.GeneratedSqlReadAst = null,
};

fn windowSpecOutputCollision(
    output: []const u8,
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
) bool {
    for (fields) |field| {
        if (std.mem.eql(u8, field, output)) return true;
    }
    for (windows) |window| {
        if (std.mem.eql(u8, window.output, output)) return true;
    }
    return false;
}

fn allocateDisambiguatedWindowSpecOutputAlloc(
    alloc: std.mem.Allocator,
    output: []const u8,
    fields: []const []const u8,
    windows: []const db_mod.types.RelationalRowsWindowSpec,
) !?[]const u8 {
    if (!windowSpecOutputCollision(output, fields, windows)) return null;

    var suffix: usize = 2;
    while (true) : (suffix += 1) {
        const candidate = try std.fmt.allocPrint(alloc, "{s}_{d}", .{ output, suffix });
        if (!windowSpecOutputCollision(candidate, fields, windows)) return candidate;
        alloc.free(candidate);
    }
}

fn windowSpecWithOutput(
    spec: db_mod.types.RelationalRowsWindowSpec,
    output: []const u8,
) db_mod.types.RelationalRowsWindowSpec {
    return .{
        .output = output,
        .function = spec.function,
        .partition_by = spec.partition_by,
        .order_by = spec.order_by,
        .value_expression = spec.value_expression,
        .offset = spec.offset,
        .default_json = spec.default_json,
        .frame = spec.frame,
        .filter_predicates = spec.filter_predicates,
        .filter_array_any = spec.filter_array_any,
        .filter_array_contains = spec.filter_array_contains,
        .filter_array_eq = spec.filter_array_eq,
        .filter_in_predicates = spec.filter_in_predicates,
        .filter_json_contains = spec.filter_json_contains,
        .filter_json_path_eq = spec.filter_json_path_eq,
        .filter_json_path_exists = spec.filter_json_path_exists,
        .filter_text_patterns = spec.filter_text_patterns,
        .filter_expressions = spec.filter_expressions,
        .filter_expression_array_contains = spec.filter_expression_array_contains,
        .filter_any = spec.filter_any,
        .filter_not = spec.filter_not,
    };
}

pub fn parseSelectListAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
    options: SelectListParserOptions,
) !plan_mod.WindowSelectList {
    if (parser.matchToken(tokens, pos, .star) != null) return error.UnsupportedSqlShape;

    var fields = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (fields.items) |field| alloc.free(field);
        fields.deinit(alloc);
    }
    var windows = std.ArrayListUnmanaged(db_mod.types.RelationalRowsWindowSpec).empty;
    errdefer {
        plan_mod.freeWindowSpecs(alloc, windows.items);
        windows.deinit(alloc);
    }
    var outputs = std.ArrayListUnmanaged(plan_mod.WindowSelectOutputRef).empty;
    errdefer outputs.deinit(alloc);

    while (true) {
        const item_start = pos.*;
        const generated_item = try generated_read_validate.generatedProjectionItemAtStart(tokens, item_start, options.generated_read_ast);
        if (generated_item == null and options.generated_read_ast != null) return error.UnsupportedSqlShape;
        const generated_expression = if (generated_item) |item| item.expression else null;
        const generated_window_function = if (generated_expression) |expression|
            try generated_read_validate.generatedWindowFunctionForExpression(tokens, expression)
        else
            null;
        const parse_window = if (options.generated_read_ast != null) blk: {
            if (peekFunction(tokens, pos.*) and generated_window_function == null) return error.UnsupportedSqlShape;
            break :blk generated_window_function != null;
        } else peekFunction(tokens, pos.*);

        if (parse_window) {
            var window_spec_options = options.window_spec_options;
            window_spec_options.generated_expression_ast = generated_expression;
            const parsed_spec = try parseWindowSpecAlloc(
                alloc,
                tokens,
                pos,
                options.params,
                options.schema,
                options.field_expression_qualifiers,
                options.returning_expression_qualifiers,
                options.defer_row_expression_field_validation,
                options.type_context,
                options.named_window_specs,
                options.window_definition_options,
                window_spec_options,
            );
            var parsed_spec_transferred = false;
            errdefer if (!parsed_spec_transferred) plan_mod.freeWindowSpec(alloc, parsed_spec);
            const maybe_output = try allocateDisambiguatedWindowSpecOutputAlloc(
                alloc,
                parsed_spec.output,
                fields.items,
                windows.items,
            );
            var output_transferred = false;
            errdefer if (!output_transferred) {
                if (maybe_output) |output| alloc.free(output);
            };
            const spec = if (maybe_output) |output| windowSpecWithOutput(parsed_spec, output) else parsed_spec;
            parsed_spec_transferred = true;
            if (maybe_output != null) {
                alloc.free(parsed_spec.output);
                output_transferred = true;
            }
            var spec_transferred = false;
            errdefer if (!spec_transferred) plan_mod.freeWindowSpec(alloc, spec);
            try generated_read_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
            try outputs.append(alloc, .{ .kind = .window, .index = windows.items.len });
            try windows.append(alloc, spec);
            spec_transferred = true;
        } else {
            try validateGeneratedSimpleGroupExpression(tokens, generated_expression);
            const parsed_field = try expr_generated.parseRowExpressionFieldOwnedAlloc(
                alloc,
                tokens,
                pos,
                options.schema,
                options.field_expression_qualifiers,
                options.returning_expression_qualifiers,
                options.defer_row_expression_field_validation,
            );
            defer alloc.free(parsed_field);
            const field = try binder.normalizeRowExpressionFieldAlloc(
                alloc,
                options.schema,
                parsed_field,
                options.field_expression_qualifiers,
                options.returning_expression_qualifiers,
                options.defer_row_expression_field_validation,
            );
            var field_transferred = false;
            errdefer if (!field_transferred) alloc.free(field);
            if (expr_projection.peekUnsupportedSimpleFieldTail(tokens, pos.*)) return error.UnsupportedSqlShape;
            if (binder.relationalColumnForField(options.schema, field, null) == null) return error.InvalidSqlCatalog;
            try expr_generated_validate.validateGeneratedRowExpressionIdentityStrict(
                tokens,
                item_start,
                pos.*,
                .{ .kind = .field, .field = field },
                generated_expression,
            );
            try grammar.consumeProjectionAlias(alloc, tokens, pos, field);
            try generated_read_validate.validateGeneratedExpressionItemEnd(generated_item, pos.*);
            try outputs.append(alloc, .{ .kind = .field, .index = fields.items.len });
            try fields.append(alloc, field);
            field_transferred = true;
        }
        if (parser.matchToken(tokens, pos, .comma) == null) break;
    }

    try validateSelectListOutputs(fields.items, windows.items);

    const owned_fields = try fields.toOwnedSlice(alloc);
    var fields_transferred = false;
    errdefer if (!fields_transferred) strings.freeStringSlice(alloc, owned_fields);
    const owned_windows = try windows.toOwnedSlice(alloc);
    var windows_transferred = false;
    errdefer if (!windows_transferred) {
        plan_mod.freeWindowSpecs(alloc, owned_windows);
        if (owned_windows.len > 0) alloc.free(owned_windows);
    };
    const owned_outputs = try outputs.toOwnedSlice(alloc);

    fields_transferred = true;
    windows_transferred = true;
    return .{
        .fields = owned_fields,
        .windows = owned_windows,
        .outputs = owned_outputs,
        .select_all = false,
    };
}

test "sql expr_window validates frame helpers" {
    try std.testing.expect(functionRequiresOrder(.lag));
    try std.testing.expect(!functionRequiresOrder(.count));
    try std.testing.expectEqualStrings("row_number", functionName(.row_number));
    try std.testing.expectEqual(db_mod.types.RelationalRowsWindowFunction.rank, functionForName("RANK").?);
    try std.testing.expect(functionSupportsFilter(.count));
    try std.testing.expect(!functionSupportsFilter(.lag));
    const top_level_window_tokens = [_]Token{
        .{ .kind = .identifier, .text = "SELECT", .keyword = .select },
        .{ .kind = .identifier, .text = "WINDOW", .keyword = .window },
    };
    try std.testing.expectEqual(@as(?usize, 1), topLevelClauseStart(top_level_window_tokens[0..], 0));
    const quoted_window_tokens = [_]Token{
        .{ .kind = .identifier, .text = "window", .source_start = 0, .source_end = 8 },
    };
    try std.testing.expect(topLevelClauseStart(quoted_window_tokens[0..], 0) == null);
    const generated_window_count_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 3 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .window_function_kind = .count,
    };
    try validateGeneratedWindowFunctionFilterPayloads(null, null, null, null, &generated_window_count_ast);
    const stale_window_count_filter_kind = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 3 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .window_function_kind = .count,
        .filter_expression_kind = .token_range,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedWindowFunctionFilterPayloads(null, null, null, null, &stale_window_count_filter_kind),
    );
    var generated_filter_child = generated_parser.GeneratedSqlExpressionAst{
        .kind = .comparison,
        .tokens = .{ .start = 3, .end = 6 },
    };
    const filtered_window_count_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 8 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .window_function_kind = .count,
        .filter_tokens = .{ .start = 0, .end = 8 },
        .filter_predicate_tokens = .{ .start = 3, .end = 6 },
        .filter_expression = &generated_filter_child,
    };
    try validateGeneratedWindowFunctionFilterPayloads(0, 3, 6, 8, &filtered_window_count_ast);
    const stale_filtered_window_count_ast = generated_parser.GeneratedSqlExpressionAst{
        .kind = .function_call,
        .tokens = .{ .start = 0, .end = 8 },
        .function_name_tokens = .{ .start = 0, .end = 1 },
        .window_function_kind = .count,
        .filter_tokens = .{ .start = 0, .end = 8 },
        .filter_predicate_tokens = .{ .start = 3, .end = 5 },
        .filter_expression = &generated_filter_child,
    };
    try std.testing.expectError(
        error.UnsupportedSqlShape,
        validateGeneratedWindowFunctionFilterPayloads(0, 3, 6, 8, &stale_filtered_window_count_ast),
    );

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
        .filter_array_any = &.{.{ .field = "tags", .value_json = "[\"new\"]" }},
    }};
    try std.testing.expectEqual(@as(usize, 1), valueExpressionCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), defaultCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), filterPredicateCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), filterExpressionCount(&windows));
    try std.testing.expectEqual(@as(usize, 1), filterAccessCount(&windows));
    try std.testing.expect(frameSignature(&windows) != 0);
}
