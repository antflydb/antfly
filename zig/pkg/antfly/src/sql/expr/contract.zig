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

const runtime_schema = @import("../../storage/schema.zig");

pub const Expression = runtime_schema.RelationalRowsExpression;
pub const Condition = runtime_schema.RelationalRowsExpressionCondition;
pub const Assignment = runtime_schema.RelationalRowsExpressionAssignment;
pub const Projection = runtime_schema.RelationalRowsExpressionProjection;
pub const UniqueExpression = runtime_schema.UniqueExpression;

pub const TypedExpressionFamily = enum {
    structural,
    text,
    json,
    array,
    regex,
    datetime,
    numeric,
    boolean,
    query_function,
};

pub const TypedExpressionSurface = enum {
    check_constraint,
    generated_column,
    partial_index_predicate,
    expression_index_element,
    conflict_action,
    update_transform,
    aggregate_filter,
    having,
    order_key,
    window,
    returning,
    alter_column_type_using,
};

pub const TypedExpressionShape = enum {
    scalar,
    condition,
    assignment,
    order_key,
    window_spec,
};

pub fn surfaceShape(surface: TypedExpressionSurface) TypedExpressionShape {
    return switch (surface) {
        .check_constraint,
        .partial_index_predicate,
        .aggregate_filter,
        .having,
        => .condition,

        .conflict_action,
        .update_transform,
        => .assignment,

        .order_key => .order_key,
        .window => .window_spec,

        .generated_column,
        .expression_index_element,
        .returning,
        .alter_column_type_using,
        => .scalar,
    };
}

pub fn surfaceRequiresDeterministicExpression(surface: TypedExpressionSurface) bool {
    return switch (surface) {
        .check_constraint,
        .generated_column,
        .partial_index_predicate,
        .expression_index_element,
        .alter_column_type_using,
        => true,

        .conflict_action,
        .update_transform,
        .aggregate_filter,
        .having,
        .order_key,
        .window,
        .returning,
        => false,
    };
}

pub fn expressionFamily(kind: runtime_schema.RelationalRowsExpressionKind) TypedExpressionFamily {
    return switch (kind) {
        .field,
        .value,
        .coalesce,
        .nullif,
        .greatest,
        .least,
        .case,
        .cast,
        => .structural,

        .now,
        .uuid_v4,
        => .query_function,

        .lower,
        .upper,
        .initcap,
        .trim,
        .ltrim,
        .rtrim,
        .replace,
        .translate,
        .substring,
        .overlay,
        .split_part,
        .strpos,
        .left,
        .right,
        .lpad,
        .rpad,
        .repeat,
        .reverse,
        .starts_with,
        .ends_with,
        .ascii,
        .chr,
        .md5,
        .like,
        .ilike,
        .concat,
        .concat_ws,
        .length,
        .octet_length,
        .bit_length,
        => .text,

        .regexp_replace,
        .regexp_match,
        .regexp_count,
        .regexp_instr,
        .regexp_substr,
        => .regex,

        .bool_and,
        .bool_or,
        .bool_not,
        => .boolean,

        .abs,
        .round,
        .trunc,
        .floor,
        .ceil,
        .sqrt,
        .sign,
        .power,
        .add,
        .sub,
        .mul,
        .div,
        .mod,
        => .numeric,

        .interval_ns,
        .interval_months,
        .date_trunc,
        .date_bin,
        .date_part,
        => .datetime,

        .json_extract,
        .json_typeof,
        .json_array_length,
        .json_build_object,
        .to_jsonb,
        .json_path_exists,
        => .json,

        .array_length,
        .array_position,
        .array_positions,
        .array_append,
        .array_prepend,
        .array_cat,
        .array_remove,
        .array_replace,
        .array_to_string,
        .string_to_array,
        => .array,
    };
}

pub fn familyIsPublished(family: TypedExpressionFamily) bool {
    return switch (family) {
        .text,
        .json,
        .array,
        .regex,
        .datetime,
        .numeric,
        .boolean,
        .query_function,
        => true,
        .structural => false,
    };
}

test "sql expr contract publishes every required typed expression family" {
    inline for (std.meta.fields(TypedExpressionFamily)) |field| {
        const family: TypedExpressionFamily = @field(TypedExpressionFamily, field.name);
        if (family == .structural) continue;
        try std.testing.expect(familyIsPublished(family));
    }
}

test "sql expr contract classifies every durable row expression kind" {
    var saw_text = false;
    var saw_json = false;
    var saw_array = false;
    var saw_regex = false;
    var saw_datetime = false;
    var saw_numeric = false;
    var saw_boolean = false;
    var saw_query_function = false;

    inline for (std.meta.fields(runtime_schema.RelationalRowsExpressionKind)) |field| {
        const kind: runtime_schema.RelationalRowsExpressionKind = @field(runtime_schema.RelationalRowsExpressionKind, field.name);
        switch (expressionFamily(kind)) {
            .structural => {},
            .text => saw_text = true,
            .json => saw_json = true,
            .array => saw_array = true,
            .regex => saw_regex = true,
            .datetime => saw_datetime = true,
            .numeric => saw_numeric = true,
            .boolean => saw_boolean = true,
            .query_function => saw_query_function = true,
        }
    }

    try std.testing.expect(saw_text);
    try std.testing.expect(saw_json);
    try std.testing.expect(saw_array);
    try std.testing.expect(saw_regex);
    try std.testing.expect(saw_datetime);
    try std.testing.expect(saw_numeric);
    try std.testing.expect(saw_boolean);
    try std.testing.expect(saw_query_function);
}

test "sql expr contract pins shared expression surfaces" {
    try std.testing.expectEqual(TypedExpressionShape.condition, surfaceShape(.check_constraint));
    try std.testing.expectEqual(TypedExpressionShape.scalar, surfaceShape(.generated_column));
    try std.testing.expectEqual(TypedExpressionShape.condition, surfaceShape(.partial_index_predicate));
    try std.testing.expectEqual(TypedExpressionShape.scalar, surfaceShape(.expression_index_element));
    try std.testing.expectEqual(TypedExpressionShape.assignment, surfaceShape(.conflict_action));
    try std.testing.expectEqual(TypedExpressionShape.assignment, surfaceShape(.update_transform));
    try std.testing.expectEqual(TypedExpressionShape.condition, surfaceShape(.aggregate_filter));
    try std.testing.expectEqual(TypedExpressionShape.condition, surfaceShape(.having));
    try std.testing.expectEqual(TypedExpressionShape.order_key, surfaceShape(.order_key));
    try std.testing.expectEqual(TypedExpressionShape.window_spec, surfaceShape(.window));
    try std.testing.expectEqual(TypedExpressionShape.scalar, surfaceShape(.returning));
    try std.testing.expectEqual(TypedExpressionShape.scalar, surfaceShape(.alter_column_type_using));

    inline for (std.meta.fields(TypedExpressionSurface)) |field| {
        const surface: TypedExpressionSurface = @field(TypedExpressionSurface, field.name);
        _ = surfaceShape(surface);
        _ = surfaceRequiresDeterministicExpression(surface);
    }
}
