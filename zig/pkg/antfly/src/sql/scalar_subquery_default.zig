// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

const SourceBinding = struct {
    table_name: []const u8,
    alias: []const u8 = "",
};

pub fn validatePayloadAlloc(alloc: std.mem.Allocator, value_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return error.InvalidRowsRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRowsRequest;
    try requireOnlyKeys(parsed.value.object, &.{"query"});
    const query = parsed.value.object.get("query") orelse return error.InvalidRowsRequest;
    try validateQuery(query);
}

fn validateQuery(value: std.json.Value) !void {
    if (value != .object) return error.InvalidRowsRequest;
    if (value.object.get("kind") != null) return error.UnsupportedSqlShape;
    try requireOnlyKeys(value.object, &.{ "table", "alias", "select", "expressions", "where", "limit", "offset", "order_by", "distinct_on" });

    const table_value = value.object.get("table") orelse return error.InvalidRowsRequest;
    if (table_value != .string or table_value.string.len == 0) return error.InvalidRowsRequest;
    const binding: SourceBinding = .{
        .table_name = normalizeTableName(table_value.string),
        .alias = try validateAlias(value.object.get("alias")),
    };

    const select = value.object.get("select");
    const expressions = value.object.get("expressions");
    if ((select == null) == (expressions == null)) return error.UnsupportedSqlShape;
    if (select) |selected| {
        if (selected != .array or selected.array.items.len != 1) return error.UnsupportedSqlShape;
        const field = selected.array.items[0];
        if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
        try validateDistinctOn(value.object.get("distinct_on"), normalizeFieldName(field.string, binding));
    } else {
        if (value.object.get("distinct_on") != null) return error.UnsupportedSqlShape;
        try validateExpressionProjection(expressions.?);
    }

    try validateWhere(value.object.get("where"));
    try validateOrderBy(value.object.get("order_by"));
    try validateOptionalU32(value.object.get("limit"));
    try validateOptionalU32(value.object.get("offset"));
}

fn validateExpressionProjection(value: std.json.Value) !void {
    if (value != .array or value.array.items.len != 1) return error.UnsupportedSqlShape;
    const item = value.array.items[0];
    if (item != .object) return error.InvalidRowsRequest;
    try requireOnlyKeys(item.object, &.{ "as", "expr" });
    const output = item.object.get("as") orelse return error.InvalidRowsRequest;
    if (output != .string or output.string.len == 0) return error.InvalidRowsRequest;
    try validateExpression(item.object.get("expr") orelse return error.InvalidRowsRequest);
}

fn validateExpression(value: std.json.Value) !void {
    if (value != .object) return error.InvalidRowsRequest;
    if (value.object.get("field")) |field| {
        try requireOnlyKeys(value.object, &.{"field"});
        if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
        return;
    }
    if (value.object.get("value") != null) {
        try requireOnlyKeys(value.object, &.{"value"});
        return;
    }

    const op_value = value.object.get("op") orelse return error.InvalidRowsRequest;
    const args_value = value.object.get("args") orelse return error.InvalidRowsRequest;
    if (op_value != .string or args_value != .array) return error.InvalidRowsRequest;
    const kind = expressionKind(op_value.string) orelse return error.UnsupportedSqlShape;
    if (kind == .cast) {
        try requireOnlyKeys(value.object, &.{ "op", "args", "to" });
    } else {
        try requireOnlyKeys(value.object, &.{ "op", "args" });
    }
    if (!expressionArityValid(kind, args_value.array.items.len)) return error.UnsupportedSqlShape;
    for (args_value.array.items) |arg| try validateExpression(arg);
    if (kind == .cast) {
        const target = value.object.get("to") orelse return error.InvalidRowsRequest;
        if (target != .string or !castTypeSupported(target.string)) return error.InvalidRowsRequest;
    }
}

const ExpressionKind = enum { lower, upper, length, md5, concat, reverse, cast, add, sub, mul, div, mod };

fn expressionKind(op: []const u8) ?ExpressionKind {
    inline for (std.meta.fields(ExpressionKind)) |field| {
        if (std.ascii.eqlIgnoreCase(op, field.name)) return @enumFromInt(field.value);
    }
    return null;
}

fn expressionArityValid(kind: ExpressionKind, len: usize) bool {
    return switch (kind) {
        .concat, .sub, .div, .mod => len == 2,
        .add, .mul => len >= 2,
        .cast, .lower, .upper, .length, .md5, .reverse => len == 1,
    };
}

fn castTypeSupported(value: []const u8) bool {
    return std.mem.eql(u8, value, "text") or
        std.mem.eql(u8, value, "numeric") or
        std.mem.eql(u8, value, "bool") or
        std.mem.eql(u8, value, "boolean") or
        std.mem.eql(u8, value, "datetime");
}

fn validateWhere(maybe_value: ?std.json.Value) !void {
    const value = maybe_value orelse return;
    if (value != .object) return error.InvalidRowsRequest;
    if (value.object.get("any") != null or value.object.get("not") != null) {
        try requireOnlyKeys(value.object, &.{ "all", "any", "not" });
        if (value.object.get("all")) |all| try validatePredicateArray(all, false);
        if (value.object.get("any")) |any| try validatePredicateGroups(any);
        if (value.object.get("not")) |not| try validatePredicateGroups(not);
        return;
    }
    if (value.object.get("all")) |all| {
        try requireOnlyKeys(value.object, &.{"all"});
        try validatePredicateAll(all);
        return;
    }
    if (try predicateIsIn(value)) return validateInPredicate(value);
    try validatePredicate(value);
}

fn validatePredicateAll(value: std.json.Value) !void {
    if (value != .array) return error.InvalidRowsRequest;
    for (value.array.items) |item| {
        if (item == .object and item.object.get("any") != null) {
            try requireOnlyKeys(item.object, &.{"any"});
            try validatePredicateGroups(item.object.get("any").?);
        } else if (item == .object and item.object.get("not") != null) {
            try requireOnlyKeys(item.object, &.{"not"});
            try validatePredicateGroups(item.object.get("not").?);
        } else if (item == .object and item.object.get("all") != null) {
            try requireOnlyKeys(item.object, &.{"all"});
            try validatePredicateAll(item.object.get("all").?);
        } else if (try predicateIsIn(item)) {
            try validateInPredicate(item);
        } else {
            try validatePredicate(item);
        }
    }
}

fn validatePredicateGroups(value: std.json.Value) !void {
    if (value != .array or value.array.items.len == 0) return error.InvalidRowsRequest;
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        if (item.object.get("all")) |all| {
            try requireOnlyKeys(item.object, &.{"all"});
            try validatePredicateArray(all, true);
        } else {
            try validatePredicate(item);
        }
    }
}

fn validatePredicateArray(value: std.json.Value, require_nonempty: bool) !void {
    if (value != .array or (require_nonempty and value.array.items.len == 0)) return error.InvalidRowsRequest;
    for (value.array.items) |item| try validatePredicate(item);
}

fn predicateIsIn(value: std.json.Value) !bool {
    if (value != .object) return false;
    const op = value.object.get("op") orelse return false;
    if (op != .string) return error.InvalidRowsRequest;
    return std.mem.eql(u8, op.string, "in") or std.mem.eql(u8, op.string, "not_in");
}

fn validateInPredicate(value: std.json.Value) !void {
    if (value != .object) return error.InvalidRowsRequest;
    try requireOnlyKeys(value.object, &.{ "field", "op", "value" });
    const field = value.object.get("field") orelse return error.InvalidRowsRequest;
    const op = value.object.get("op") orelse return error.InvalidRowsRequest;
    const list = value.object.get("value") orelse return error.InvalidRowsRequest;
    if (field != .string or field.string.len == 0 or op != .string) return error.InvalidRowsRequest;
    if (!std.mem.eql(u8, op.string, "in") and !std.mem.eql(u8, op.string, "not_in")) return error.InvalidRowsRequest;
    if (list != .array or list.array.items.len == 0) return error.InvalidRowsRequest;
}

fn validatePredicate(value: std.json.Value) !void {
    if (value != .object) return error.InvalidRowsRequest;
    try requireOnlyKeys(value.object, &.{ "field", "op", "value" });
    const field = value.object.get("field") orelse return error.InvalidRowsRequest;
    const op = value.object.get("op") orelse return error.InvalidRowsRequest;
    if (field != .string or field.string.len == 0 or op != .string or op.string.len == 0) return error.InvalidRowsRequest;
    const needs_value = if (std.mem.eql(u8, op.string, "is_null") or std.mem.eql(u8, op.string, "is_not_null"))
        false
    else if (std.mem.eql(u8, op.string, "is_distinct") or
        std.mem.eql(u8, op.string, "is_not_distinct") or
        std.mem.eql(u8, op.string, "eq") or
        std.mem.eql(u8, op.string, "ne") or
        std.mem.eql(u8, op.string, "gt") or
        std.mem.eql(u8, op.string, "gte") or
        std.mem.eql(u8, op.string, "lt") or
        std.mem.eql(u8, op.string, "lte"))
        true
    else
        return error.InvalidRowsRequest;
    if (needs_value != (value.object.get("value") != null)) return error.InvalidRowsRequest;
}

fn validateOrderBy(maybe_value: ?std.json.Value) !void {
    const value = maybe_value orelse return;
    if (value != .array) return error.InvalidRowsRequest;
    for (value.array.items) |item| {
        if (item != .object) return error.InvalidRowsRequest;
        try requireOnlyKeys(item.object, &.{ "field", "direction", "null_test" });
        const field = item.object.get("field") orelse return error.InvalidRowsRequest;
        if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
        if (item.object.get("direction")) |direction| {
            if (direction != .string or
                (!std.ascii.eqlIgnoreCase(direction.string, "asc") and !std.ascii.eqlIgnoreCase(direction.string, "desc")))
            {
                return error.InvalidRowsRequest;
            }
        }
        if (item.object.get("null_test")) |null_test| {
            if (null_test != .string or
                (!std.mem.eql(u8, null_test.string, "is_null") and !std.mem.eql(u8, null_test.string, "is_not_null")))
            {
                return error.InvalidRowsRequest;
            }
        }
    }
}

fn validateDistinctOn(maybe_value: ?std.json.Value, selected_field: []const u8) !void {
    const value = maybe_value orelse return;
    if (value != .array or value.array.items.len != 1) return error.UnsupportedSqlShape;
    const field = value.array.items[0];
    if (field != .string or field.string.len == 0) return error.InvalidRowsRequest;
    if (!std.mem.eql(u8, field.string, selected_field)) return error.UnsupportedSqlShape;
}

fn validateOptionalU32(maybe_value: ?std.json.Value) !void {
    const value = maybe_value orelse return;
    if (value != .integer or value.integer < 0 or value.integer > std.math.maxInt(u32)) return error.InvalidRowsRequest;
}

fn validateAlias(maybe_value: ?std.json.Value) ![]const u8 {
    const value = maybe_value orelse return "";
    if (value != .string or value.string.len == 0 or std.mem.indexOfScalar(u8, value.string, '.') != null) return error.InvalidRowsRequest;
    return value.string;
}

fn normalizeTableName(name: []const u8) []const u8 {
    const member = stripQualifier(name, "public") orelse return name;
    if (member.len == 0 or std.mem.indexOfScalar(u8, member, '.') != null) return name;
    return member;
}

fn normalizeFieldName(name: []const u8, binding: SourceBinding) []const u8 {
    if (stripQualifier(name, binding.alias)) |member| if (member.len != 0) return member;
    if (stripQualifier(name, binding.table_name)) |member| if (member.len != 0) return member;
    if (stripQualifier(name, "public")) |public_member| {
        if (stripQualifier(public_member, binding.table_name)) |member| if (member.len != 0) return member;
    }
    return name;
}

fn stripQualifier(name: []const u8, qualifier: []const u8) ?[]const u8 {
    if (qualifier.len == 0 or name.len <= qualifier.len + 1 or name[qualifier.len] != '.') return null;
    if (!std.ascii.eqlIgnoreCase(name[0..qualifier.len], qualifier)) return null;
    return name[qualifier.len + 1 ..];
}

fn requireOnlyKeys(object: std.json.ObjectMap, comptime allowed: []const []const u8) !void {
    for (object.keys()) |key| {
        var found = false;
        inline for (allowed) |allowed_key| {
            if (std.mem.eql(u8, key, allowed_key)) found = true;
        }
        if (!found) return error.InvalidRowsRequest;
    }
}

test "scalar subquery default validator accepts structured query grammar" {
    try validatePayloadAlloc(std.testing.allocator,
        \\{"query":{"table":"public.usage","alias":"u","expressions":[{"as":"amount_text","expr":{"op":"cast","to":"text","args":[{"op":"add","args":[{"field":"u.amount"},{"value":2}]}]}}],"where":{"all":[{"field":"u.id","op":"in","value":[1,2]},{"any":[{"field":"status","op":"eq","value":"ready"}]}]},"order_by":[{"field":"u.id","direction":"DESC","null_test":"is_not_null"}],"limit":1,"offset":0}}
    );
}

test "scalar subquery default validator rejects unsupported shapes" {
    try std.testing.expectError(error.InvalidRowsRequest, validatePayloadAlloc(std.testing.allocator, "{}"));
    try std.testing.expectError(error.UnsupportedSqlShape, validatePayloadAlloc(std.testing.allocator,
        \\{"query":{"table":"usage","select":["id"],"expressions":[{"as":"id","expr":{"field":"id"}}]}}
    ));
    try std.testing.expectError(error.UnsupportedSqlShape, validatePayloadAlloc(std.testing.allocator,
        \\{"query":{"table":"usage","expressions":[{"as":"id","expr":{"op":"concat","args":[{"field":"id"}]}}]}}
    ));
}
