// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Shared lowering for the bounded SQL arbiter predicate and native unique
//! predicate contracts. Keeping this at the SQL/storage boundary prevents
//! HTTP and Lite from drifting in their partial-index inference semantics.
const std = @import("std");
const catalog = @import("catalog.zig");
const native = @import("../storage/relational_index.zig");

pub fn expressionsToNative(alloc: std.mem.Allocator, expressions: []const catalog.ConflictExpression) ![]const native.RelationalIndexKey {
    const keys = try alloc.alloc(native.RelationalIndexKey, expressions.len);
    for (expressions, keys) |expression, *key| key.* = .{
        .expression_json = expression.json,
        .result_type = std.meta.stringToEnum(@import("../storage/schema.zig").RelationalColumnType, @tagName(expression.result_type)) orelse return error.UnsupportedSqlShape,
    };
    return keys;
}

pub fn toNative(alloc: std.mem.Allocator, conditions: []const catalog.Condition) ![]const native.UniquePredicate {
    const result = try alloc.alloc(native.UniquePredicate, conditions.len);
    for (conditions, result) |condition, *predicate| {
        const op: native.UniquePredicateOp = switch (condition.op) {
            .eq => .eq,
            .neq => .ne,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            .is_null => .is_null,
            .is_not_null => .is_not_null,
        };
        predicate.* = .{
            .field = condition.column,
            .op = op,
            .value_json = if (condition.op == .is_null or condition.op == .is_not_null)
                null
            else
                try std.json.Stringify.valueAlloc(alloc, condition.value, .{}),
        };
    }
    return result;
}
