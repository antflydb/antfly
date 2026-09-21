// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded public shape validation, independent of a table's typed compiler.
//! Storage binds types and dependencies; this boundary rejects ambiguous or
//! unsupported JSON before admission and preserves the same closed GET shape.
const std = @import("std");
const wire = @import("antfly_schema_openapi");
const impl = @import("../schema/table_schema_impl.zig");
const ColumnKind = @import("../storage/schema.zig").RelationalColumnType;

/// Request-local cached binding for column/op predicates. Parsed API handlers
/// reuse their immutable schema; raw/wire paths parse only referenced fields.
pub const ColumnTypes = struct {
    alloc: std.mem.Allocator,
    source: union(enum) { parsed: *const impl.TableSchema, raw: *const std.json.Value, public: *const wire.TableSchema },
    cache: std.StringHashMapUnmanaged(?ColumnKind) = .empty,

    pub fn deinit(self: *ColumnTypes) void {
        self.cache.deinit(self.alloc);
    }

    pub fn kind(self: *ColumnTypes, name: []const u8) !?ColumnKind {
        if (self.cache.get(name)) |known| return known;
        const result = switch (self.source) {
            .parsed => |schema| found: {
                for (schema.document_schemas) |document| for (document.properties) |property| {
                    if (std.mem.eql(u8, property.name, name)) break :found @import("../schema/mod.zig").runtimeRelationalColumnType(property);
                };
                break :found null;
            },
            .raw => |schema| found: {
                if (schema.* != .object) break :found null;
                const documents = schema.object.get("document_schemas") orelse break :found null;
                if (documents != .object) break :found null;
                var it = documents.object.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* != .object) continue;
                    const document = entry.value_ptr.object.get("schema") orelse continue;
                    if (document != .object) continue;
                    if (try impl.projectedRelationalColumnType(self.alloc, document.object, name)) |bound| break :found bound;
                }
                break :found null;
            },
            .public => |schema| found: {
                const documents = schema.document_schemas orelse break :found null;
                var it = documents.map.iterator();
                while (it.next()) |entry| {
                    const document = entry.value_ptr.schema orelse continue;
                    if (try impl.projectedRelationalColumnType(self.alloc, document.map, name)) |bound| break :found bound;
                }
                break :found null;
            },
        };
        try self.cache.put(self.alloc, name, result);
        return result;
    }

    pub fn canonicalValue(self: *ColumnTypes, alloc: std.mem.Allocator, name: []const u8, value: std.json.Value) !std.json.Value {
        if (value == .null or value == .string) return value;
        const column_type = (try self.kind(name)) orelse return value;
        return switch (column_type) {
            .integer => canonicalLiteral(alloc, .integer, value),
            .datetime => canonicalLiteral(alloc, .datetime, value),
            else => value,
        };
    }
};

pub fn valid(value: std.json.Value) bool {
    var nodes: usize = 0;
    return visit(value, 0, &nodes);
}

fn visit(value: std.json.Value, depth: usize, nodes: *usize) bool {
    if (depth >= 16 or nodes.* >= 128 or value != .object) return false;
    nodes.* += 1;
    const op_value = value.object.get("op") orelse return false;
    if (op_value != .string) return false;
    const op = std.meta.stringToEnum(wire.RelationalExpressionOp, op_value.string) orelse return false;
    const comparison = switch (op) {
        .eq, .ne, .gt, .gte, .lt, .lte, .is_distinct, .is_not_distinct => true,
        else => false,
    };
    var fields = value.object.iterator();
    while (fields.next()) |field| {
        const name = field.key_ptr.*;
        if (std.mem.eql(u8, name, "op")) continue;
        const allowed = switch (op) {
            .literal => std.mem.eql(u8, name, "type") or std.mem.eql(u8, name, "value"),
            .column => std.mem.eql(u8, name, "column"),
            else => std.mem.eql(u8, name, "args") or (comparison and std.mem.eql(u8, name, "collation")),
        };
        if (!allowed) return false;
    }
    switch (op) {
        .literal => {
            const kind = value.object.get("type") orelse return false;
            if (!validType(kind)) return false;
            if (value.object.get("value")) |literal| if (literal == .object or literal == .array) return false;
        },
        .column => {
            const column = value.object.get("column") orelse return false;
            if (column != .string or column.string.len == 0) return false;
        },
        else => {
            if (value.object.get("collation")) |collation| if (collation != .string or collation.string.len == 0) return false;
            const args = value.object.get("args") orelse return false;
            if (args != .array) return false;
            const count = args.array.items.len;
            const correct = switch (op) {
                .negate, .lower_ascii, .upper_ascii, .is_null, .is_not_null, .not => count == 1,
                .concat, .coalesce, .@"and", .@"or" => count >= 2 and count <= 32,
                else => count == 2,
            };
            if (!correct) return false;
            for (args.array.items) |arg| if (!visit(arg, depth + 1, nodes)) return false;
        },
    }
    return true;
}

pub fn validType(value: std.json.Value) bool {
    return value == .string and std.meta.stringToEnum(wire.RelationalExpressionType, value.string) != null;
}

/// The generated response tree is arena-owned. Preserve exact typed values
/// across JavaScript/Go JSON decoders, whose generic numeric values use f64.
/// This changes transport spelling only; native typed fingerprints are equal.
pub fn canonicalizeOwnedExpression(alloc: std.mem.Allocator, expression: *wire.RelationalScalarExpression) !void {
    if (expression.op == .literal) {
        if (expression.type) |kind| if (expression.value) |value| {
            expression.value = try canonicalLiteral(alloc, kind, value);
        };
    }
    if (expression.args) |args| for (@constCast(args)) |*child| try canonicalizeOwnedExpression(alloc, child);
}

fn canonicalLiteral(alloc: std.mem.Allocator, kind: wire.RelationalExpressionType, value: std.json.Value) !std.json.Value {
    if (value == .null or value == .string) return value;
    const safe = 9007199254740991;
    switch (kind) {
        .integer => {
            // Invalid values remain untouched for normal typed admission to
            // reject, never rounded or silently coerced by normalization.
            const integer = @import("../schema/table_schema_impl.zig").documentIntegerToI64(value) orelse return value;
            if (integer > safe or integer < -safe) return .{ .string = try std.fmt.allocPrint(alloc, "{d}", .{integer}) };
        },
        .datetime => {
            const timestamp = @import("../schema/table_schema_impl.zig").documentDateTimeToNs(value) orelse return value;
            if (timestamp > safe) return .{ .string = try std.fmt.allocPrint(alloc, "{d}", .{timestamp}) };
        },
        else => {},
    }
    return value;
}

/// Normalize only known expression declarations, never arbitrary document
/// schema literals. Used before source/runtime schema equality and persistence
/// so GET -> PUT retains both the table epoch and index generation identity.
pub fn canonicalizeSchemaValue(alloc: std.mem.Allocator, schema: *std.json.Value) !void {
    if (schema.* != .object) return;
    var columns: ColumnTypes = .{ .alloc = alloc, .source = .{ .raw = schema } };
    defer columns.deinit();
    for ([_][]const u8{ "column_defaults", "generated_columns", "checks" }) |field| {
        const items = schema.object.getPtr(field) orelse continue;
        if (items.* != .array) continue;
        for (items.array.items) |*item| if (item.* == .object) {
            if (item.object.getPtr("expression")) |expression| {
                var nodes: usize = 0;
                try canonicalizeExpressionValue(alloc, expression, 0, &nodes);
            }
            if (std.mem.eql(u8, field, "checks")) try canonicalizePredicateValue(alloc, &columns, item);
        };
    }
    const indexes = schema.object.getPtr("relational_indexes") orelse return;
    if (indexes.* != .array) return;
    for (indexes.array.items) |*index| if (index.* == .object) {
        if (index.object.getPtr("where")) |conditions| if (conditions.* == .array) {
            for (conditions.array.items) |*condition| try canonicalizePredicateValue(alloc, &columns, condition);
        };
        const keys = index.object.getPtr("keys") orelse continue;
        if (keys.* != .array) continue;
        for (keys.array.items) |*key| if (key.* == .object) {
            if (key.object.getPtr("expression")) |expression| {
                var nodes: usize = 0;
                try canonicalizeExpressionValue(alloc, expression, 0, &nodes);
            }
        };
    };
}

fn canonicalizePredicateValue(alloc: std.mem.Allocator, columns: *ColumnTypes, predicate: *std.json.Value) !void {
    if (predicate.* != .object) return;
    const name = predicate.object.get("column") orelse return;
    if (name != .string) return;
    if (predicate.object.getPtr("value")) |value| value.* = try columns.canonicalValue(alloc, name.string, value.*);
}

fn canonicalizeExpressionValue(alloc: std.mem.Allocator, expression: *std.json.Value, depth: usize, nodes: *usize) !void {
    if (depth >= 16 or nodes.* >= 128) return error.InvalidSchemaUpdateRequest;
    nodes.* += 1;
    if (expression.* != .object) return;
    const op = expression.object.get("op") orelse return;
    if (op == .string and std.mem.eql(u8, op.string, "literal")) {
        const kind_value = expression.object.get("type") orelse return;
        if (kind_value != .string) return;
        const kind = std.meta.stringToEnum(wire.RelationalExpressionType, kind_value.string) orelse return;
        if (expression.object.getPtr("value")) |value| value.* = try canonicalLiteral(alloc, kind, value.*);
    }
    if (expression.object.getPtr("args")) |args| if (args.* == .array) {
        for (args.array.items) |*child| try canonicalizeExpressionValue(alloc, child, depth + 1, nodes);
    };
}

pub fn canonicalizeOwnedSchema(alloc: std.mem.Allocator, schema: *wire.TableSchema) !void {
    var columns: ColumnTypes = .{ .alloc = alloc, .source = .{ .public = schema } };
    defer columns.deinit();
    if (schema.column_defaults) |items| for (@constCast(items)) |*item| try canonicalizeOwnedExpression(alloc, &item.expression);
    if (schema.generated_columns) |items| for (@constCast(items)) |*item| try canonicalizeOwnedExpression(alloc, &item.expression);
    if (schema.checks) |items| for (@constCast(items)) |*item| {
        if (item.expression) |*expression| try canonicalizeOwnedExpression(alloc, expression);
        if (item.column) |column| {
            if (item.value) |value| item.value = try columns.canonicalValue(alloc, column, value);
        }
    };
    if (schema.relational_indexes) |indexes| for (@constCast(indexes)) |*index| {
        for (@constCast(index.keys)) |*key| if (key.expression) |*expression| try canonicalizeOwnedExpression(alloc, expression);
        if (index.where) |conditions| for (@constCast(conditions)) |*condition| {
            if (condition.value) |value| condition.value = try columns.canonicalValue(alloc, condition.column, value);
        };
    };
}

/// The source belongs to a pinned schema. Clone only expression nodes whose
/// slices would otherwise alias it; strings and immutable literals may borrow.
pub fn cloneCanonicalExpression(alloc: std.mem.Allocator, source: wire.RelationalScalarExpression) !wire.RelationalScalarExpression {
    var result = source;
    if (source.args) |args| {
        const children = try alloc.alloc(wire.RelationalScalarExpression, args.len);
        for (args, children) |child, *copy| copy.* = try cloneCanonicalExpression(alloc, child);
        result.args = children;
    }
    // Children were already canonicalized while copying; avoid revisiting
    // their subtrees at every ancestor.
    const children = result.args;
    result.args = null;
    try canonicalizeOwnedExpression(alloc, &result);
    result.args = children;
    return result;
}

test "relational declarations canonical public expression copy preserves borrowed epoch nodes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const source: wire.RelationalScalarExpression = .{ .op = .add, .args = &.{
        .{ .op = .literal, .type = .integer, .value = .{ .number_string = "9007199254740993" } },
        .{ .op = .literal, .type = .integer, .value = .{ .number_string = "1" } },
    } };
    const copy = try cloneCanonicalExpression(arena.allocator(), source);
    try std.testing.expectEqualStrings("9007199254740993", source.args.?[0].value.?.number_string);
    try std.testing.expectEqualStrings("9007199254740993", copy.args.?[0].value.?.string);
    try std.testing.expectEqualStrings("1", copy.args.?[1].value.?.number_string);
}
