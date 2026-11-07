// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared table-trigger syntax and deterministic dispatch shape. Trigger names
//! belong to an immutable table identity, not the routine overload namespace.
const std = @import("std");
const ast = @import("ast.zig");

pub const Timing = enum { before, after };
pub const Level = enum { statement, row };
pub const Event = enum { insert, update, delete, truncate };
pub const Events = packed struct(u8) {
    insert: bool = false,
    update: bool = false,
    delete: bool = false,
    truncate: bool = false,
    reserved: u4 = 0,

    pub fn contains(self: Events, event: Event) bool {
        return switch (event) {
            .insert => self.insert,
            .update => self.update,
            .delete => self.delete,
            .truncate => self.truncate,
        };
    }
    pub fn validate(self: Events, level: Level) !void {
        if (self.reserved != 0 or (!self.insert and !self.update and !self.delete and !self.truncate) or
            (self.truncate and level == .row)) return error.InvalidSqlTriggerDefinition;
    }
};

/// Compiler-owned request. EXECUTE FUNCTION arguments are trigger string
/// arguments (TG_ARGV), never declared SQL function arguments. PostgreSQL's
/// omitted FOR EACH clause means STATEMENT, including BEFORE UPDATE.
pub const Syntax = struct {
    name: []const u8,
    table: ast.Name,
    operation: union(enum) {
        create: struct {
            replace: bool = false,
            timing: Timing,
            events: Events,
            level: Level = .statement,
            update_columns: []const []const u8 = &.{},
            function: ast.Name,
            arguments: []const []const u8 = &.{},
            predicate: ?*const ast.Scalar = null,
            predicate_sql: ?[]const u8 = null,
        },
        drop: struct { if_exists: bool = false, cascade: bool = false },
    },

    pub fn validate(self: Syntax) !void {
        if (!validName(self.name)) return error.InvalidSqlTriggerDefinition;
        switch (self.operation) {
            .drop => {},
            .create => |create| {
                try create.events.validate(create.level);
                if (create.arguments.len > 32 or create.update_columns.len > 256 or
                    (create.update_columns.len != 0 and !create.events.update)) return error.InvalidSqlTriggerDefinition;
                var argument_bytes: usize = 0;
                for (create.arguments) |value| {
                    if (!std.unicode.utf8ValidateSlice(value) or std.mem.indexOfScalar(u8, value, 0) != null) return error.InvalidSqlTriggerDefinition;
                    argument_bytes = std.math.add(usize, argument_bytes, value.len) catch return error.InvalidSqlTriggerDefinition;
                }
                if (argument_bytes > 64 * 1024) return error.SqlProgramLimitExceeded;
                for (create.update_columns, 0..) |name, i| {
                    if (!validName(name)) return error.InvalidSqlTriggerDefinition;
                    for (create.update_columns[0..i]) |previous| if (std.mem.eql(u8, name, previous)) return error.DuplicateSqlColumn;
                }
            },
        }
    }
};

pub fn validName(value: []const u8) bool {
    return value.len != 0 and value.len <= 256 and std.unicode.utf8ValidateSlice(value) and std.mem.indexOfScalar(u8, value, 0) == null;
}

pub fn nameLess(_: void, left: []const u8, right: []const u8) bool {
    return std.mem.order(u8, left, right) == .lt;
}

test "SQL table trigger syntax preserves default statement level and event domains" {
    const value: Syntax = .{ .name = "touch", .table = .{ .table = "rows" }, .operation = .{ .create = .{ .timing = .before, .events = .{ .update = true }, .function = .{ .table = "touch_updated_at" }, .arguments = &.{"updated_at_ns"} } } };
    try value.validate();
    try std.testing.expectEqual(Level.statement, value.operation.create.level);
    try std.testing.expect(value.operation.create.events.contains(.update));
    try std.testing.expect(!value.operation.create.events.contains(.insert));
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, (Events{ .truncate = true }).validate(.row));
    try std.testing.expectError(error.InvalidSqlTriggerDefinition, (Events{}).validate(.statement));
    var invalid = value;
    invalid.operation.create.update_columns = &.{ "x", "x" };
    try std.testing.expectError(error.DuplicateSqlColumn, invalid.validate());
}

/// Scope qualification uses the ordinary scalar AST, never substring matching.
pub fn referencesImage(expression: *const ast.Scalar, image: []const u8) bool {
    return switch (expression.*) {
        .literal => false,
        .column => |name| std.mem.startsWith(u8, name, image) and name.len > image.len and name[image.len] == 0,
        .unary => |part| referencesImage(part.operand, image),
        .binary => |part| referencesImage(part.left, image) or referencesImage(part.right, image),
        .cast => |part| referencesImage(part.operand, image),
        .call => |part| block: {
            for (part.args) |arg| if (referencesImage(arg, image)) break :block true;
            break :block false;
        },
        .case_when => |part| block: {
            for (part.branches) |branch| if (referencesImage(branch.condition, image) or referencesImage(branch.value, image)) break :block true;
            break :block if (part.otherwise) |other| referencesImage(other, image) else false;
        },
        .in_list => |part| block: {
            if (referencesImage(part.operand, image)) break :block true;
            for (part.values) |value| if (referencesImage(value, image)) break :block true;
            break :block false;
        },
    };
}
