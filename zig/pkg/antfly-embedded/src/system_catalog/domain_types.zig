// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable domain contracts. Expressions are owned durable native IR, never
//! SQL text resolved again on a row path or copied onto dependent columns.
const std = @import("std");
const builtin = @import("../common/sql_builtin_type.zig");
const A = std.mem.Allocator;
pub const max_checks = 128;
pub const max_expression_bytes = 64 * 1024;
pub const max_definition_bytes = 1024 * 1024;

pub const Reference = struct {
    id: u64,
    origin_id: [16]u8,
    generation: u32,
    pub fn validate(self: @This()) !void {
        if (self.id == 0 or self.generation == 0 or std.mem.allEqual(u8, &self.origin_id, 0)) return error.InvalidSqlNamedType;
    }
};
pub const Base = union(enum) { builtin: builtin.Type, named: Reference };
pub const Check = struct {
    name: []const u8,
    expression: []const u8,
    /// NOT VALID still checks every new assignment. This flag describes only
    /// whether the historical dependent cohort has completed validation.
    validated: bool = true,
};
pub const Definition = struct {
    base: Base,
    numeric_modifier: ?builtin.NumericModifier = null,
    default_expression: ?[]const u8 = null,
    not_null: bool = false,
    checks: []const Check = &.{},

    pub fn validate(self: Definition, a: A) !void {
        switch (self.base) {
            .builtin => |kind| if (self.numeric_modifier != null and kind != .numeric) return error.InvalidSqlNamedType,
            .named => |reference| {
                try reference.validate();
                if (self.numeric_modifier != null) return error.InvalidSqlNamedType;
            },
        }
        if (self.numeric_modifier) |modifier| try modifier.validate();
        if (self.checks.len > max_checks) return error.SqlProgramLimitExceeded;
        var total: usize = 0;
        if (self.default_expression) |expression| {
            try validateExpression(a, expression);
            total += expression.len;
        }
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        for (self.checks) |check| {
            if (check.name.len == 0 or check.name.len > 63 or !std.unicode.utf8ValidateSlice(check.name) or std.mem.indexOfScalar(u8, check.name, 0) != null) return error.InvalidSqlNamedType;
            if ((try names.getOrPut(a, check.name)).found_existing) return error.SqlDuplicateConstraint;
            try validateExpression(a, check.expression);
            total +|= check.expression.len + check.name.len;
            if (total > max_definition_bytes) return error.SqlProgramLimitExceeded;
        }
    }

    pub fn cloneOwned(self: Definition, a: A) !Definition {
        const checks = try a.alloc(Check, self.checks.len);
        var initialized: usize = 0;
        errdefer {
            for (checks[0..initialized]) |check| {
                a.free(check.name);
                a.free(check.expression);
            }
            a.free(checks);
        }
        for (self.checks, checks) |source, *check| {
            const name = try a.dupe(u8, source.name);
            errdefer a.free(name);
            check.* = .{ .name = name, .expression = try a.dupe(u8, source.expression), .validated = source.validated };
            initialized += 1;
        }
        var result = self;
        result.checks = checks;
        result.default_expression = if (self.default_expression) |expression| try a.dupe(u8, expression) else null;
        return result;
    }
    pub fn deinitOwned(self: *Definition, a: A) void {
        for (self.checks) |check| {
            a.free(check.name);
            a.free(check.expression);
        }
        a.free(self.checks);
        if (self.default_expression) |expression| a.free(expression);
        self.* = undefined;
    }
};

fn validateExpression(a: A, bytes: []const u8) !void {
    if (bytes.len == 0 or bytes.len > max_expression_bytes) return error.SqlProgramLimitExceeded;
    var expression = std.json.parseFromSlice(std.json.Value, a, bytes, .{ .parse_numbers = false, .max_value_len = max_expression_bytes }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidSqlNamedType;
    defer expression.deinit();
    if (expression.value != .object) return error.InvalidSqlNamedType;
    // Executable opcode/type/dependency checks belong to the shared native VM
    // when a registry is prepared. Structural decoding grants no authority.
    if (expression.value.object.get("op") == null) return error.InvalidSqlNamedType;
}

test "SQL domain definitions own immutable defaults constraints and exact numeric programs" {
    const a = std.testing.allocator;
    const definition: Definition = .{
        .base = .{ .builtin = .numeric },
        .not_null = true,
        .default_expression =
        \\{"op":"literal","type":"numeric","value":"9007199254740993.001"}
        ,
        .checks = &.{.{ .name = "positive", .expression =
        \\{"op":"gt","args":[{"op":"column","column":"value"},{"op":"literal","type":"numeric","value":"0"}]}
        , .validated = false }},
    };
    try definition.validate(a);
    var owned = try definition.cloneOwned(a);
    defer owned.deinitOwned(a);
    try std.testing.expectEqualDeep(definition, owned);
    try std.testing.expect(owned.checks.ptr != definition.checks.ptr);
    try std.testing.expect(owned.checks[0].expression.ptr != definition.checks[0].expression.ptr);
    var duplicate = definition;
    duplicate.checks = &.{ definition.checks[0], definition.checks[0] };
    try std.testing.expectError(error.SqlDuplicateConstraint, duplicate.validate(a));
    var invalid = definition;
    invalid.base = .{ .named = .{ .id = 1, .origin_id = @splat(0), .generation = 1 } };
    try std.testing.expectError(error.InvalidSqlNamedType, invalid.validate(a));
}

test "SQL domain immutable ownership unwinds every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            const definition: Definition = .{ .base = .{ .builtin = .text }, .default_expression =
                \\{"op":"literal","type":"string","value":"queued"}
            , .checks = &.{.{ .name = "present", .expression =
                \\{"op":"is_not_null","args":[{"op":"column","column":"value"}]}
            }} };
            try definition.validate(a);
            var copy = try definition.cloneOwned(a);
            defer copy.deinitOwned(a);
            try copy.validate(a);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}
