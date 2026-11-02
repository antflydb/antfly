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

//! Owned lowering of SQL domain declarations into durable native programs.
//! The metadata binder supplies named-base authority; no catalog is consulted
//! during expression execution and preparation publishes nothing.
const std = @import("std");
const ast = @import("ast.zig");
const scalar = @import("scalar.zig");
const lowering = @import("schema_expression.zig");
const domains = @import("../system_catalog/domain_types.zig");
const values = @import("../schema/domain_value.zig");
const names = @import("../system_catalog/constraint_names.zig");
const A = std.mem.Allocator;

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    definition: domains.Definition,

    pub fn init(a: A, name: []const u8, declaration: @FieldType(ast.DomainDefinition, "create")) !Prepared {
        if (declaration.base.named_type != null) return error.SqlDomainBaseRequiresRegistry;
        if (declaration.base.type == .array or declaration.base.type == .datetime or declaration.base.type == .enumeration) return error.UnsupportedSqlExecution;
        const base: @import("../common/sql_builtin_type.zig").Type = declaration.base.element_type orelse switch (declaration.base.type) {
            .string => .text,
            .uuid => .uuid,
            .integer => .int64,
            .number => .float64,
            .boolean => .boolean,
            .json => .jsonb,
            else => return error.UnsupportedSqlExecution,
        };
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const region = arena.allocator();
        const column: scalar.Column = .{
            .name = "value",
            .type = declaration.base.type,
            .element_type = declaration.base.element_type,
            .numeric_modifier = declaration.base.numeric_modifier,
            .nullable = true,
        };
        var definition: domains.Definition = .{
            .base = .{ .builtin = base },
            .not_null = !declaration.base.nullable,
            .numeric_modifier = declaration.base.numeric_modifier,
        };
        if (declaration.base.default_expression) |expression| {
            const lowered = try lowering.lowerAssignment(region, .null, expression, declaration.base, false);
            definition.default_expression = try std.json.Stringify.valueAlloc(region, lowered, .{});
        }
        const checks = try region.alloc(domains.Check, declaration.checks.len);
        var occupied: std.StringHashMapUnmanaged(void) = .empty;
        // Domain constraint names are allocated in declaration order.
        // Unlike table schema admission, a later explicit name may therefore
        // conflict with an already allocated anonymous CHECK (PostgreSQL 18).
        const candidate = try names.Candidate.initKind(name, &.{}, .check);
        for (declaration.checks, checks) |check, *output| {
            var label: []const u8 = undefined;
            if (check.name) |explicit| {
                if ((try occupied.getOrPut(region, explicit)).found_existing) return error.SqlDuplicateConstraint;
                label = try region.dupe(u8, explicit);
            } else {
                var attempt: u32 = 0;
                while (true) : (attempt += 1) {
                    if (attempt > domains.max_checks) return error.SqlProgramLimitExceeded;
                    var buffer: [names.max_name_bytes]u8 = undefined;
                    const current = try candidate.format(attempt, &buffer);
                    if (occupied.contains(current)) continue;
                    label = try region.dupe(u8, current);
                    try occupied.put(region, label, {});
                    break;
                }
            }
            const lowered = try lowering.lowerColumns(region, &.{column}, check.expression, .boolean);
            output.* = .{
                .name = label,
                .expression = try std.json.Stringify.valueAlloc(region, lowered.expression, .{}),
                .validated = !check.not_valid,
            };
        }
        definition.checks = checks;
        try definition.validate(region);
        // Structural IR is not sufficient: validate every type, opcode and
        // dependency with the same VM that will enforce prepared writes.
        var executable = try values.View.init(a, definition);
        executable.deinit();
        return .{ .arena = arena, .definition = definition };
    }
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

test "SQL domain original declarations lower VALUE defaults and numeric checks into the shared native VM" {
    const a = std.testing.allocator;
    const compiler = @import("compiler.zig");
    var corpus = try @import("parity_fixtures.zig").Corpus.init(a);
    defer corpus.deinit();
    var numeric_sql = try compiler.compile(a, (try corpus.get("sql-0306")).sql, .{});
    defer numeric_sql.deinit();
    const ddl = numeric_sql.statement.catalog_ddl;
    var prepared = try Prepared.init(a, ddl.name.table, ddl.domain_definition.?.create);
    defer prepared.deinit();
    try std.testing.expectEqualStrings("positive_amount_check", prepared.definition.checks[0].name);
    var view = try values.View.init(a, prepared.definition);
    defer view.deinit();
    const vm = @import("../schema/relational_expression.zig");
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var bytes: usize = vm.max_allocated_bytes;
    var execution = vm.Execution.init(scratch.allocator(), &bytes);
    const positive = try vm.numericJson(&execution, .{ .string = "9007199254740993.001" });
    const negative = try vm.numericJson(&execution, .{ .string = "-0.00000000000001" });
    try std.testing.expectEqual(@as(?values.Violation, null), try view.validate(&execution, positive, .assignment));
    try std.testing.expectEqualStrings("positive_amount_check", (try view.validate(&execution, negative, .assignment)).?.constraint.?);
    try std.testing.expectEqual(@as(?values.Violation, null), try view.validate(&execution, .null, .assignment));
    var text_sql = try compiler.compile(a, (try corpus.get("sql-0307")).sql, .{});
    defer text_sql.deinit();
    var text = try Prepared.init(a, text_sql.statement.catalog_ddl.name.table, text_sql.statement.catalog_ddl.domain_definition.?.create);
    defer text.deinit();
    var text_view = try values.View.init(a, text.definition);
    defer text_view.deinit();
    try std.testing.expectEqualStrings("queued", (try text_view.defaultValue(&execution)).?.string);
    try std.testing.expectEqual(.not_null, (try text_view.validate(&execution, .null, .assignment)).?.kind);
}

test "SQL domain lowered constraints allocate names in declaration order and reject unrelated VALUE inputs" {
    const a = std.testing.allocator;
    const compiler = @import("compiler.zig");
    var compiled = try compiler.compile(a, "CREATE DOMAIN state AS text CONSTRAINT state_check CHECK (VALUE = 'queued') CHECK (VALUE IS NOT NULL) CHECK (VALUE <> 'bad')", .{});
    defer compiled.deinit();
    var prepared = try Prepared.init(a, "state", compiled.statement.catalog_ddl.domain_definition.?.create);
    defer prepared.deinit();
    try std.testing.expectEqualStrings("state_check1", prepared.definition.checks[1].name);
    try std.testing.expectEqualStrings("state_check", prepared.definition.checks[0].name);
    try std.testing.expectEqualStrings("state_check2", prepared.definition.checks[2].name);
    var collision = try compiler.compile(a, "CREATE DOMAIN state AS text CHECK (VALUE IS NOT NULL) CONSTRAINT state_check CHECK (VALUE = 'queued')", .{});
    defer collision.deinit();
    try std.testing.expectError(error.SqlDuplicateConstraint, Prepared.init(a, "state", collision.statement.catalog_ddl.domain_definition.?.create));
    var invalid = try compiler.compile(a, "CREATE DOMAIN state AS text CHECK (other IS NOT NULL)", .{});
    defer invalid.deinit();
    try std.testing.expectError(error.UnknownColumn, Prepared.init(a, "state", invalid.statement.catalog_ddl.domain_definition.?.create));
}

test "SQL domain complete declaration lowering unwinds every allocation failure" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compile(a, "CREATE DOMAIN status_text AS text DEFAULT 'queued' NOT NULL CHECK (VALUE IN ('queued','done'))", .{});
    defer compiled.deinit();
    const Harness = struct {
        fn run(alloc: A, declaration: @FieldType(ast.DomainDefinition, "create")) !void {
            var prepared = try Prepared.init(alloc, "status_text", declaration);
            defer prepared.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{compiled.statement.catalog_ddl.domain_definition.?.create});
}
