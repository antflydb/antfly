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

//! Stored schema programs have builtin-only dependencies today. Validate their
//! casts at the statement cut rather than silently persisting a different
//! conversion or retaining a request-owned callable pointer.
const std = @import("std");
const local = @import("antfly_local_sources");
const ast = local.sql_ast;
const catalog = local.sql_catalog;
const scalar = local.sql_scalar;
const Adapter = @import("sql_execution.zig").Adapter;
const A = std.mem.Allocator;
fn check(root: *Adapter, a: A, columns: []const scalar.Column, expression: *const ast.Scalar, expected: ?scalar.Type, assignment: bool) !void {
    _ = try scalar.inferTypedParametersExpected(a, expression, columns, &.{}, expected, .{ .routines = @import("sql_routine_execution.zig").persistedResolver(root), .assignment = assignment });
}
fn column(root: *Adapter, a: A, columns: []const scalar.Column, value: ast.Column) !void {
    const target: scalar.Type = .{ .kind = value.type, .element_type = value.element_type, .numeric_modifier = value.numeric_modifier, .enum_view = value.enum_view };
    if (value.default_expression) |expression| try check(root, a, &.{}, expression, target, true);
    if (value.generated_expression) |expression| try check(root, a, columns, expression, target, true);
}
fn hasExpressions(change: ast.SchemaChange) bool {
    return switch (change) {
        .set_default, .set_type, .add_check => true,
        .create_index => |index| expressions: {
            if (index.predicate != null) break :expressions true;
            for (index.keys) |key| if (key.expression != null) break :expressions true;
            break :expressions false;
        },
        .add_column => |value| value.default_expression != null or value.generated_expression != null,
        else => false,
    };
}
fn schemaChangeTarget(ddl: ast.CatalogDdl) !ast.Name {
    return if (ddl.kind == .index and ddl.action == .create) ddl.index_table orelse error.InvalidSqlSyntax else ddl.name;
}
pub fn validate(raw: *anyopaque, backing: A, statement: ast.Statement) !void {
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    const root: *Adapter = @ptrCast(@alignCast(raw));
    if (!root.server.source.vtable.supports_sql_casts) return;
    switch (statement) {
        .create_table => |create| {
            const needed = for (create.columns) |value| {
                if (value.default_expression != null or value.generated_expression != null) break true;
            } else for (create.constraints) |constraint| {
                if (constraint == .add_check) break true;
            } else false;
            if (!needed) return;
            const columns = try a.alloc(scalar.Column, create.columns.len);
            for (create.columns, columns) |declared, *out| out.* = .{ .name = declared.name, .type = declared.type, .element_type = declared.element_type, .numeric_modifier = declared.numeric_modifier, .enum_view = declared.enum_view };
            for (create.columns) |value| try column(root, a, columns, value);
            for (create.constraints) |constraint| if (constraint == .add_check) try check(root, a, columns, constraint.add_check.expression, .{ .kind = .boolean }, false);
        },
        .catalog_ddl => |ddl| {
            const change = ddl.schema_change orelse return;
            if (!hasExpressions(change)) return;
            const backend = root.backend();
            const table = try backend.vtable.resolve(backend.ptr, a, try schemaChangeTarget(ddl), .admin);
            var columns: std.ArrayList(scalar.Column) = .empty;
            for (table.columns) |value| try columns.append(a, .{ .name = value.name, .type = value.type, .element_type = value.element_type, .numeric_modifier = value.numeric_modifier, .temporal_kind = value.temporal_kind });
            switch (change) {
                .add_column => |value| {
                    try columns.append(a, .{ .name = value.name, .type = value.type, .element_type = value.element_type, .numeric_modifier = value.numeric_modifier });
                    try column(root, a, columns.items, value);
                },
                .set_default => |value| try check(root, a, &.{}, value.expression, scalar.columnType(try table.column(value.column)), true),
                .set_type => |value| try check(root, a, columns.items, value.using, .{ .kind = value.column.type, .element_type = value.column.element_type, .numeric_modifier = value.column.numeric_modifier }, true),
                .add_check => |value| try check(root, a, columns.items, value.expression, .{ .kind = .boolean }, false),
                .create_index => |value| {
                    for (value.keys) |key| if (key.expression) |expression| try check(root, a, columns.items, expression, null, false);
                    if (value.predicate) |expression| try check(root, a, columns.items, expression, .{ .kind = .boolean }, false);
                },
                else => unreachable,
            }
        },
        .policy_ddl => |policy| {
            if (policy.using == null and policy.with_check == null) return;
            const backend = root.backend();
            const table = try backend.vtable.resolve(backend.ptr, a, policy.table, .write);
            const columns = try a.alloc(scalar.Column, table.columns.len);
            for (table.columns, columns) |value, *out| out.* = .{ .name = value.name, .type = value.type, .element_type = value.element_type, .numeric_modifier = value.numeric_modifier, .temporal_kind = value.temporal_kind };
            if (policy.using) |expression| try check(root, a, columns, expression, .{ .kind = .boolean }, false);
            if (policy.with_check) |expression| try check(root, a, columns, expression, .{ .kind = .boolean }, false);
        },
        else => {},
    }
}

test "SQL persisted index validation binds the owning table and skips column-only indexes" {
    const a = std.testing.allocator;
    const cases = [_]struct { sql: []const u8, expressions: bool }{
        .{ .sql = "CREATE INDEX by_status ON public.usage_records(status)", .expressions = false },
        .{ .sql = "CREATE INDEX by_status ON public.usage_records(lower(status))", .expressions = true },
        .{ .sql = "CREATE INDEX by_status ON public.usage_records(status) WHERE status='pending'", .expressions = true },
    };
    for (cases) |input| {
        var compiled = try local.sql_compiler.compile(a, input.sql, .{});
        defer compiled.deinit();
        const ddl = compiled.statement.catalog_ddl;
        const target = try schemaChangeTarget(ddl);
        try std.testing.expectEqualStrings("usage_records", target.table);
        try std.testing.expectEqualStrings("public", target.namespace.?);
        try std.testing.expectEqual(input.expressions, hasExpressions(ddl.schema_change.?));
    }
}
