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

//! RETURNING binds against an authorized source scope, while programs consume
//! typed execution slots. Mutation preparation supplies the final target image;
//! the same programs retain source identity without a second physical read.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const relation = @import("relation_binding.zig");
const describe = @import("describe.zig");

pub const Plan = struct {
    columns: []const describe.Column,
    programs: []const scalar.Program,
};

pub fn bind(a: std.mem.Allocator, backend: catalog.Backend, scope: []const relation.Column, slots: []const scalar.Column, requested: []const ast.Projection, target: []const u8, parameters: []?ast.ColumnType) !Plan {
    const projections = try relation.expandWildcards(a, scope, if (requested.len == 0) &[_]ast.Projection{.{ .wildcard = true }} else requested, target);
    const expressions = try a.alloc(*const ast.Scalar, projections.len);
    for (projections, expressions) |projection, *out| {
        const expression = projection.expression orelse blk: {
            const node = try a.create(ast.Scalar);
            node.* = .{ .column = if (projection.bound_column) |ordinal| scope[ordinal].internal else projection.field };
            break :blk node;
        };
        out.* = if (projection.bound_column != null) expression else try relation.lowerBoundExpression(a, scope, expression);
    }
    for (expressions) |expression| _ = try scalar.inferParameters(a, expression, slots, parameters, null, .{ .invocation = backend.parameter_invocation });
    const programs = try a.alloc(scalar.Program, projections.len);
    const columns = try a.alloc(describe.Column, projections.len);
    for (expressions, projections, programs, columns) |expression, projection, *program, *column| {
        program.* = try scalar.bindWithSettings(a, expression, slots, parameters, .{ .invocation = backend.parameter_invocation }, backend.settings_view);
        const field = if (std.mem.lastIndexOfScalar(u8, projection.field, 0)) |index| projection.field[index + 1 ..] else projection.field;
        column.* = .{ .name = projection.alias orelse if (projection.expression == null) field else "?column?", .type = program.output_type.kind orelse .string, .element_type = program.output_type.element_type, .numeric_modifier = program.output_type.numeric_modifier, .enum_view = program.output_type.enum_view, .domain_view = scalar.domainViewOf(program.output_type), .temporal_kind = program.output_type.temporal_kind, .untyped_null = program.output_type.kind == null };
    }
    return .{ .columns = columns, .programs = programs };
}

test "SQL RETURNING dense integer slots preserve arithmetic errors across PostgreSQL widths" {
    const a = std.testing.allocator;
    var compiled = try @import("compiler.zig").compile(a, "SELECT v/(v-v)", .{});
    defer compiled.deinit();
    for ([_]@import("array_value.zig").ElementType{ .int16, .int32, .int64 }) |width| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const owned = arena.allocator();
        const scope = [_]relation.Column{
            .{ .name = "id", .internal = "target_id", .qualifier = "target", .type = .string, .nullable = false },
            .{ .name = "v", .internal = "target_v", .qualifier = "target", .type = .integer, .element_type = width, .nullable = true },
        };
        const slots = [_]scalar.Column{.{ .name = "target_v", .type = .integer, .element_type = width }};
        var token: u8 = 0;
        const backend: catalog.Backend = .{ .ptr = &token, .vtable = &.{ .resolve = undefined, .scan = undefined, .mutate = undefined, .checkpoint = undefined } };
        const plan = try bind(owned, backend, &scope, &slots, compiled.statement.select.columns, "target", &.{});
        const table: catalog.Table = .{ .id = 1, .physical_name = "target", .schema_version = 1, .columns = &.{.{ .name = "v", .path = "v", .type = .integer, .element_type = width, .nullable = true }} };
        const images = try @import("runtime.zig").Context.ReturningImages.init(owned, table, &.{"v"});
        for ([_]std.json.Value{ .{ .integer = 1 }, .{ .number_string = "1" }, .null }) |value_| {
            var object: std.json.ObjectMap = .empty;
            try object.put(owned, "v", value_);
            const mutation: catalog.Mutation = .{ .key = "row", .expected_version = 1, .row = .{ .object = object } };
            const row = try images.row(owned, mutation, mutation);
            const cell = try row.cell("v");
            const borrowed = try images.projection.adaptBorrowed(owned, images.layout, row);
            try std.testing.expect(borrowed.typed_cells.?.values.ptr == row.typed_cells.?.values.ptr);
            if (value_ == .null) {
                try std.testing.expect((try plan.programs[0].evaluate(owned, &.{cell}, &.{}, .{})).sql_null);
            } else try std.testing.expectError(error.SqlDivisionByZero, plan.programs[0].evaluate(owned, &.{cell}, &.{}, .{}));
        }
        if (width == .int64) {
            var exact = try @import("compiler.zig").compile(a, "SELECT (v+1)-v", .{});
            defer exact.deinit();
            const exact_plan = try bind(owned, backend, &scope, &slots, exact.statement.select.columns, "target", &.{});
            var object: std.json.ObjectMap = .empty;
            try object.put(owned, "v", .{ .number_string = "9007199254740993" });
            const mutation: catalog.Mutation = .{ .key = "row", .expected_version = 1, .row = .{ .object = object } };
            const row = try images.row(owned, mutation, mutation);
            const cell = try row.cell("v");
            try std.testing.expectEqual(@as(i64, 9007199254740993), cell.value.integer);
            try std.testing.expectEqual(@as(i64, 1), (try exact_plan.programs[0].evaluate(owned, &.{cell}, &.{}, .{})).value.integer);
        }
    }
}
