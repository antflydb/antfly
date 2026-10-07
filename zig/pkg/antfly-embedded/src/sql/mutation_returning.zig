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
        column.* = .{ .name = projection.alias orelse if (projection.expression == null) field else "?column?", .type = program.output_type.kind orelse .string, .element_type = program.output_type.element_type, .untyped_null = program.output_type.kind == null };
    }
    return .{ .columns = columns, .programs = programs };
}
