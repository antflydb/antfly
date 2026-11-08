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

//! Own and forward scalar observer obligations through proved identity frames.
const std = @import("std");
const catalog = @import("catalog.zig");
const A = std.mem.Allocator;
pub const Forward = struct {
    kind: ?@import("cypher").runtime.NodeCursor.ScalarObserver = null,
    field: ?[]const u8 = null,
    keys: []const []const u8 = &.{},
    filter: ?catalog.Scan.GraphFilter = null,
    reducer_filter: ?catalog.Scan.GraphFilter = null,
};
fn name(input: []const u8, from: []const []const u8, to: []const []const u8) ?[]const u8 {
    for (from, to) |old, new| if (std.mem.eql(u8, input, old)) return new;
    return null;
}
pub fn mapFilter(a: A, input: catalog.Scan.GraphFilter, from: []const []const u8, to: []const []const u8) anyerror!?catalog.Scan.GraphFilter {
    return switch (input) {
        .comparison => |condition| blk: {
            const mapped = name(condition.column, from, to) orelse return null;
            const datum = try @import("operators.zig").cloneDatum(a, condition.datum());
            break :blk .{ .comparison = .{ .column = try a.dupe(u8, mapped), .op = condition.op, .value = datum.value, .operand = datum } };
        },
        .all, .any => |children| blk: {
            const mapped = try a.alloc(catalog.Scan.GraphFilter, children.len);
            for (children, mapped) |child, *out| out.* = (try mapFilter(a, child, from, to)) orelse return null;
            break :blk if (input == .all) .{ .all = mapped } else .{ .any = mapped };
        },
    };
}
pub fn forward(a: A, request: catalog.AggregateWeight, from: []const []const u8, to: []const []const u8) !Forward {
    const kind = request.observer orelse return .{};
    const source = request.observer_field orelse if (request.fields.len == 1) request.fields[0] else return .{};
    const field = name(source, from, to) orelse return .{};
    const keys = try a.alloc([]const u8, request.observer_keys.len);
    for (request.observer_keys, keys) |key, *out| out.* = try a.dupe(u8, name(key, from, to) orelse return .{});
    return .{ .kind = kind, .field = try a.dupe(u8, field), .keys = keys, .reducer_filter = if (request.observer_reducer_filter) |predicate| (try mapFilter(a, predicate, from, to)) orelse return .{} else null, .filter = if (request.observer_filter) |predicate| (try mapFilter(a, predicate, from, to)) orelse return .{} else null };
}

/// Translate the complete typed predicate of a row-preserving frame. Closed
/// operands bind per invocation; speculative evaluation never demands errors.
pub fn fromProgram(a: A, program: @import("scalar.zig").Program, columns: []const @import("scalar.zig").Column, parameters: []const std.json.Value) !?catalog.Scan.GraphFilter {
    var remaining: usize = 128;
    return predicateAt(a, program, columns, parameters, program.root, &remaining);
}
fn closedOperand(program: @import("scalar.zig").Program, index: u32, remaining: *usize) bool {
    if (remaining.* == 0 or index >= program.instructions.len) return false;
    remaining.* -= 1;
    return switch (program.instructions[index].operation) {
        .literal, .parameter => true,
        .cast => |cast| closedOperand(program, cast.operand, remaining),
        .unary => |unary| closedOperand(program, unary.operand, remaining),
        .binary => |operation| closedOperand(program, operation.left, remaining) and closedOperand(program, operation.right, remaining),
        else => false,
    };
}
fn predicateAt(a: A, program: @import("scalar.zig").Program, columns: []const @import("scalar.zig").Column, parameters: []const std.json.Value, index: u32, remaining: *usize) anyerror!?catalog.Scan.GraphFilter {
    if (remaining.* == 0 or index >= program.instructions.len) return null;
    remaining.* -= 1;
    const operation = program.instructions[index].operation;
    // Empty conjunction/disjunction retain TRUE/FALSE through every identity
    // frame. Evaluate only closed, repeatable scalar operands, per invocation.
    var closed_budget = remaining.*;
    if (closedOperand(program, index, &closed_budget)) {
        const value = program.evaluateInstruction(a, index, parameters) catch |err| {
            if (err == error.InvalidSqlParameters or err == error.UnsupportedSqlShape or err == error.SqlProgramLimitExceeded or std.mem.startsWith(u8, @import("errors.zig").describe(err).code, "22")) return null;
            return err;
        };
        if (value.sql_null) return .{ .any = &.{} };
        if (value.value == .bool) return if (value.value.bool) .{ .all = &.{} } else .{ .any = &.{} };
        return null;
    }
    if (operation == .binary and (operation.binary.op == .@"and" or operation.binary.op == .@"or")) {
        const left = (try predicateAt(a, program, columns, parameters, operation.binary.left, remaining)) orelse return null;
        const right = (try predicateAt(a, program, columns, parameters, operation.binary.right, remaining)) orelse return null;
        // Admission observes SQL TRUE only. UNKNOWN and FALSE therefore share
        // the empty domain under these monotone AND/OR operators.
        const conjunction = operation.binary.op == .@"and";
        if (left == .all and left.all.len == 0) return if (conjunction) right else left;
        if (right == .all and right.all.len == 0) return if (conjunction) left else right;
        if (left == .any and left.any.len == 0) return if (conjunction) left else right;
        if (right == .any and right.any.len == 0) return if (conjunction) right else left;
        const children = try a.dupe(catalog.Scan.GraphFilter, &.{ left, right });
        return if (operation.binary.op == .@"and") .{ .all = children } else .{ .any = children };
    }
    var column_index: u32 = undefined;
    var operand_index: ?u32 = null;
    var op: catalog.Condition.Op = undefined;
    if (operation == .unary) {
        if (operation.unary.op != .is_null and operation.unary.op != .is_not_null) return null;
        column_index = operation.unary.operand;
        op = if (operation.unary.op == .is_null) .is_null else .is_not_null;
    } else if (operation == .binary) {
        op = switch (operation.binary.op) {
            .eq => .eq,
            .neq => .neq,
            .lt => .lt,
            .lte => .lte,
            .gt => .gt,
            .gte => .gte,
            else => return null,
        };
        column_index = operation.binary.left;
        operand_index = operation.binary.right;
        if (program.instructions[column_index].operation != .column) {
            column_index = operation.binary.right;
            operand_index = operation.binary.left;
            op = switch (op) {
                .lt => .gt,
                .lte => .gte,
                .gt => .lt,
                .gte => .lte,
                else => op,
            };
        }
    } else return null;
    const column = program.instructions[column_index];
    if (column.operation != .column or column.operation.column >= columns.len or column.input_function) return null;
    const definition = columns[column.operation.column];
    if (definition.type != .integer and definition.type != .string and definition.type != .boolean) return null;
    if (definition.element_type == .numeric or std.mem.eql(u8, definition.name, "_id")) return null;
    const datum = if (operand_index) |input| blk: {
        // Closed coercions bind once in this invocation. A failed speculative
        // conversion declines; it never demands an error on an unvisited row.
        if (!closedOperand(program, input, remaining)) return null;
        break :blk program.evaluateInstruction(a, input, parameters) catch |err| {
            if (err == error.InvalidSqlParameters or err == error.UnsupportedSqlShape or err == error.SqlProgramLimitExceeded or std.mem.startsWith(u8, @import("errors.zig").describe(err).code, "22")) return null;
            return err;
        };
    } else @import("scalar.zig").Datum{};
    return .{ .comparison = .{ .column = try a.dupe(u8, definition.name), .op = op, .value = datum.value, .operand = try @import("operators.zig").cloneDatum(a, datum) } };
}
