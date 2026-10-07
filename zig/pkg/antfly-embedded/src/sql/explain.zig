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

//! Dry-run plan description from the same authorized, immutable binding used
//! by execution. No storage cursor or mutation is opened here.
const std = @import("std");
const ast = @import("ast.zig");
const describe = @import("describe.zig");
const relation = @import("relation_binding.zig");
const catalog = @import("catalog.zig");
const Allocator = std.mem.Allocator;

fn displayName(alloc: Allocator, table: catalog.Table) ![]const u8 {
    if (table.scope) |scope| return std.fmt.allocPrint(alloc, "{s}.{s}.{s}", .{ scope.database, scope.namespace, scope.name });
    return table.physical_name;
}

const Plan = struct {
    node_type: []const u8,
    relation: ?[]const u8 = null,
    join_kind: ?[]const u8 = null,
    index: ?[]const u8 = null,
    source_format: ?[]const u8 = null,
    snapshot_mode: ?[]const u8 = null,
    table_id: ?u64 = null,
    schema_version: ?u32 = null,
    fields: ?[]const []const u8 = null,
    functions: ?[]const []const u8 = null,
    plans: []const Plan = &.{},
};

fn wrap(alloc: Allocator, name: []const u8, child: Plan) !Plan {
    return .{ .node_type = name, .plans = try alloc.dupe(Plan, &.{child}) };
}

fn decisionFunctions(alloc: Allocator, program: *const @import("scalar.zig").Program, functions: *std.ArrayList([]const u8)) !void {
    for (program.instructions) |instruction| if (instruction.operation == .call) {
        const name = @tagName(instruction.operation.call.function);
        if (@import("../functions/decisions.zig").descriptor(name) != null) try functions.append(alloc, name);
    };
}

fn boundDecisionFunctions(alloc: Allocator, bound: describe.BoundStatement, functions: *std.ArrayList([]const u8)) anyerror!void {
    try scalarDecisionFunctions(alloc, bound.scalars, functions);
    if (bound.aggregate) |aggregate| {
        try scalarDecisionFunctions(alloc, aggregate.input, functions);
        for (aggregate.outputs) |*program| try decisionFunctions(alloc, program, functions);
        for (aggregate.orders) |*program| try decisionFunctions(alloc, program, functions);
        if (aggregate.having) |*program| try decisionFunctions(alloc, program, functions);
    }
    if (bound.window) |window| {
        try boundDecisionFunctions(alloc, window.input.*, functions);
        for (window.outputs) |*program| try decisionFunctions(alloc, program, functions);
        for (window.orders) |*program| try decisionFunctions(alloc, program, functions);
    }
}
fn scalarDecisionFunctions(alloc: Allocator, bound: @import("bound_scalars.zig").Bound, functions: *std.ArrayList([]const u8)) !void {
    if (bound.predicate) |*program| try decisionFunctions(alloc, program, functions);
    for (bound.projections) |optional| if (optional) |*program| try decisionFunctions(alloc, program, functions);
    for (bound.orders) |optional| if (optional) |*program| try decisionFunctions(alloc, program, functions);
    for (bound.assignments) |optional| if (optional) |*program| try decisionFunctions(alloc, program, functions);
    for (bound.insert_rows) |row| for (row) |optional| if (optional) |*program| try decisionFunctions(alloc, program, functions);
}

/// Input relations describe their own nested query stages. Only collect the
/// outer input binding here, so a call already shown below a Query node is not
/// advertised as a second invocation at the mutation boundary.
fn inputDecisionFunctions(alloc: Allocator, bound: describe.BoundStatement, functions: *std.ArrayList([]const u8)) !void {
    if (bound.relation == null or bound.relation.?.root.operation != .query)
        try boundDecisionFunctions(alloc, bound, functions);
}

fn mutationDecisionFunctions(alloc: Allocator, bound: describe.BoundStatement, functions: *std.ArrayList([]const u8)) !void {
    try boundDecisionFunctions(alloc, bound, functions);
    if (bound.returning) |returning| try boundDecisionFunctions(alloc, returning.*, functions);
    if (bound.insert_source) |source| try inputDecisionFunctions(alloc, source.*, functions);
    if (bound.joined_mutation) |joined| try inputDecisionFunctions(alloc, joined.input.*, functions);
    if (bound.conflict) |conflict| {
        if (conflict.predicate) |*program| try decisionFunctions(alloc, program, functions);
        for (conflict.assignments) |optional| if (optional) |*program| try decisionFunctions(alloc, program, functions);
    }
    if (bound.merge_mutation) |merge| {
        try inputDecisionFunctions(alloc, merge.input.*, functions);
        for (merge.arms) |arm| {
            if (arm.predicate) |*program| try decisionFunctions(alloc, program, functions);
            switch (arm.action) {
                .insert, .update => |assignments| for (assignments) |assignment| {
                    if (assignment.program) |*program| try decisionFunctions(alloc, program, functions);
                },
                .delete, .nothing => {},
            }
        }
        if (merge.returning_plan) |returning| for (returning.programs) |*program| try decisionFunctions(alloc, program, functions);
    }
}

fn queryStages(alloc: Allocator, source: Plan, statement: ast.Select, bound: describe.BoundStatement) !Plan {
    var plan = source;
    var functions: std.ArrayList([]const u8) = .empty;
    try boundDecisionFunctions(alloc, bound, &functions);
    if (functions.items.len > 0) plan = .{ .node_type = "DecisionEval", .functions = functions.items, .plans = try alloc.dupe(Plan, &.{plan}) };
    if (bound.aggregate != null or statement.group_by.len != 0 or statement.having != null) plan = try wrap(alloc, "Aggregate", plan);
    if (bound.window != null) plan = try wrap(alloc, "Window", plan);
    if (statement.order_by.len != 0) plan = try wrap(alloc, "Order", plan);
    if (statement.limit != null or statement.offset != null) plan = try wrap(alloc, "Limit", plan);
    return plan;
}

fn relationPlan(alloc: Allocator, bound: *const relation.Bound, node: *const relation.Node, verbose: bool, depth: usize, remaining: *usize) !Plan {
    if (depth >= 64 or remaining.* == 0) return error.SqlLimitExceeded;
    remaining.* -= 1;
    return switch (node.operation) {
        .singleton => .{ .node_type = "Values" },
        .literal_rows => .{ .node_type = "Values" },
        .recursive_ref => .{ .node_type = "Recursive Reference" },
        .materialized_ref => |producer| .{ .node_type = "Materialized Reference", .plans = try alloc.dupe(Plan, &.{try relationPlan(alloc, bound, producer, verbose, depth + 1, remaining)}) },
        .scan => |scan| blk: {
            if (scan.index >= bound.scans.len) return error.InvalidSqlBackendResponse;
            const source = bound.scans[scan.index];
            break :blk .{
                .node_type = if (source.table.external_base_source != null) "Lake Scan" else if (source.request.index_equality != null) "Index Scan" else "Table Scan",
                .source_format = if (verbose) if (source.table.external_base_source) |lake| @tagName(lake.binding.format) else null else null,
                .snapshot_mode = if (verbose) if (source.table.external_base_source) |lake| @tagName(lake.binding.snapshot_mode) else null else null,
                .relation = try displayName(alloc, source.table),
                .index = if (source.request.index_equality) |index| index.name else null,
                .table_id = if (verbose) source.table.id else null,
                .schema_version = if (verbose) source.table.schema_version else null,
                .fields = if (verbose) source.request.fields else null,
            };
        },
        .join => |join| blk: {
            const source: Plan = .{
                .node_type = if (join.left_keys.len != 0 and join.right_keys.len != 0) "Hash Join" else "Join",
                .join_kind = @tagName(join.kind),
                .plans = try alloc.dupe(Plan, &.{ try relationPlan(alloc, bound, join.left, verbose, depth + 1, remaining), try relationPlan(alloc, bound, join.right, verbose, depth + 1, remaining) }),
            };
            var functions: std.ArrayList([]const u8) = .empty;
            if (join.condition) |*program| try decisionFunctions(alloc, program, &functions);
            for (join.left_keys) |*program| try decisionFunctions(alloc, program, &functions);
            for (join.right_keys) |*program| try decisionFunctions(alloc, program, &functions);
            if (functions.items.len == 0) break :blk source;
            break :blk .{ .node_type = "DecisionEval", .functions = functions.items, .plans = try alloc.dupe(Plan, &.{source}) };
        },
        .query => |query| .{ .node_type = "Query", .plans = try alloc.dupe(Plan, &.{try queryStages(alloc, try relationPlan(alloc, bound, query.source, verbose, depth + 1, remaining), query.statement, query.binding)}) },
        .set => |set| .{ .node_type = @tagName(set.kind), .plans = try alloc.dupe(Plan, &.{ try relationPlan(alloc, bound, set.left, verbose, depth + 1, remaining), try relationPlan(alloc, bound, set.right, verbose, depth + 1, remaining) }) },
        .values => |arms| blk: {
            const plans = try alloc.alloc(Plan, arms.len);
            for (arms, plans) |arm, *plan| plan.* = try relationPlan(alloc, bound, arm, verbose, depth + 1, remaining);
            break :blk .{ .node_type = "Values", .plans = plans };
        },
        .recursive => |recursive| .{ .node_type = if (recursive.all) "Recursive Union All" else "Recursive Union", .plans = try alloc.dupe(Plan, &.{ try relationPlan(alloc, bound, recursive.seed, verbose, depth + 1, remaining), try relationPlan(alloc, bound, recursive.step, verbose, depth + 1, remaining) }) },
    };
}

fn statementPlan(alloc: Allocator, statement: ast.Statement, bound: describe.BoundStatement, verbose: bool) anyerror!Plan {
    const kind: []const u8 = switch (statement) {
        .select => "Select",
        .insert => "Insert",
        .update => "Update",
        .delete => "Delete",
        .merge => "Merge",
        else => return error.UnsupportedSqlShape,
    };
    const relation_bound = if (bound.merge_mutation) |merge| merge.input.relation else if (bound.joined_mutation) |joined| joined.input.relation else if (bound.insert_source) |source| source.relation else bound.relation;
    var remaining: usize = 8192;
    var child: []const Plan = if (relation_bound) |source|
        try alloc.dupe(Plan, &.{try relationPlan(alloc, source, source.root, verbose, 0, &remaining)})
    else if (if (statement == .insert) null else bound.table) |table|
        try alloc.dupe(Plan, &.{.{
            .node_type = if (table.external_base_source != null) "Lake Scan" else "Native Table Access",
            .source_format = if (verbose) if (table.external_base_source) |lake| @tagName(lake.binding.format) else null else null,
            .snapshot_mode = if (verbose) if (table.external_base_source) |lake| @tagName(lake.binding.snapshot_mode) else null else null,
            .relation = try displayName(alloc, table),
            .table_id = if (verbose) table.id else null,
            .schema_version = if (verbose) table.schema_version else null,
        }})
    else
        &.{};
    if (statement == .select and child.len == 0) {
        var functions: std.ArrayList([]const u8) = .empty;
        try boundDecisionFunctions(alloc, bound, &functions);
        if (functions.items.len > 0) child = try alloc.dupe(Plan, &.{.{ .node_type = "Values" }});
    }
    if (statement == .select and child.len != 0 and (relation_bound == null or relation_bound.?.root.operation != .query))
        child = try alloc.dupe(Plan, &.{try queryStages(alloc, child[0], statement.select, bound)});
    if (bound.conflict) |conflict| {
        var inputs: std.ArrayList(Plan) = .empty;
        try inputs.appendSlice(alloc, child);
        for (conflict.deferred) |optional| if (optional) |deferred| {
            try inputs.append(alloc, .{
                .node_type = "Conflict Scalar Subquery",
                .plans = try alloc.dupe(Plan, &.{try statementPlan(alloc, .{ .select = deferred.query.* }, deferred.binding.*, verbose)}),
            });
        };
        child = inputs.items;
    }
    if (statement != .select) {
        var functions: std.ArrayList([]const u8) = .empty;
        try mutationDecisionFunctions(alloc, bound, &functions);
        if (functions.items.len > 0) child = try alloc.dupe(Plan, &.{.{
            .node_type = "DecisionEval",
            .functions = functions.items,
            .plans = child,
        }});
    }
    return .{
        .node_type = kind,
        .relation = if (bound.table) |table| try displayName(alloc, table) else null,
        .table_id = if (verbose) if (bound.table) |table| table.id else null else null,
        .schema_version = if (verbose) if (bound.table) |table| table.schema_version else null else null,
        .plans = child,
    };
}

fn appendText(alloc: Allocator, out: *std.ArrayList(u8), plan: Plan, depth: usize) !void {
    for (0..depth) |_| try out.appendSlice(alloc, "  ");
    try out.appendSlice(alloc, plan.node_type);
    if (plan.functions) |functions| for (functions) |name| {
        try out.appendSlice(alloc, " ");
        try out.appendSlice(alloc, name);
    };
    if (plan.join_kind) |kind| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " ({s})", .{kind}));
    if (plan.relation) |name| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " on {s}", .{name}));
    if (plan.index) |name| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " using {s}", .{name}));
    if (plan.schema_version) |version| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " [schema {d}]", .{version}));
    if (plan.source_format) |format| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " [format {s}]", .{format}));
    if (plan.snapshot_mode) |mode| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " [snapshot {s}]", .{mode}));
    try out.append(alloc, '\n');
    for (plan.plans) |child| try appendText(alloc, out, child, depth + 1);
}

pub fn render(alloc: Allocator, explanation: @FieldType(ast.Statement, "explain"), bound: describe.BoundStatement) ![]const u8 {
    const plan = try statementPlan(alloc, explanation.statement.*, bound, explanation.verbose);
    if (explanation.format == .json) return std.json.Stringify.valueAlloc(alloc, .{ .plan_version = 1, .Plan = plan }, .{});
    var out: std.ArrayList(u8) = .empty;
    try appendText(alloc, &out, plan, 0);
    return out.toOwnedSlice(alloc);
}
