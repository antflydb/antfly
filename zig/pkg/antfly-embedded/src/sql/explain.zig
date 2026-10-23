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

//! Logical plans share authorized binding with execution. Ordinary EXPLAIN
//! opens no cursor; COSTS uses a dedicated metadata-only provider.
//! ANALYZE measurements come from one completed read-only execution.
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

const cost = @import("cost_model.zig");
const Plan = struct {
    source_ordinal: ?usize = null,
    estimates: ?cost.Estimate = null,
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
    plans: []Plan = &.{},
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
        for (aggregate.ordered) |plan| if (plan.direct) |*program| try decisionFunctions(alloc, program, functions);
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
        .prepared_rows => .{ .node_type = "Prepared Mutation Images" },
        .recursive_ref => .{ .node_type = "Recursive Reference" },
        .outer_ref => .{ .node_type = "Outer Reference" },
        .apply => |apply| blk: {
            const source: Plan = .{ .node_type = if (apply.demand != null) "Masked Apply" else "Lateral Apply", .join_kind = @tagName(apply.kind), .plans = try alloc.dupe(Plan, &.{ try relationPlan(alloc, bound, apply.left, verbose, depth + 1, remaining), try relationPlan(alloc, bound, apply.right, verbose, depth + 1, remaining) }) };
            var functions: std.ArrayList([]const u8) = .empty;
            if (apply.condition) |*program| try decisionFunctions(alloc, program, &functions);
            if (apply.demand) |*program| try decisionFunctions(alloc, program, &functions);
            if (functions.items.len == 0) break :blk source;
            break :blk .{ .node_type = "DecisionEval", .functions = functions.items, .plans = try alloc.dupe(Plan, &.{source}) };
        },
        .materialized_ref => |producer| .{ .node_type = "Materialized Reference", .plans = try alloc.dupe(Plan, &.{try relationPlan(alloc, bound, producer, verbose, depth + 1, remaining)}) },
        .scan => |scan| blk: {
            if (scan.index >= bound.scans.len) return error.InvalidSqlBackendResponse;
            const source = bound.scans[scan.index];
            break :blk .{
                .source_ordinal = scan.index,
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
                .node_type = if (join.membership != null) "Null-aware Membership" else if (join.left_keys.len != 0 and join.right_keys.len != 0) "Hash Join" else "Join",
                .join_kind = if (join.membership != null) "membership" else @tagName(join.kind),
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
    var child: []Plan = if (relation_bound) |source|
        try alloc.dupe(Plan, &.{try relationPlan(alloc, source, source.root, verbose, 0, &remaining)})
    else if (if (statement == .insert) null else bound.table) |table|
        try alloc.dupe(Plan, &.{.{
            .source_ordinal = 0,
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
    if (plan.estimates) |estimate| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " [estimated rows={?d}; local work={?d}]", .{ estimate.rows, estimate.local_work_units }));
    try out.append(alloc, '\n');
    for (plan.plans) |child| try appendText(alloc, out, child, depth + 1);
}

fn estimatePlan(plan: *Plan, cursors: []const catalog.ScanEstimate, scans: []const catalog.StatementScan) void {
    for (plan.plans) |*child| estimatePlan(child, cursors, scans);
    if (plan.source_ordinal) |index| {
        const stat = if (index < cursors.len) cursors[index] else null;
        const request: ?catalog.Scan = if (index < scans.len) scans[index].request else null;
        const filtered = request != null and request.?.conditions.len != 0 and (stat == null or stat.?.estimated_filtered_rows == null);
        plan.estimates = cost.source(if (stat) |c| c.estimated_filtered_rows orelse c.estimated_rows else null, if (stat) |c| c.estimated_bytes else null, filtered, request != null and request.?.primary_key != null);
    } else if (plan.plans.len == 2 and (std.mem.eql(u8, plan.node_type, "Hash Join") or std.mem.eql(u8, plan.node_type, "Join"))) {
        plan.estimates = cost.join(plan.plans[0].estimates orelse .{}, plan.plans[1].estimates orelse .{}, std.mem.eql(u8, plan.node_type, "Hash Join"));
    } else if (plan.plans.len == 1) {
        plan.estimates = cost.unary(plan.node_type, plan.plans[0].estimates orelse .{});
    } else plan.estimates = .{};
}
fn captureEstimates(a: Allocator, backend: catalog.Backend, bound: describe.BoundStatement, parameters: []const std.json.Value, plan: *Plan) !void {
    const scans_input: []const catalog.StatementScan = if (bound.relation) |source| source.scans else if (bound.table) |table| blk: {
        const fields = try a.alloc([]const u8, table.columns.len);
        for (fields, table.columns) |*field, column| field.* = column.path;
        break :blk try a.dupe(catalog.StatementScan, &.{.{ .table = table, .request = .{ .fields = fields, .limit = 1 } }});
    } else &.{};
    if (scans_input.len == 0) {
        estimatePlan(plan, &.{}, &.{});
        return;
    }
    // Ordinary capture can activate tracking or prepare search work. Only
    // the dedicated metadata capability may serve a planning-only request.
    if (backend.vtable.estimate_scans) |estimate| {
        const scans = try @import("relation_runtime.zig").bindSearchScans(a, scans_input, parameters);
        const metadata = try estimate(backend.ptr, a, scans);
        if (metadata.len != scans.len) return error.InvalidSqlBackendResponse;
        estimatePlan(plan, metadata, scans);
    } else estimatePlan(plan, &.{}, scans_input);
}
pub fn renderMeasured(alloc: Allocator, explanation: @FieldType(ast.Statement, "explain"), bound: describe.BoundStatement, profile: ?*const @import("execution_profile.zig").Profile, backend: catalog.Backend, parameters: []const std.json.Value) ![]const u8 {
    var plan = try statementPlan(alloc, explanation.statement.*, bound, explanation.verbose);
    if (explanation.costs) try captureEstimates(alloc, backend, bound, parameters, &plan);
    if (explanation.format == .json) return std.json.Stringify.valueAlloc(alloc, .{ .plan_version = 1, .plan_kind = "logical", .planning_only = !explanation.analyze, .Plan = plan, .cost_model = if (explanation.costs) cost.model else null, .runtime = profile }, .{});
    var out: std.ArrayList(u8) = .empty;
    try appendText(alloc, &out, plan, 0);
    if (explanation.costs) try out.appendSlice(alloc, "Cost units are logical work, not predicted time. Unknown statistics remain null.\n");
    if (profile) |measured| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, "Runtime: {d} result rows; elapsed_ns={?d}; retained_peak_bytes={?d}\n", .{ measured.result_rows, measured.elapsed_ns, measured.retained_peak_bytes }));
    return out.toOwnedSlice(alloc);
}
