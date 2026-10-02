// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
    table_id: ?u64 = null,
    schema_version: ?u32 = null,
    fields: ?[]const []const u8 = null,
    plans: []const Plan = &.{},
};

fn wrap(alloc: Allocator, name: []const u8, child: Plan) !Plan {
    return .{ .node_type = name, .plans = try alloc.dupe(Plan, &.{child}) };
}

fn queryStages(alloc: Allocator, source: Plan, statement: ast.Select, bound: describe.BoundStatement) !Plan {
    var plan = source;
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
                .node_type = if (source.request.index_equality != null) "Index Scan" else "Table Scan",
                .relation = try displayName(alloc, source.table),
                .index = if (source.request.index_equality) |index| index.name else null,
                .table_id = if (verbose) source.table.id else null,
                .schema_version = if (verbose) source.table.schema_version else null,
                .fields = if (verbose) source.request.fields else null,
            };
        },
        .join => |join| .{
            .node_type = if (join.left_keys.len != 0 and join.right_keys.len != 0) "Hash Join" else "Join",
            .join_kind = @tagName(join.kind),
            .plans = try alloc.dupe(Plan, &.{ try relationPlan(alloc, bound, join.left, verbose, depth + 1, remaining), try relationPlan(alloc, bound, join.right, verbose, depth + 1, remaining) }),
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

fn statementPlan(alloc: Allocator, statement: ast.Statement, bound: describe.BoundStatement, verbose: bool) !Plan {
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
            .node_type = "Native Table Access",
            .relation = try displayName(alloc, table),
            .table_id = if (verbose) table.id else null,
            .schema_version = if (verbose) table.schema_version else null,
        }})
    else
        &.{};
    if (statement == .select and child.len != 0 and (relation_bound == null or relation_bound.?.root.operation != .query))
        child = try alloc.dupe(Plan, &.{try queryStages(alloc, child[0], statement.select, bound)});
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
    if (plan.join_kind) |kind| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " ({s})", .{kind}));
    if (plan.relation) |name| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " on {s}", .{name}));
    if (plan.index) |name| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " using {s}", .{name}));
    if (plan.schema_version) |version| try out.appendSlice(alloc, try std.fmt.allocPrint(alloc, " [schema {d}]", .{version}));
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
