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

//! Separate row/group/window inputs from scalar output reads. Source names
//! survive through typed binding metadata, not lexical rewrites of children.
const std = @import("std");
const ast = @import("ast.zig");
const aggregates = @import("aggregate_binding.zig");
const windows = @import("window_binding.zig");
const Allocator = std.mem.Allocator;

fn phaseCall(value: *const ast.Scalar) bool {
    return value.* == .call and (value.call.window != null or value.call.within_group != null or aggregates.aggregateKind(value.call.name) != null);
}

fn ownAggregate(value: *const ast.Scalar) bool {
    return switch (value.*) {
        .call => |call| blk: {
            if (call.subquery != null) break :blk false;
            if (call.window == null and (call.within_group != null or aggregates.aggregateKind(call.name) != null)) break :blk true;
            for (call.args) |arg| if (ownAggregate(arg)) break :blk true;
            if (call.filter) |filter| if (ownAggregate(filter)) break :blk true;
            if (call.window) |window| {
                for (window.partition) |arg| if (ownAggregate(arg)) break :blk true;
                for (window.order) |order| if (order.expression) |arg| if (ownAggregate(arg)) break :blk true;
            }
            break :blk false;
        },
        .unary => |part| ownAggregate(part.operand),
        .cast => |part| ownAggregate(part.operand),
        .binary => |part| ownAggregate(part.left) or ownAggregate(part.right),
        .case_when => |part| blk: {
            for (part.branches) |branch| if (ownAggregate(branch.condition) or ownAggregate(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| ownAggregate(other) else false;
        },
        .in_list => |part| blk: {
            if (ownAggregate(part.operand)) break :blk true;
            for (part.values) |arg| if (ownAggregate(arg)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

fn isGrouped(statement: ast.Select) bool {
    if (statement.group_by.len != 0 or statement.having != null or statement.count_all) return true;
    for (statement.columns) |projection| if (projection.expression) |value| if (ownAggregate(value)) return true;
    for (statement.order_by) |order| if (order.expression) |value| if (ownAggregate(value)) return true;
    return false;
}

fn outputRead(value: *const ast.Scalar) bool {
    if (phaseCall(value)) return false;
    return switch (value.*) {
        .call => |call| blk: {
            if (call.subquery != null) break :blk true;
            for (call.args) |arg| if (outputRead(arg)) break :blk true;
            break :blk if (call.filter) |filter| outputRead(filter) else false;
        },
        .unary => |part| outputRead(part.operand),
        .cast => |part| outputRead(part.operand),
        .binary => |part| outputRead(part.left) or outputRead(part.right),
        .case_when => |part| blk: {
            for (part.branches) |branch| if (outputRead(branch.condition) or outputRead(branch.value)) break :blk true;
            break :blk if (part.otherwise) |other| outputRead(other) else false;
        },
        .in_list => |part| blk: {
            if (outputRead(part.operand)) break :blk true;
            for (part.values) |item| if (outputRead(item)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

pub fn accepts(statement: ast.Select) bool {
    if (statement.phase_inputs or (!aggregates.accepts(statement) and !windows.accepts(statement) and !statement.count_all)) return false;
    for (statement.columns) |projection| if (projection.expression) |value| if (outputRead(value)) return true;
    if (statement.having) |value| if (outputRead(value)) return true;
    for (statement.order_by) |order| if (order.expression) |value| if (outputRead(value)) return true;
    return false;
}

/// Resolve correlated dependencies with the ordinary catalog binder. Do not
/// guess which unqualified names belong to a child versus its outer frame.
pub fn visitReferences(value: *const ast.Scalar, groups: []const *const ast.Scalar, context: anytype) anyerror!void {
    for (groups) |group| if (aggregates.same(value, group)) return;
    if (phaseCall(value)) return;
    switch (value.*) {
        .column => |name| try context.column(name),
        .literal => {},
        .call => |call| {
            if (call.subquery) |child| try context.subquery(child);
            for (call.args) |arg| try visitReferences(arg, groups, context);
            if (call.filter) |filter| try visitReferences(filter, groups, context);
        },
        .unary => |part| try visitReferences(part.operand, groups, context),
        .cast => |part| try visitReferences(part.operand, groups, context),
        .binary => |part| {
            try visitReferences(part.left, groups, context);
            try visitReferences(part.right, groups, context);
        },
        .case_when => |part| {
            for (part.branches) |branch| {
                try visitReferences(branch.condition, groups, context);
                try visitReferences(branch.value, groups, context);
            }
            if (part.otherwise) |other| try visitReferences(other, groups, context);
        },
        .in_list => |part| {
            try visitReferences(part.operand, groups, context);
            for (part.values) |item| try visitReferences(item, groups, context);
        },
    }
}

pub const Field = struct { name: []const u8, qualifier: []const u8, needed: bool };
pub const Equality = struct {
    ptr: *anyopaque,
    same: *const fn (*anyopaque, *const ast.Scalar, *const ast.Scalar) anyerror!bool,
};
const Builder = struct {
    alloc: Allocator,
    alias: []const u8,
    grouped: bool,
    groups: []const *const ast.Scalar,
    equality: Equality,
    lift_windows: bool,
    inputs: std.ArrayList(ast.Projection) = .empty,
    scopes: std.ArrayList(?[]const u8) = .empty,
    values: std.ArrayList(*const ast.Scalar) = .empty,

    fn node(self: *Builder, value: ast.Scalar) !*const ast.Scalar {
        const out = try self.alloc.create(ast.Scalar);
        out.* = value;
        return out;
    }
    fn slot(self: *Builder, index: usize) !*const ast.Scalar {
        return self.node(.{ .column = try std.fmt.allocPrint(self.alloc, "{s}\x00$phase_{d}", .{ self.alias, index }) });
    }
    fn input(self: *Builder, value: *const ast.Scalar, source: ?[]const u8) !*const ast.Scalar {
        for (self.values.items, 0..) |prior, index| if (try self.equality.same(self.equality.ptr, prior, value)) return self.slot(index);
        const index = self.inputs.items.len;
        if (index == 1024) return error.SqlProgramLimitExceeded;
        try self.values.append(self.alloc, value);
        try self.scopes.append(self.alloc, source);
        try self.inputs.append(self.alloc, .{ .alias = try std.fmt.allocPrint(self.alloc, "$phase_{d}", .{index}), .expression = value });
        return self.slot(index);
    }
    fn rewrite(self: *Builder, value: *const ast.Scalar) anyerror!*const ast.Scalar {
        if (self.grouped) for (self.groups) |group| if (try self.equality.same(self.equality.ptr, value, group)) {
            // Bare grouped columns retain their original lexical identity.
            if (group.* == .column) return group;
            return self.input(group, null);
        };
        if (phaseCall(value) and (value.call.window == null or self.lift_windows)) return self.input(value, null);
        return switch (value.*) {
            .literal, .column => value,
            .unary => |part| self.node(.{ .unary = .{ .op = part.op, .operand = try self.rewrite(part.operand) } }),
            .cast => |part| self.node(.{ .cast = .{ .type = part.type, .element_type = part.element_type, .operand = try self.rewrite(part.operand) } }),
            .binary => |part| self.node(.{ .binary = .{ .op = part.op, .left = try self.rewrite(part.left), .right = try self.rewrite(part.right) } }),
            .call => |part| blk: {
                // The child binds later against completed phase identities.
                var call = part;
                const args = try self.alloc.alloc(*const ast.Scalar, part.args.len);
                for (part.args, args) |arg, *out| out.* = try self.rewrite(arg);
                call.args = args;
                call.filter = if (part.filter) |filter| try self.rewrite(filter) else null;
                if (part.window) |window| call.window = try self.rewriteWindow(window);
                break :blk try self.node(.{ .call = call });
            },
            .case_when => |part| blk: {
                const branches = try self.alloc.alloc(ast.Scalar.Branch, part.branches.len);
                for (part.branches, branches) |branch, *out| out.* = .{ .condition = try self.rewrite(branch.condition), .value = try self.rewrite(branch.value) };
                break :blk try self.node(.{ .case_when = .{ .branches = branches, .otherwise = if (part.otherwise) |other| try self.rewrite(other) else null } });
            },
            .in_list => |part| blk: {
                const values = try self.alloc.alloc(*const ast.Scalar, part.values.len);
                for (part.values, values) |item, *out| out.* = try self.rewrite(item);
                break :blk try self.node(.{ .in_list = .{ .operand = try self.rewrite(part.operand), .values = values, .negated = part.negated } });
            },
        };
    }

    fn rewriteWindow(self: *Builder, original: ast.Window) anyerror!ast.Window {
        var result = original;
        const partition = try self.alloc.alloc(*const ast.Scalar, original.partition.len);
        for (original.partition, partition) |value, *out| out.* = try self.rewrite(value);
        result.partition = partition;
        const order = try self.alloc.dupe(ast.Order, original.order);
        for (order) |*item| {
            item.expression = try self.rewrite(item.expression orelse try self.node(.{ .column = item.field }));
            item.field = "";
        }
        result.order = order;
        return result;
    }
};

pub fn lower(alloc: Allocator, statement: ast.Select, fields: []const Field, equality: Equality) !ast.Select {
    var normalized = try @import("order_aliases.zig").normalize(alloc, statement);
    if (normalized.count_all) {
        const count = try alloc.create(ast.Scalar);
        count.* = .{ .call = .{ .name = "count", .args = &.{}, .star = true } };
        normalized.columns = try alloc.dupe(ast.Projection, &.{.{ .alias = normalized.count_alias orelse "count", .expression = count }});
        normalized.count_all = false;
    }
    const grouped = isGrouped(normalized);
    const having_read = if (normalized.having) |having| outputRead(having) else false;
    const lift_windows = !(grouped and having_read and windows.accepts(normalized));
    var serial: usize = 0;
    const alias = while (serial < 1024) : (serial += 1) {
        const candidate = try std.fmt.allocPrint(alloc, "$phase_output_{d}", .{serial});
        const collision = for (fields) |field| {
            if (std.mem.eql(u8, field.qualifier, candidate)) break true;
        } else false;
        if (!collision) break candidate;
    } else return error.SqlProgramLimitExceeded;
    var builder: Builder = .{ .alloc = alloc, .alias = alias, .grouped = grouped, .groups = normalized.group_by, .equality = equality, .lift_windows = lift_windows };
    if (grouped) {
        for (normalized.group_by) |group| _ = try builder.input(group, if (group.* == .column) group.column else null);
    } else for (fields) |field| if (field.needed) {
        _ = try builder.input(try builder.node(.{ .column = field.name }), field.name);
    };
    const outputs = try alloc.dupe(ast.Projection, normalized.columns);
    for (outputs) |*output| if (output.expression) |value| {
        if (output.alias == null) output.alias = if (value.* == .call and value.call.subquery == null) value.call.name else if (value.* == .call and std.mem.eql(u8, value.call.name, "$exists")) "exists" else "?column?";
        output.expression = try builder.rewrite(value);
    };
    const orders = try alloc.dupe(ast.Order, normalized.order_by);
    for (orders) |*order| if (order.expression) |value| {
        order.expression = try builder.rewrite(value);
    };
    const having = if (having_read) try builder.rewrite(normalized.having.?) else null;
    var output_windows: []const ast.NamedWindow = &.{};
    if (!lift_windows) {
        const definitions = try alloc.alloc(ast.NamedWindow, normalized.windows.len);
        for (normalized.windows, definitions) |definition, *out| out.* = .{ .name = definition.name, .window = try builder.rewriteWindow(definition.window) };
        output_windows = definitions;
    }
    if (builder.inputs.items.len == 0) {
        _ = try builder.input(try builder.node(if (grouped) .{ .call = .{ .name = "count", .args = &.{}, .star = true } } else .{ .literal = .{ .integer = 1 } }), null);
    }
    const input = try alloc.create(ast.Select);
    input.* = normalized;
    input.columns = try builder.inputs.toOwnedSlice(alloc);
    input.order_by = &.{};
    input.order_aliases_expanded = false;
    input.limit = null;
    input.offset = null;
    input.scalar_cardinality_limit = false;
    input.required_output_columns = null;
    input.having = if (having_read) null else normalized.having;
    if (!lift_windows) input.windows = &.{};
    input.ctes = &.{};
    input.phase_inputs = true;
    const scope = try alloc.create(ast.PhaseScope);
    scope.* = .{ .columns = try builder.scopes.toOwnedSlice(alloc), .grouped = grouped };
    const source = try alloc.create(ast.Relation);
    source.* = .{ .derived = .{ .query = input, .alias = alias, .phase_scope = scope } };
    var output = normalized;
    output.source = source;
    output.table = null;
    output.columns = outputs;
    output.order_by = orders;
    output.group_by = &.{};
    output.having = null;
    output.windows = output_windows;
    output.predicate = if (having) |value| blk: {
        const predicate = try alloc.create(ast.Predicate);
        predicate.* = .{ .scalar = value };
        break :blk predicate;
    } else null;
    return output;
}
