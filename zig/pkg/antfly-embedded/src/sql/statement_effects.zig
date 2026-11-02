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

//! One bound graph and one native publication authority for writable WITH.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const scope = @import("statement_cte_scope.zig");
const refs = @import("recursive_shape.zig");
const A = std.mem.Allocator;

pub const Step = struct {
    statement: ast.Statement,
    binding: describe.BoundStatement,
    mutation: bool,
    demanded: bool,
    ordered_effects: bool = false,
};
pub const Bound = struct {
    scope: *const scope.Scope,
    steps: []const Step,
    root: Step,
};

fn combine(a: A, prefix: []const ast.Cte, inner: []const ast.Cte) ![]const ast.Cte {
    if (prefix.len == 0) return inner;
    const result = try a.alloc(ast.Cte, prefix.len + inner.len);
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], inner);
    return result;
}
fn withPrefix(a: A, input: ast.Statement, prefix: []const ast.Cte) !ast.Statement {
    var statement = input;
    switch (statement) {
        .select => |*query| query.ctes = try combine(a, prefix, query.ctes),
        .insert => |*insertion| if (insertion.source) |source| {
            const copy = try a.create(ast.Select);
            copy.* = source.*;
            copy.ctes = try combine(a, prefix, copy.ctes);
            insertion.source = copy;
        },
        .update => |*update| {
            update.ctes = try combine(a, prefix, update.ctes);
            if (update.source == null) {
                const source = try a.create(ast.Relation);
                source.* = .{ .table = .{ .name = update.table, .alias = update.alias, .mutation_target = true, .mutation_document = true, .mutation_presence = true } };
                update.source = source;
            }
        },
        .delete => |*deletion| {
            deletion.ctes = try combine(a, prefix, deletion.ctes);
            if (deletion.source == null) {
                const source = try a.create(ast.Relation);
                source.* = .{ .table = .{ .name = deletion.table, .alias = deletion.alias, .mutation_target = true, .mutation_presence = true } };
                deletion.source = source;
            }
        },
        .merge => |*merge| merge.ctes = try combine(a, prefix, merge.ctes),
        else => return error.UnsupportedSqlShape,
    }
    return statement;
}
fn bodyStatement(body: ast.StatementCte) ast.Statement {
    return switch (body.body) {
        .query => |query| .{ .select = query.* },
        .mutation => |mutation| mutation.*,
    };
}
fn returnsRows(statement: ast.Statement) bool {
    return switch (statement) {
        .select => true,
        .insert => |v| v.returning != null,
        .update => |v| v.returning != null,
        .delete => |v| v.returning != null,
        .merge => |v| v.returning != null,
        else => false,
    };
}
fn bindStep(a: A, backend: catalog.Backend, original: *const compiler.Compiled, statement: ast.Statement, hints: []const ?ast.ColumnType, mutation: bool, demanded: bool) !Step {
    var compiled = original.*;
    compiled.arena = undefined;
    compiled.statement = statement;
    return .{ .statement = statement, .binding = try describe.bindInternal(a, backend, &compiled, hints), .mutation = mutation, .demanded = demanded };
}
fn mergeParameters(into: []?ast.ColumnType, bound: describe.BoundStatement) !void {
    if (into.len != bound.parameter_types.len) return error.InvalidSqlBackendResponse;
    for (into, bound.parameter_types) |*target, input| if (input) |kind| {
        if (target.*) |previous| if (previous != kind) return error.SqlTypeMismatch;
        target.* = kind;
    };
}

const Dependencies = struct { order: []const usize, demand: []const u8 };
fn dependencies(a: A, backend: catalog.Backend, block: ast.EffectBlock) !Dependencies {
    const n = block.definitions.len;
    const edges = try a.alloc(u8, n * n);
    @memset(edges, 0);
    const demand = try a.alloc(u8, n);
    for (block.definitions, 0..) |definition, consumer| {
        try backend.vtable.checkpoint(backend.ptr);
        demand[consumer] = @intCast(@min(2, try refs.statementReferences(block.statement.*, definition.name, 0)));
        const body = bodyStatement(definition);
        if (block.recursive and definition.body == .mutation and try refs.statementReferences(body, definition.name, 0) != 0) return error.SqlInvalidRecursion;
        for (block.definitions, 0..) |source, producer| {
            if (consumer == producer or (!block.recursive and producer >= consumer)) continue;
            edges[consumer * n + producer] = @intCast(@min(2, try refs.statementReferences(body, source.name, 0)));
        }
    }
    const order = try a.alloc(usize, n);
    var seen: [64]bool = @splat(false);
    var completed: usize = 0;
    while (completed != n) {
        var progress = false;
        for (0..n) |consumer| {
            if (seen[consumer]) continue;
            var ready = true;
            for (0..n) |producer| if (edges[consumer * n + producer] != 0 and !seen[producer]) {
                ready = false;
                break;
            };
            if (!ready) continue;
            order[completed] = consumer;
            seen[consumer] = true;
            completed += 1;
            progress = true;
        }
        // PostgreSQL rejects mutual recursion. Direct read recursion stays in
        // the existing recursive operator; direct mutation recursion is 42P19.
        if (!progress) return error.UnsupportedSqlShape;
    }
    var remaining = n;
    while (remaining != 0) {
        remaining -= 1;
        const consumer = order[remaining];
        const definition = block.definitions[consumer];
        const evaluations: usize = if (definition.body == .mutation) 1 else if (demand[consumer] == 0) 0 else if (definition.materialization != .not_materialized) 1 else demand[consumer];
        for (0..n) |producer| demand[producer] = @intCast(@min(2, demand[producer] + evaluations * edges[consumer * n + producer]));
    }
    return .{ .order = order, .demand = demand };
}

pub fn bind(a: A, input_backend: catalog.Backend, compiled: *const compiler.Compiled, hints: []const ?ast.ColumnType) anyerror!describe.BoundStatement {
    const block = compiled.statement.effect_block;
    if (block.definitions.len == 0 or block.definitions.len > 64) return error.SqlProgramLimitExceeded;
    const definitions = try a.alloc(scope.Definition, block.definitions.len);
    for (block.definitions, definitions, 0..) |definition, *slot, index| {
        for (block.definitions[0..index]) |earlier| if (std.mem.eql(u8, earlier.name, definition.name)) return error.SqlDuplicateCte;
        slot.* = .{ .name = definition.name, .returns_rows = returnsRows(bodyStatement(definition)) };
    }
    const visibility = try a.create(scope.Scope);
    visibility.* = .{ .definitions = definitions, .binding_scope = input_backend.binding_scope };
    var backend = input_backend;
    backend.statement_ctes = visibility;
    // Statement-owned cursors, not the MERGE point optimization, establish the
    // common cut before any producer. No child may refresh that snapshot.
    backend.coordinated_point_reads = false;
    backend.coordinated_index_reads = false;
    const dependency_plan = try dependencies(a, backend, block);
    const counts = dependency_plan.demand;
    var effects: Effects = .{ .a = a };
    var ordered: Effects = .{ .a = a, .ordered_only = true };
    const steps = try a.alloc(Step, definitions.len);
    var prefix: std.ArrayList(ast.Cte) = .empty;
    const parameters = try a.alloc(?ast.ColumnType, compiled.parameter_count);
    @memset(parameters, null);
    if (hints.len > parameters.len) return error.InvalidSqlParameters;
    @memcpy(parameters[0..hints.len], hints);
    for (dependency_plan.order) |ordinal| {
        const definition = block.definitions[ordinal];
        const body = bodyStatement(definition);
        backend.statement_cte_visibility = if (block.recursive) definitions.len else ordinal;
        if (definition.body == .query) {
            const declaration: ast.Cte = .{ .name = definition.name, .columns = definition.columns, .query = definition.body.query, .recursive = block.recursive, .materialization = .not_materialized, .statement_cte_visibility = backend.statement_cte_visibility };
            const local = try combine(a, prefix.items, &.{declaration});
            // A single wildcard wrapper makes the root iterator represent
            // the complete query (including its bounds, grouping and sort).
            const query: ast.Select = .{ .table = .{ .table = definition.name }, .columns = &.{.{ .wildcard = true }}, .ctes = local };
            steps[ordinal] = try bindStep(a, backend, compiled, .{ .select = query }, parameters, false, counts[ordinal] != 0);
            definitions[ordinal].ordered_effects = try ordered.bound(steps[ordinal].binding, 0);
            definitions[ordinal].inline_read = definition.materialization != .materialized and
                !try effects.bound(steps[ordinal].binding, 0) and
                (counts[ordinal] <= 1 or definition.materialization == .not_materialized);
            if (definitions[ordinal].inline_read) {
                var inline_definition = declaration;
                inline_definition.materialization = definition.materialization;
                try prefix.append(a, inline_definition);
            }
        } else {
            const statement = try withPrefix(a, body, prefix.items);
            steps[ordinal] = try bindStep(a, backend, compiled, statement, parameters, true, counts[ordinal] != 0);
            definitions[ordinal].ordered_effects = try ordered.bound(steps[ordinal].binding, 0);
            definitions[ordinal].ordered_effects = definitions[ordinal].ordered_effects or nativeDefaultEffects(statement, steps[ordinal].binding);
        }
        steps[ordinal].ordered_effects = definitions[ordinal].ordered_effects;
        try mergeParameters(parameters, steps[ordinal].binding);
        const columns = try a.dupe(describe.Column, steps[ordinal].binding.columns);
        if (definition.columns.len > columns.len) return error.InvalidSqlSyntax;
        for (definition.columns, 0..) |name, i| columns[i].name = name;
        definitions[ordinal].columns = columns;
        definitions[ordinal].bound = true;
    }
    backend.statement_cte_visibility = definitions.len;
    var root = try bindStep(a, backend, compiled, try withPrefix(a, block.statement.*, prefix.items), parameters, block.statement.* != .select, true);
    root.ordered_effects = try ordered.bound(root.binding, 0) or nativeDefaultEffects(root.statement, root.binding);
    try mergeParameters(parameters, root.binding);
    const graph = try a.create(Bound);
    graph.* = .{ .scope = visibility, .steps = steps, .root = root };
    return .{ .table = null, .action = .read_write, .columns = root.binding.columns, .parameter_types = parameters, .json_literals = .empty, .effect_graph = graph };
}

fn nativeDefaultEffects(statement: ast.Statement, bound: describe.BoundStatement) bool {
    if (bound.joined_mutation) |mutation| for (mutation.default_paths) |path| for (bound.table.?.columns) |column| {
        if (std.mem.eql(u8, column.path, path) and (column.sequence_default != null or column.clock_defaulted)) return true;
    };
    if (statement == .insert) if (bound.table) |table| for (table.columns) |column| {
        if (column.sequence_default != null or column.clock_defaulted) return true;
    };
    if (bound.merge_mutation) |mutation| for (mutation.arms) |arm| switch (arm.action) {
        .insert, .update => |assignments| for (assignments) |assignment| {
            if (assignment.program == null and (assignment.column.sequence_default != null or assignment.column.clock_defaulted)) return true;
        },
        else => {},
    };
    return false;
}

fn programEffect(program: @import("scalar.zig").Program, ordered_only: bool) bool {
    for (program.instructions) |instruction| if (instruction.operation == .call) {
        const call = instruction.operation.call;
        if (ordered_only) switch (call.function) {
            // Inference has a request-batch contract; volatility prevents CTE
            // duplication but does not turn that batch into ordered row RPCs.
            .ai_decide, .ai_choice, .ai_score, .ai_probability => continue,
            else => {},
        };
        const volatility = if (call.function == .@"$routine") (if (call.routine) |routine| routine.volatility else .@"volatile") else call.function.volatility();
        if (volatility == .@"volatile") return true;
    };
    return false;
}
fn scalarVolatile(values: @import("bound_scalars.zig").Bound, ordered_only: bool) bool {
    if (values.predicate) |program| if (programEffect(program, ordered_only)) return true;
    for (values.projections) |program| if (program) |p| if (programEffect(p, ordered_only)) return true;
    for (values.orders) |program| if (program) |p| if (programEffect(p, ordered_only)) return true;
    for (values.assignments) |program| if (program) |p| if (programEffect(p, ordered_only)) return true;
    for (values.insert_rows) |row| for (row) |program| {
        if (program) |p| if (programEffect(p, ordered_only)) return true;
    };
    return false;
}
/// Immutable effect analysis shared by binding and cursor admission. Memoize
/// relation DAGs so shared CTE inputs never multiply classification work.
pub const Effects = struct {
    a: A,
    ordered_only: bool = false,
    nodes: std.AutoHashMapUnmanaged(*const @import("relation_binding.zig").Node, bool) = .empty,
    fn effect(self: *const @This(), program: @import("scalar.zig").Program) bool {
        return programEffect(program, self.ordered_only);
    }
    pub fn bound(self: *@This(), value: describe.BoundStatement, depth: usize) anyerror!bool {
        if (depth >= 64) return error.SqlProgramLimitExceeded;
        if (scalarVolatile(value.scalars, self.ordered_only)) return true;
        if (value.insert_source) |input| if (try self.bound(input.*, depth + 1)) return true;
        if (value.joined_mutation) |mutation| {
            if (try self.bound(mutation.input.*, depth + 1)) return true;
            if (mutation.returning_binding) |input| if (try self.bound(input.*, depth + 1)) return true;
            if (mutation.returning_plan) |output| for (output.programs) |program| {
                if (self.effect(program)) return true;
            };
        }
        if (value.merge_mutation) |mutation| {
            if (try self.bound(mutation.input.*, depth + 1)) return true;
            for (mutation.arms) |arm| {
                if (arm.predicate) |program| if (self.effect(program)) return true;
                switch (arm.action) {
                    .update, .insert => |assignments| for (assignments) |assignment| {
                        if (assignment.program) |program| if (self.effect(program)) return true;
                    },
                    else => {},
                }
            }
            if (mutation.returning_plan) |output| for (output.programs) |program| {
                if (self.effect(program)) return true;
            };
        }
        if (value.conflict) |conflict| {
            for (conflict.assignments) |optional| if (optional) |program| if (self.effect(program)) return true;
            if (conflict.predicate) |program| if (self.effect(program)) return true;
            for (conflict.deferred) |optional| if (optional) |deferred| if (try self.bound(deferred.binding.*, depth + 1)) return true;
        }
        if (value.returning) |input| if (try self.bound(input.*, depth + 1)) return true;
        if (value.relation) |relation| if (try self.node(relation.root, depth + 1)) return true;
        if (value.aggregate) |aggregate| {
            if (scalarVolatile(aggregate.input, self.ordered_only)) return true;
            for (aggregate.ordered) |ordered| if (ordered.direct) |program| if (self.effect(program)) return true;
            for (aggregate.collections) |collection| if (collection.user) |routine| {
                if (routine.transition.volatility == .@"volatile") return true;
                if (routine.final) |final| if (final.volatility == .@"volatile") return true;
            };
            for (aggregate.outputs) |program| if (self.effect(program)) return true;
            for (aggregate.orders) |program| if (self.effect(program)) return true;
            if (aggregate.having) |program| if (self.effect(program)) return true;
        }
        if (value.window) |window| {
            if (try self.bound(window.input.*, depth + 1)) return true;
            for (window.outputs) |program| if (self.effect(program)) return true;
            for (window.orders) |program| if (self.effect(program)) return true;
        }
        return false;
    }
    pub fn node(self: *@This(), value: *const @import("relation_binding.zig").Node, depth: usize) anyerror!bool {
        if (depth >= 64) return error.SqlProgramLimitExceeded;
        if (self.nodes.get(value)) |known| return known;
        if (self.nodes.count() >= 16384) return error.SqlProgramLimitExceeded;
        const result: bool = switch (value.operation) {
            .statement_cte => |v| v.ordered,
            .query => |v| try self.bound(v.binding, depth + 1) or try self.node(v.source, depth + 1),
            .materialized_ref => |v| try self.node(v, depth + 1),
            .recursive => |v| try self.node(v.seed, depth + 1) or try self.node(v.step, depth + 1),
            .join => |v| blk: {
                if (v.condition) |program| if (self.effect(program)) break :blk true;
                for (v.left_keys) |program| if (self.effect(program)) break :blk true;
                for (v.right_keys) |program| if (self.effect(program)) break :blk true;
                break :blk try self.node(v.left, depth + 1) or try self.node(v.right, depth + 1);
            },
            .apply => |v| blk: {
                if (v.condition) |program| if (self.effect(program)) break :blk true;
                if (v.demand) |program| if (self.effect(program)) break :blk true;
                break :blk try self.node(v.left, depth + 1) or try self.node(v.right, depth + 1);
            },
            .unnest => |v| blk: {
                for (v.arrays) |program| if (self.effect(program)) break :blk true;
                break :blk try self.node(v.source, depth + 1);
            },
            .set => |v| try self.node(v.left, depth + 1) or try self.node(v.right, depth + 1),
            .values => |values| blk: {
                for (values) |v| if (try self.node(v, depth + 1)) break :blk true;
                break :blk false;
            },
            else => false,
        };
        try self.nodes.put(self.a, value, result);
        return result;
    }
};
