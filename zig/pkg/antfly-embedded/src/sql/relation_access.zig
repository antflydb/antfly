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

//! Immutable relation topology and statement-local native access preparation.
//! SQL residuals remain authoritative; materialization and row bounds are fences.
const std = @import("std");
const binding = @import("relation_binding.zig");
const catalog = @import("catalog.zig");
const scalar = @import("scalar.zig");
const access = @import("index_access.zig");
const A = std.mem.Allocator;
const Node = binding.Node;
const absent = std.math.maxInt(u32);
const visiting = absent - 1;

pub const Graph = struct {
    const Entry = struct { node: *const Node, children: []const u32 };
    entries: []const Entry,
    lookup: std.AutoHashMapUnmanaged(*const Node, u32),
    immediate: []const u32,
    scans: [64]?u32,

    /// Allocation belongs to the immutable binding region, never a reader.
    pub fn createLeaky(a: A, root: *const Node, control: access.Control) !*const Graph {
        var builder: Builder = .{ .a = a, .control = control };
        const root_index = try builder.visit(root, 0);
        const entries = try builder.entries.toOwnedSlice(a);
        const immediate = try a.alloc(u32, entries.len);
        @memset(immediate, absent);
        var scans: [64]?u32 = @splat(null);
        if (root_index != absent) {
            immediate[root_index] = root_index;
            var remaining = entries.len;
            while (remaining != 0) {
                remaining -= 1;
                try poll(control);
                const entry = entries[remaining];
                if (entry.node.operation == .scan) {
                    const scan = entry.node.operation.scan.index;
                    if (scan >= scans.len or scans[scan] != null) return error.InvalidSqlBackendResponse;
                    scans[scan] = @intCast(remaining);
                }
                for (entry.children) |child_| {
                    immediate[child_] = if (immediate[child_] == absent) @intCast(remaining) else intersect(immediate, immediate[child_], @intCast(remaining));
                }
            }
        }
        const graph = try a.create(Graph);
        graph.* = .{ .entries = entries, .lookup = builder.lookup, .immediate = immediate, .scans = scans };
        return graph;
    }

    fn intersect(immediate: []const u32, first: u32, second: u32) u32 {
        var left = first;
        var right = second;
        while (left != right) {
            if (left < right) left = immediate[left] else right = immediate[right];
        }
        return left;
    }

    fn dominates(self: Graph, ancestor: u32, node: u32) bool {
        var current = node;
        while (current < ancestor) current = self.immediate[current];
        return current == ancestor;
    }

    fn nodeIndex(self: Graph, node: *const Node) ?u32 {
        const index = self.lookup.get(node) orelse return null;
        return if (index == absent or index == visiting) null else index;
    }

    const Lineage = struct { scan: usize, column: []const u8, nullable: bool = false };

    fn lineage(self: Graph, node: *const Node, name: []const u8, skip_nullable: ?*const Node, depth: usize) error{ SqlProgramLimitExceeded, InvalidSqlBackendResponse }!?Lineage {
        if (depth >= 128) return error.SqlProgramLimitExceeded;
        const ordinal = columnOrdinal(node.columns, name) orelse return null;
        const result: ?Lineage = switch (node.operation) {
            .scan => |scan| .{ .scan = scan.index, .column = scan.source_columns[ordinal] },
            .join => |join| try self.joinLineage(node, join.left, join.right, join.kind, name, skip_nullable, depth),
            .apply => |join| try self.joinLineage(node, join.left, join.right, join.kind, name, skip_nullable, depth),
            .unnest => |part| if (hasVolatile(part.arrays)) null else try self.lineage(part.source, name, skip_nullable, depth + 1),
            .query => |query| derived: {
                if (!transparent(query)) break :derived null;
                if (ordinal >= query.statement.columns.len) break :derived null;
                const projection = query.statement.columns[ordinal];
                const input = if (projection.expression == null) projection.field else input: {
                    if (ordinal >= query.binding.scalars.projections.len) break :derived null;
                    const program = query.binding.scalars.projections[ordinal] orelse break :derived null;
                    const operation = program.instructions[program.root].operation;
                    if (operation != .column) break :derived null;
                    break :input query.binding.scalars.columns[operation.column].name;
                };
                const input_ordinal = columnOrdinal(query.source.columns, input) orelse break :derived null;
                if (!sameDomain(node.columns[ordinal], query.source.columns[input_ordinal])) break :derived null;
                break :derived try self.lineage(query.source, input, skip_nullable, depth + 1);
            },
            // Explicit/shared materialization, recursion and set coercions are
            // optimizer fences. Their own producer predicates are visited below.
            else => null,
        };
        const found = result orelse return null;
        const index = self.nodeIndex(node) orelse return null;
        const scan_node = self.scans[found.scan] orelse return error.InvalidSqlBackendResponse;
        return if (self.dominates(index, scan_node)) found else null;
    }

    fn joinLineage(self: Graph, node: *const Node, left: *const Node, right: *const Node, kind: @import("ast.zig").JoinKind, name: []const u8, skip_nullable: ?*const Node, depth: usize) !?Lineage {
        const in_left = columnOrdinal(left.columns, name) != null;
        const in_right = columnOrdinal(right.columns, name) != null;
        if (in_left == in_right) return null;
        var found = (try self.lineage(if (in_left) left else right, name, skip_nullable, depth + 1)) orelse return null;
        if (skip_nullable != node and (kind == .full or (in_left and kind == .right) or (in_right and kind == .left))) found.nullable = true;
        return found;
    }

    const Builder = struct {
        a: A,
        control: access.Control,
        lookup: std.AutoHashMapUnmanaged(*const Node, u32) = .empty,
        entries: std.ArrayList(Entry) = .empty,

        fn visit(self: *@This(), node: *const Node, depth: usize) anyerror!u32 {
            if (depth >= 128) return error.SqlProgramLimitExceeded;
            try poll(self.control);
            if (self.lookup.get(node)) |known| return if (known == visiting) error.InvalidSqlBackendResponse else known;
            if (self.lookup.count() >= 8192) return error.SqlProgramLimitExceeded;
            try self.lookup.put(self.a, node, visiting);
            var children: std.ArrayList(u32) = .empty;
            switch (node.operation) {
                .query => |part| try self.child(&children, part.source, depth),
                .materialized_ref => |source| try self.child(&children, source, depth),
                .unnest => |part| try self.child(&children, part.source, depth),
                inline .join, .apply => |part| {
                    try self.child(&children, part.left, depth);
                    try self.child(&children, part.right, depth);
                },
                .set => |part| {
                    try self.child(&children, part.left, depth);
                    try self.child(&children, part.right, depth);
                },
                .recursive => |part| {
                    try self.child(&children, part.seed, depth);
                    try self.child(&children, part.step, depth);
                },
                .values => |arms| for (arms) |arm| try self.child(&children, arm, depth),
                else => {},
            }
            const index: u32 = if (node.operation == .scan or children.items.len != 0) @intCast(self.entries.items.len) else absent;
            if (index != absent) try self.entries.append(self.a, .{ .node = node, .children = try children.toOwnedSlice(self.a) });
            try self.lookup.put(self.a, node, index);
            return index;
        }
        fn child(self: *@This(), list: *std.ArrayList(u32), node: *const Node, depth: usize) !void {
            const index = try self.visit(node, depth + 1);
            if (index != absent) try list.append(self.a, index);
        }
    };
};

fn poll(control: access.Control) !void {
    if (control.checkpoint) |check| try check(control.ptr);
}
fn columnOrdinal(columns: []const binding.Column, name: []const u8) ?usize {
    for (columns, 0..) |column, index| if (std.mem.eql(u8, column.internal, name)) return index;
    return null;
}
fn sameDomain(left: anytype, right: anytype) bool {
    return left.type == right.type and access.canonicalElement(left.type, left.element_type) == access.canonicalElement(right.type, right.element_type) and scalar.NumericModifier.eql(left.numeric_modifier, right.numeric_modifier);
}
fn hasVolatile(programs: anytype) bool {
    for (programs) |optional| {
        const program = if (@typeInfo(@TypeOf(optional)) == .optional) optional orelse continue else optional;
        for (program.instructions) |instruction| if (instruction.operation == .call and instruction.operation.call.function.volatility() == .@"volatile") return true;
    }
    return false;
}
fn transparent(query: anytype) bool {
    return query.statement.limit == null and query.statement.offset == null and !query.statement.scalar_cardinality_limit and !query.statement.count_all and query.binding.aggregate == null and query.binding.window == null and
        !hasVolatile(query.binding.scalars.projections) and !hasVolatile(query.binding.scalars.orders) and
        (query.binding.scalars.predicate == null or !hasVolatile(@as([]const scalar.Program, &.{query.binding.scalars.predicate.?})));
}

pub fn eligible(scans: []const catalog.StatementScan) bool {
    for (scans) |scan| if (scan.table.storage_mode == .relational and scan.table.external_base_source == null and scan.request.search == null) return true;
    return false;
}

pub fn prepareGraph(a: A, root: *const Node, scans: []const catalog.StatementScan, backend: catalog.Backend) !?*const Graph {
    if (!backend.supports_expression_access_hints or !eligible(scans)) return null;
    const Check = struct {
        fn pollBackend(raw: ?*anyopaque) !void {
            const value: *const catalog.Backend = @ptrCast(@alignCast(raw.?));
            try value.vtable.checkpoint(value.ptr);
        }
    };
    return try Graph.createLeaky(a, root, .{ .ptr = @constCast(&backend), .checkpoint = Check.pollBackend });
}

/// All facts are prepared before opening any physical cursor. No bound program
/// or shared scan is mutated, and the complete logical predicate is retained.
pub fn prepareScans(output: A, temporary: A, backend: catalog.Backend, bound: *const @import("describe.zig").BoundStatement, parameters: []const std.json.Value) ![]const catalog.StatementScan {
    const relation = bound.relation orelse return error.InvalidSqlBackendResponse;
    if (!backend.supports_expression_access_hints or !eligible(relation.scans)) return relation.scans;
    const Budget = @import("memory_budget.zig");
    var memory: Budget = .{ .backing = temporary, .limit = @min(4 * 1024 * 1024, if (Budget.headroom(temporary)) |available| available / 4 else 4 * 1024 * 1024) };
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    var state: State = .{ .a = arena.allocator(), .backing = temporary, .backend = backend, .relation = relation, .parameters = parameters, .graph = undefined };
    return state.run(output, bound) catch |err| {
        if (err == error.OutOfMemory and !state.publishing and memory.isExhausted()) return relation.scans;
        return err;
    };
}

const State = struct {
    a: A,
    backing: A,
    backend: catalog.Backend,
    relation: *const binding.Bound,
    parameters: []const std.json.Value,
    graph: *const Graph,
    publishing: bool = false,
    conditions: [64]std.ArrayList(catalog.Condition) = @splat(.empty),
    hints: [64]std.ArrayList(@import("../storage/relational_index.zig").ExpressionEqualityHint) = @splat(.empty),
    primary_keys: [64]?[]const u8 = @splat(null),

    fn checkpoint(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }
    fn run(self: *@This(), output: A, bound: *const @import("describe.zig").BoundStatement) ![]const catalog.StatementScan {
        const control: access.Control = .{ .ptr = self, .checkpoint = checkpoint, .clock_values = self.backend.clock_values };
        self.graph = self.relation.access_graph orelse try Graph.createLeaky(self.a, self.relation.root, control);
        if (bound.scalars.predicate) |*program| try self.prepareProgram(self.relation.root, null, null, program, bound.scalars.columns);
        for (self.graph.entries, 0..) |entry, index| {
            try checkpoint(self);
            switch (entry.node.operation) {
                .query => |*query| if (query.binding.scalars.predicate) |*program| try self.prepareProgram(query.source, @intCast(index), null, program, query.binding.scalars.columns),
                // ON predicates are pre-extension. Inner joins permit either
                // input; one-sided outer joins permit only the nonpreserved input.
                inline .join, .apply => |*join| if (join.condition) |*program| {
                    if (join.kind == .full) continue;
                    try self.prepareProgram(entry.node, @intCast(index), entry.node, program, try scalarColumns(self.a, entry.node.columns));
                },
                else => {},
            }
        }
        var changed = false;
        for (self.relation.scans, 0..) |_, index| if (self.conditions[index].items.len != 0 or self.hints[index].items.len != 0 or self.primary_keys[index] != null) {
            changed = true;
        };
        if (!changed) return self.relation.scans;
        self.publishing = true;
        const scans = try output.dupe(catalog.StatementScan, self.relation.scans);
        for (scans, 0..) |*scan, index| {
            const conditions = self.conditions[index].items;
            const hints = self.hints[index].items;
            if (conditions.len == 0 and hints.len == 0 and self.primary_keys[index] == null) continue;
            if (scan.request.primary_key == null) if (self.primary_keys[index]) |key| {
                scan.request.primary_key = try output.dupe(u8, key);
            };
            var owned_conditions: std.ArrayList(catalog.Condition) = .empty;
            try owned_conditions.appendSlice(output, scan.request.conditions);
            for (conditions) |condition| {
                if (owned_conditions.items.len == 256) break;
                var owned = condition;
                owned.value = try @import("../storage/typed_json.zig").clone(output, condition.value);
                try owned_conditions.append(output, owned);
            }
            scan.request.conditions = try owned_conditions.toOwnedSlice(output);
            var owned_hints: std.ArrayList(@import("../storage/relational_index.zig").ExpressionEqualityHint) = .empty;
            try owned_hints.appendSlice(output, scan.request.expression_equalities);
            for (hints) |hint| {
                if (owned_hints.items.len == 256) break;
                var owned = hint;
                owned.value = try @import("../storage/typed_json.zig").clone(output, hint.value);
                try owned_hints.append(output, owned);
            }
            scan.request.expression_equalities = try owned_hints.toOwnedSlice(output);
            scan.request.row_goal = null;
        }
        return scans;
    }

    fn prepareProgram(self: *@This(), source: *const Node, owner: ?u32, on: ?*const Node, program_: *const scalar.Program, columns: []const scalar.Column) !void {
        const lineage = try self.a.alloc(?Graph.Lineage, columns.len);
        @memset(lineage, null);
        var targets: u64 = 0;
        for (program_.required_columns) |ordinal| {
            if (ordinal >= columns.len) return error.InvalidSqlProgram;
            const found = (try self.graph.lineage(source, columns[ordinal].name, on, 0)) orelse continue;
            if (found.scan >= self.relation.scans.len) return error.InvalidSqlBackendResponse;
            const scan_node = self.graph.scans[found.scan] orelse return error.InvalidSqlBackendResponse;
            if (owner) |ancestor| if (!self.graph.dominates(ancestor, scan_node)) continue;
            if (on) |join_node| {
                const join: struct { kind: @import("ast.zig").JoinKind, left: *const Node, right: *const Node } = switch (join_node.operation) {
                    .join => |part| .{ .kind = part.kind, .left = part.left, .right = part.right },
                    .apply => |part| .{ .kind = part.kind, .left = part.left, .right = part.right },
                    else => unreachable,
                };
                const forbidden: ?*const Node = if (join.kind == .left) join.left else if (join.kind == .right) join.right else null;
                if (forbidden) |side| if (self.graph.nodeIndex(side)) |id| if (self.graph.dominates(id, scan_node)) continue;
            }
            const table = self.relation.scans[found.scan].table;
            const column = table.column(found.column) catch continue;
            if (!sameDomain(columns[ordinal], column)) continue;
            lineage[ordinal] = found;
            targets |= @as(u64, 1) << @intCast(found.scan);
        }
        for (self.relation.scans, 0..) |scan, index| {
            if (targets & (@as(u64, 1) << @intCast(index)) == 0 or scan.table.storage_mode != .relational or scan.table.external_base_source != null or scan.request.search != null) continue;
            const canonical = try self.a.dupe(scalar.Column, columns);
            const nullable = try self.a.alloc(bool, columns.len);
            for (canonical, lineage, nullable) |*column, item, *flag| {
                column.name = if (item) |found| if (found.scan == index) found.column else "" else "";
                column.aliases = &.{};
                flag.* = if (item) |found| found.scan == index and found.nullable else false;
            }
            const facts = try access.prepareAccess(self.a, self.backing, scan.table, program_, canonical, self.parameters, .{ .include_conditions = true, .null_extended_columns = nullable, .ptr = self, .checkpoint = checkpoint, .clock_values = self.backend.clock_values });
            if (self.primary_keys[index] == null) self.primary_keys[index] = facts.primary_key;
            for (facts.conditions) |condition| if (self.conditions[index].items.len < 256) try self.conditions[index].append(self.a, condition);
            for (facts.equalities) |hint| if (self.hints[index].items.len < 256) try self.hints[index].append(self.a, hint);
        }
    }
};

fn scalarColumns(a: A, columns: []const binding.Column) ![]const scalar.Column {
    const result = try a.alloc(scalar.Column, columns.len);
    for (columns, result) |column, *out| out.* = .{ .name = column.internal, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = column.nullable };
    return result;
}

test "SQL relation access binds canonical predicates before capture with outer join and shared producer fences" {
    const Fixture = struct {
        table: catalog.Table,
        fn resolve(raw: *anyopaque, _: A, _: @import("ast.zig").Name, _: catalog.Action) !catalog.Table {
            return @as(*@This(), @ptrCast(@alignCast(raw))).table;
        }
        fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedScan;
        }
        fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var owner = std.heap.ArenaAllocator.init(a);
    defer owner.deinit();
    const owned = owner.allocator();
    var table: catalog.Table = .{ .id = 1, .physical_name = "items", .schema_version = 1, .columns = &.{
        .{ .name = "label", .path = "label", .type = .string, .element_type = .text },
        .{ .name = "tenant", .path = "tenant", .type = .integer, .element_type = .int64 },
        .{ .name = "active", .path = "active", .type = .boolean, .element_type = .boolean },
    } };
    const keys = try owned.alloc(catalog.IndexDefinition.Key, 2);
    for (keys, [_][]const u8{
        \\{"op":"lower_ascii","args":[{"op":"column","column":"label"}]}
        ,
        \\{"op":"coalesce","args":[{"op":"column","column":"label"},{"op":"literal","type":"string","value":"empty"}]}
    }) |*key_, text| {
        key_.* = .{ .column = "", .expression = try std.json.parseFromSliceLeaky(std.json.Value, owned, text, .{}), .result_type = .string, .collation = null, .direction = .asc, .nulls = .default };
        key_.identity = try key_.bindIdentity(owned, try table.expressionSchemaLeaky(owned));
    }
    const definitions = try owned.alloc(catalog.IndexDefinition, 2);
    for (definitions, keys, 0..) |*definition, _, index| definition.* = .{ .name = if (index == 0) "lower_label" else "default_label", .keys = keys[index..][0..1], .include_columns = &.{}, .where = &.{} };
    table.index_definitions = definitions;
    var fixture: Fixture = .{ .table = table };
    const backend: catalog.Backend = .{ .ptr = &fixture, .supports_expression_access_hints = true, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
    const Case = struct { sql: []const u8, hints: []const usize, conditions: []const usize, primary: []const ?[]const u8 = &.{} };
    const cases = [_]Case{
        .{ .sql = "SELECT p.label FROM items p WHERE p._id='one'", .hints = &.{0}, .conditions = &.{0}, .primary = &.{"one"} },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON q._id='one'", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 }, .primary = &.{ null, "one" } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON p._id='one'", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p JOIN items q ON p.tenant=q.tenant WHERE p.tenant=7 AND p.active AND lower(p.label)='ready'", .hints = &.{ 1, 0 }, .conditions = &.{ 2, 0 } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON true WHERE lower(q.label)='ready'", .hints = &.{ 0, 1 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON true WHERE coalesce(q.label,'empty')='empty'", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON true WHERE q.label IS NULL", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON true WHERE q.label IS NOT NULL", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 1 } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON coalesce(q.label,'empty')='empty'", .hints = &.{ 0, 1 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p LEFT JOIN items q ON coalesce(p.label,'empty')='empty'", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p RIGHT JOIN items q ON coalesce(p.label,'empty')='empty'", .hints = &.{ 1, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p FULL JOIN items q ON coalesce(p.label,'empty')='empty'", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "SELECT p.label FROM items p FULL JOIN items q ON true WHERE lower(p.label)='ready'", .hints = &.{ 1, 0 }, .conditions = &.{ 0, 0 } },
        .{ .sql = "WITH c AS MATERIALIZED (SELECT label FROM items) SELECT p.label FROM c p CROSS JOIN c q WHERE lower(p.label)='ready'", .hints = &.{0}, .conditions = &.{0} },
        .{ .sql = "WITH c AS MATERIALIZED (SELECT label FROM items WHERE lower(label)='ready') SELECT p.label FROM c p CROSS JOIN c q", .hints = &.{1}, .conditions = &.{0} },
        .{ .sql = "SELECT p.label FROM (SELECT label,tenant FROM items) p WHERE lower(p.label)='ready'", .hints = &.{1}, .conditions = &.{0} },
        .{ .sql = "SELECT p.label FROM (SELECT label FROM items LIMIT 1) p WHERE lower(p.label)='ready'", .hints = &.{0}, .conditions = &.{0} },
        .{ .sql = "SELECT p.label FROM items p WHERE p.tenant=7 OR lower(p.label)='ready'", .hints = &.{0}, .conditions = &.{0} },
        .{ .sql = "SELECT p.label FROM items p CROSS JOIN items q WHERE lower(p.label||q.label)='ready'", .hints = &.{ 0, 0 }, .conditions = &.{ 0, 0 } },
    };
    for (cases) |case| {
        var compiled = try @import("compiler.zig").compile(a, case.sql, .{});
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const bound = try @import("describe.zig").bind(arena.allocator(), backend, &compiled, &.{});
        const prepared = try prepareScans(arena.allocator(), a, backend, &bound, &.{});
        try std.testing.expectEqual(case.hints.len, prepared.len);
        for (prepared, case.hints, case.conditions, 0..) |scan, hints, conditions, index| {
            if (scan.request.expression_equalities.len != hints or scan.request.conditions.len != conditions) std.debug.print("relation access mismatch scan={} hints={} conditions={}: {s}\n", .{ index, scan.request.expression_equalities.len, scan.request.conditions.len, case.sql });
            try std.testing.expectEqual(hints, scan.request.expression_equalities.len);
            try std.testing.expectEqual(conditions, scan.request.conditions.len);
            const point = if (case.primary.len == 0) null else case.primary[index];
            if (point) |key| try std.testing.expectEqualStrings(key, scan.request.primary_key.?) else try std.testing.expect(scan.request.primary_key == null);
            for (scan.request.conditions) |condition| {
                try std.testing.expect(std.mem.eql(u8, condition.column, "tenant") or std.mem.eql(u8, condition.column, "active") or std.mem.eql(u8, condition.column, "label"));
            }
        }
        for (bound.relation.?.scans) |scan| {
            try std.testing.expectEqual(@as(usize, 0), scan.request.expression_equalities.len);
            try std.testing.expectEqual(@as(usize, 0), scan.request.conditions.len);
            try std.testing.expect(scan.request.primary_key == null);
        }
    }
    // Native REST schemas may leave builtin spelling implicit. Normalize the
    // same SQL domains without weakening numeric widths or modifiers.
    var implicit = table;
    const implicit_columns = try owned.dupe(catalog.Column, table.columns);
    for (implicit_columns) |*column| column.element_type = null;
    implicit.columns = implicit_columns;
    const implicit_keys = try owned.dupe(catalog.IndexDefinition.Key, keys);
    const implicit_definitions = try owned.dupe(catalog.IndexDefinition, definitions);
    for (implicit_keys, implicit_definitions, 0..) |*key_, *definition, index| {
        key_.identity = try key_.bindIdentity(owned, try implicit.expressionSchemaLeaky(owned));
        definition.keys = implicit_keys[index..][0..1];
    }
    implicit.index_definitions = implicit_definitions;
    fixture.table = implicit;
    var compiled = try @import("compiler.zig").compile(a, cases[3].sql, .{});
    defer compiled.deinit();
    const bound = try @import("describe.zig").bind(owned, backend, &compiled, &.{});
    const Faults = struct {
        fn graph(backing: A, source: *const Node) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const deterministic: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            var result = std.heap.ArenaAllocator.init(deterministic);
            defer result.deinit();
            const value = try Graph.createLeaky(result.allocator(), source, .{});
            const root = value.nodeIndex(source).?;
            try std.testing.expect(value.dominates(root, value.scans[0].?));
            try std.testing.expect(value.dominates(root, value.scans[1].?));
            try std.testing.expect(!value.dominates(value.scans[0].?, value.scans[1].?));
        }
        fn run(backing: A, provider: catalog.Backend, input: *const @import("describe.zig").BoundStatement) !void {
            var vtable = backing.vtable.*;
            vtable.resize = A.noResize;
            vtable.remap = A.noRemap;
            const deterministic: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            var result = std.heap.ArenaAllocator.init(deterministic);
            defer result.deinit();
            const scans = try prepareScans(result.allocator(), deterministic, provider, input, &.{});
            try std.testing.expectEqual(@as(usize, 1), scans[0].request.expression_equalities.len);
            try std.testing.expectEqual(@as(usize, 2), scans[0].request.conditions.len);
        }
        fn cancel(_: *anyopaque) !void {
            return error.SqlQueryCanceled;
        }
    };
    try std.testing.checkAllAllocationFailures(a, Faults.graph, .{bound.relation.?.root});
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{ backend, &bound });
    var uncached = bound;
    var uncached_relation = bound.relation.?.*;
    uncached_relation.access_graph = null;
    uncached.relation = &uncached_relation;
    try std.testing.checkAllAllocationFailures(a, Faults.run, .{ backend, &uncached });
    var canceled = backend;
    var callbacks = backend.vtable.*;
    callbacks.checkpoint = Faults.cancel;
    canceled.vtable = &callbacks;
    try std.testing.expectError(error.SqlQueryCanceled, prepareScans(owned, a, canceled, &bound, &.{}));
    var tiny: @import("memory_budget.zig") = .{ .backing = a, .limit = 256 };
    var no_output = std.heap.FixedBufferAllocator.init(&.{});
    const declined = try prepareScans(no_output.allocator(), tiny.allocator(), backend, &bound, &.{});
    try std.testing.expect(declined.ptr == bound.relation.?.scans.ptr);
    try std.testing.expect(!tiny.isExhausted());
    try std.testing.expectEqual(@as(usize, 0), tiny.live);
}
