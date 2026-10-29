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

//! Data dependencies only. Catalog and ACL validation retain the full graph.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const Plan = struct {
    graph: catalog.Graph,
    tables: std.StringHashMapUnmanaged(void) = .empty,
    a: std.mem.Allocator,
    full: bool = false,
    fn table(self: *Plan, label: []const u8) !void {
        if (self.graph.label(label)) |mapped| try self.tables.put(self.a, mapped.table, {});
    }
    fn walk(self: *Plan, value: anytype) anyerror!void {
        const T = @TypeOf(value);
        if (T == ast.Path) {
            for (value.nodes) |node| {
                // The executor may anchor a traversal at any node. An unlabeled
                // node can therefore read every catalog table before rejecting
                // an incompatible edge mapping; preserve that execution domain.
                if (node.labels.len == 0) self.full = true;
                for (node.labels) |label| try self.table(label);
                try self.walk(node.properties);
            }
            for (value.edges) |edge| {
                for (self.graph.relationships) |mapping| {
                    const matches = edge.types.len == 0 or for (edge.types) |name| {
                        if (std.mem.eql(u8, name, mapping.name)) break true;
                    } else false;
                    if (matches) {
                        try self.table(mapping.source_label);
                        try self.table(mapping.target_label);
                    }
                }
                try self.walk(edge.properties);
            }
            return;
        }
        // Preserve runtime logical/physical name resolution, including scoped
        // literal targets. Unknown or dynamic targets retain runtime diagnostics.
        if (T == ast.Procedure) {
            if (std.mem.eql(u8, value.name, "antfly.search") and value.args.len == 2) {
                if (searchTarget(value.args[0])) |target| {
                    var found = false;
                    for (self.graph.labels) |label| {
                        const logical = std.mem.eql(u8, if (label.logical_table.len == 0) label.table else label.logical_table, target.name) and
                            std.mem.eql(u8, label.database, target.database) and std.mem.eql(u8, label.namespace, target.namespace);
                        if (logical or (target.physical_alias and std.mem.eql(u8, label.table, target.name))) {
                            found = true;
                            try self.tables.put(self.a, label.table, {});
                        }
                    }
                    if (!found) self.full = true;
                } else self.full = true;
            } else self.full = true;
        }
        if (T == ast.Clause) switch (value.operation) {
            // Deletion can inspect incoming edges in any source table,
            // including DETACH cleanup and derived relationship producers.
            .delete => self.full = true,
            else => {},
        };
        switch (@typeInfo(T)) {
            .pointer => |p| {
                if (p.size == .one) try self.walk(value.*);
                if (p.size == .slice and p.child != u8) for (value) |item| try self.walk(item);
            },
            .@"struct" => |s| inline for (s.field_names) |name| try self.walk(@field(value, name)),
            .@"union" => switch (value) {
                inline else => |payload| try self.walk(payload),
            },
            .optional => if (value) |item| {
                try self.walk(item);
            },
            else => {},
        }
    }
};
const SearchTarget = struct { name: []const u8, database: []const u8 = "default", namespace: []const u8 = "public", physical_alias: bool = false };
fn searchTarget(expr: *const ast.Expr) ?SearchTarget {
    if (expr.value == .literal and expr.value.literal == .string) return .{ .name = expr.value.literal.string, .physical_alias = true };
    if (expr.value != .map) return null;
    var result: SearchTarget = .{ .name = "" };
    var seen: u8 = 0;
    for (expr.value.map) |entry| {
        const bit: u8 = if (std.mem.eql(u8, entry.name, "table")) 1 else if (std.mem.eql(u8, entry.name, "database")) 2 else if (std.mem.eql(u8, entry.name, "namespace")) 4 else continue;
        if (seen & bit != 0 or entry.value.value != .literal) return null;
        seen |= bit;
        const literal = entry.value.value.literal;
        if (literal == .null and bit != 1) continue;
        if (literal != .string) return null;
        switch (bit) {
            1 => result.name = literal.string,
            2 => result.database = literal.string,
            4 => result.namespace = literal.string,
            else => unreachable,
        }
    }
    return if (seen & 1 != 0) result else null;
}
/// Null means unknown: capture the entire cohort. Names are physical tables.
pub fn required(a: std.mem.Allocator, query: ast.Query, graph: catalog.Graph) !?[]const []const u8 {
    var plan: Plan = .{ .a = a, .graph = graph };
    defer plan.tables.deinit(a);
    try plan.walk(query);
    if (plan.full) return null;
    const result = try a.alloc([]const u8, plan.tables.count());
    var keys = plan.tables.keyIterator();
    for (result) |*out| out.* = try a.dupe(u8, keys.next().?.*);
    return result;
}
pub fn contains(tables: ?[]const []const u8, name: []const u8) bool {
    for (tables orelse return true) |table| if (std.mem.eql(u8, name, table)) return true;
    return false;
}

test "dependencies retain nested patterns, unions, procedures and write cohorts" {
    const a = std.testing.allocator;
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "A", .table = "a" }, .{ .name = "B", .table = "b" }, .{ .name = "C", .table = "c" } }, .relationships = &.{.{ .name = "R", .index = "r", .source_label = "A", .target_label = "B", .stored_type = "r" }} };
    const Case = struct { query: []const u8, count: ?usize };
    for ([_]Case{
        .{ .query = "MATCH(n:A) SET n.x=1 RETURN n", .count = 1 },
        .{ .query = "MERGE(n:A {x:1}) RETURN n", .count = 1 },
        .{ .query = "MATCH(n:A) DETACH DELETE n", .count = null },
        .{ .query = "CALL antfly.search($table,{query:'x'}) YIELD node RETURN node", .count = null },
        .{ .query = "CALL antfly.search({table:'a',namespace:'public'},{query:'x'}) YIELD node RETURN node", .count = 1 },
        .{ .query = "CALL antfly.search({table:'a',database:null},{query:'x'}) YIELD node RETURN node", .count = 1 },
        .{ .query = "CALL antfly.search({table:$table},{query:'x'}) YIELD node RETURN node", .count = null },
        .{ .query = "CALL antfly.search('missing',{query:'x'}) YIELD node RETURN node", .count = null },
        .{ .query = "RETURN 1", .count = 0 },
        .{ .query = "MATCH(n:A) RETURN n", .count = 1 },
        .{ .query = "MATCH(n:A)-[:R]->() RETURN n", .count = null },
        .{ .query = "MATCH(n:A)-[:R]->(m:B) RETURN n", .count = 2 },
        .{ .query = "MATCH(n) RETURN n", .count = null },
        .{ .query = "MATCH(n:A) RETURN EXISTS { MATCH(m:C) RETURN m }", .count = 2 },
        .{ .query = "MATCH(n:A) RETURN n UNION MATCH(n:C) RETURN n", .count = 2 },
        .{ .query = "CREATE(n:A) RETURN n", .count = 1 },
        .{ .query = "CALL antfly.search('a',{query:'x'}) YIELD node RETURN node", .count = 1 },
    }) |case| {
        var diagnostic: @import("compiler.zig").Diagnostic = .{};
        var compiled = try @import("compiler.zig").compile(a, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const found = try required(arena.allocator(), compiled.query, graph);
        if (case.count) |count| try std.testing.expectEqual(count, found.?.len) else try std.testing.expect(found == null);
    }
}

/// Physical adjacency dependencies. Null deliberately keeps every projection
/// for writes/procedures whose storage dependencies cannot be proved here.
/// Primary rows, scalar layouts, catalog and policy fences are always retained.
pub fn indexesForTable(a: std.mem.Allocator, query: ast.Query, graph: catalog.Graph, table: []const u8) !?[]const []const u8 {
    const Demand = struct {
        a: std.mem.Allocator,
        graph: catalog.Graph,
        table: []const u8,
        names: std.StringHashMapUnmanaged(void) = .empty,
        full: bool = false,
        fn walk(self: *@This(), value: anytype) anyerror!void {
            const T = @TypeOf(value);
            if (T == ast.Clause) switch (value.operation) {
                .match, .project, .unwind, .subquery => {},
                else => self.full = true,
            };
            if (T == ast.Edge) {
                for (self.graph.relationships) |mapping| {
                    if (!std.mem.eql(u8, self.graph.label(mapping.source_label).?.table, self.table)) continue;
                    const matches = value.types.len == 0 or for (value.types) |name| {
                        if (std.mem.eql(u8, name, mapping.name)) break true;
                    } else false;
                    if (matches) try self.names.put(self.a, mapping.index, {});
                }
            }
            switch (@typeInfo(T)) {
                .pointer => |p| {
                    if (p.size == .one) try self.walk(value.*);
                    if (p.size == .slice and p.child != u8) for (value) |item| try self.walk(item);
                },
                .@"struct" => |fields| inline for (fields.field_names) |name| try self.walk(@field(value, name)),
                .@"union" => switch (value) {
                    inline else => |payload| try self.walk(payload),
                },
                .optional => if (value) |item| {
                    try self.walk(item);
                },
                else => {},
            }
        }
    };
    var demand: Demand = .{ .a = a, .graph = graph, .table = table };
    defer demand.names.deinit(a);
    try demand.walk(query);
    if (demand.full) return null;
    const names = try a.alloc([]const u8, demand.names.count());
    var initialized: usize = 0;
    errdefer {
        for (names[0..initialized]) |name| a.free(name);
        a.free(names);
    }
    var keys = demand.names.keyIterator();
    for (names) |*name| {
        name.* = try a.dupe(u8, keys.next().?.*);
        initialized += 1;
    }
    std.mem.sort([]const u8, names, {}, struct {
        fn less(_: void, lhs: []const u8, rhs: []const u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.less);
    return names;
}

test "physical dependencies retain nested and wildcard edges and fail broad for procedures and writes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "A", .table = "a" }, .{ .name = "B", .table = "b" } }, .relationships = &.{ .{ .name = "R", .index = "r", .source_label = "A", .target_label = "B", .stored_type = "r" }, .{ .name = "S", .index = "s", .source_label = "B", .target_label = "A", .stored_type = "s" } } };
    const Case = struct { query: []const u8, a_count: ?usize, b_count: ?usize };
    for ([_]Case{
        .{ .query = "MATCH(n:A) RETURN n", .a_count = 0, .b_count = 0 },
        .{ .query = "MATCH(n:A)-[:R]->(m:B) RETURN m", .a_count = 1, .b_count = 0 },
        .{ .query = "MATCH(n:A)<-[:R]-(m:B) RETURN m", .a_count = 1, .b_count = 0 },
        .{ .query = "MATCH(n)-[]-(m) RETURN n", .a_count = 1, .b_count = 1 },
        .{ .query = "MATCH ()-[r]->() RETURN r LIMIT 0", .a_count = 1, .b_count = 1 },
        .{ .query = "MATCH(n:A) RETURN EXISTS { MATCH(m:B)-[:S]->(x:A) RETURN x }", .a_count = 0, .b_count = 1 },
        .{ .query = "MATCH(n:A)-[:R]->(m:B) RETURN n UNION MATCH(n:B)-[:S]->(m:A) RETURN n", .a_count = 1, .b_count = 1 },
        .{ .query = "CALL antfly.search($table,{query:'x'}) YIELD node RETURN node", .a_count = null, .b_count = null },
        .{ .query = "MATCH(n:A) SET n.x=1 RETURN n", .a_count = null, .b_count = null },
        .{ .query = "MATCH(n:A) DETACH DELETE n", .a_count = null, .b_count = null },
    }) |case| {
        var diagnostic: @import("compiler.zig").Diagnostic = .{};
        var compiled = try @import("compiler.zig").compile(a, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        const left = try indexesForTable(a, compiled.query, graph, "a");
        const right = try indexesForTable(a, compiled.query, graph, "b");
        try std.testing.expectEqual(case.a_count, if (left) |names| names.len else null);
        try std.testing.expectEqual(case.b_count, if (right) |names| names.len else null);
    }
}
