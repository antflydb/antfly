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
        // Dynamic procedure sources and writes conservatively keep all owners.
        if (T == ast.Procedure) self.full = true;
        if (T == ast.Clause) switch (value.operation) {
            .create, .merge, .set, .remove, .delete => self.full = true,
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
        .{ .query = "RETURN 1", .count = 0 },
        .{ .query = "MATCH(n:A) RETURN n", .count = 1 },
        .{ .query = "MATCH(n:A)-[:R]->() RETURN n", .count = null },
        .{ .query = "MATCH(n:A)-[:R]->(m:B) RETURN n", .count = 2 },
        .{ .query = "MATCH(n) RETURN n", .count = null },
        .{ .query = "MATCH(n:A) RETURN EXISTS { MATCH(m:C) RETURN m }", .count = 2 },
        .{ .query = "MATCH(n:A) RETURN n UNION MATCH(n:C) RETURN n", .count = 2 },
        .{ .query = "CREATE(n:A) RETURN n", .count = null },
        .{ .query = "CALL antfly.search('a',{query:'x'}) YIELD node RETURN node", .count = null },
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
