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

const std = @import("std");
const root = @import("root.zig");
const graph: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
    .{ .name = "D", .table = "docs" },
    .{ .name = "Wide", .table = "docs", .predicate = &.{.{ .name = "body", .value = .{ .string = "hidden" } }} },
}, .relationships = &.{.{ .name = "R", .stored_type = "link", .index = "links", .source_label = "D", .target_label = "D" }} };
test "read demand separates nullness values and catalog membership across aliases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(n:D) RETURN sum(CASE WHEN n.body IS NULL THEN 0 ELSE 1 END)",
        "MATCH(n:D) WITH n AS x RETURN x.body IS NULL",
        "MATCH(n:D) WHERE n.body IS NOT NULL RETURN sum(1)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const plan = try root.read_fields.analyze(a, compiled.query, graph);
        try expectNames(&.{"body"}, plan.demand.nodes.fields.?);
        try expectNames(&.{"body"}, plan.demand.nodes.presence);
        try expectNames(&.{"D"}, plan.demand.labels.?);
    }
    for ([_][]const u8{ "MATCH(n:D) RETURN n.body IS NULL,n.body", "MATCH(n:D) RETURN labels(n),n.body IS NULL" }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const plan = try root.read_fields.analyze(a, compiled.query, graph);
        try std.testing.expectEqual(@as(usize, 0), plan.demand.nodes.presence.len);
    }
}
test "read demand keeps node relationship and whole entity observers distinct" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "n.body IS NULL,r.rank", "n", "r", "p" }, 0..) |projection, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, try std.fmt.allocPrint(a, "MATCH p=(n:D)-[r:R]->(b:D) RETURN {s}", .{projection}), .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand;
        if (i == 0) {
            try expectNames(&.{"body"}, demand.nodes.presence);
            try expectNames(&.{"rank"}, demand.relationships.fields.?);
        }
        if (i == 1 or i == 3) try std.testing.expect(demand.nodes.fields == null and demand.labels == null);
        if (i == 2 or i == 3) try std.testing.expect(demand.relationships.fields == null);
    }
}

fn expectNames(expected: []const []const u8, actual: []const []const u8) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |left, right| try std.testing.expectEqualStrings(left, right);
}

test "read demand certifies unlabeled coverage and score-only search membership" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(n) RETURN sum(1)",
        "CALL antfly.search('docs',{query:'body:needle',mode:'text',limit:200}) YIELD score RETURN count(*)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand;
        try expectNames(&.{"D"}, demand.labels.?);
        try expectNames(&.{}, demand.nodes.fields.?);
    }
}

test "read demand partitions complete entities and membership by physical table" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const mapped: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "D", .table = "docs" },
        .{ .name = "Wide", .table = "docs", .predicate = &.{.{ .name = "body", .value = .{ .string = "hidden" } }} },
        .{ .name = "C", .table = "categories" },
    }, .relationships = &.{} };
    for ([_][]const u8{
        "MATCH(d:D),(c:C) RETURN c,sum(d.score)",
        "MATCH(d:D),(c:C) RETURN labels(c),sum(d.score)",
        "MATCH(d:D),(c:C) WITH d AS x,c AS y RETURN y,sum(x.score)",
        "MATCH(d:D) WHERE EXISTS { MATCH(c:C) RETURN c } RETURN sum(d.score)",
        "MATCH(d:D) CALL antfly.search('categories',{query:'id:c1',mode:'text'}) YIELD node AS c RETURN c,sum(d.score)",
    }, 0..) |query, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
        const docs = demand.table("docs").?;
        try expectNames(&.{"score"}, docs.nodes.fields.?);
        try expectNames(&.{"D"}, docs.labels.?);
        const categories = demand.table("categories").?;
        if (i != 1) try std.testing.expect(categories.nodes.fields == null);
        try std.testing.expect(categories.labels == null);
    }
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(d:D),(c:Wide) RETURN c,sum(d.score)", .{}, &diagnostic);
    defer compiled.deinit();
    // Both aliases can name the same physical identity: whole wins globally
    // within that table, regardless of which alias happens to be read first.
    const shared = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
    try std.testing.expect(shared.table("docs").?.nodes.fields == null);
}

test "read demand normalizes literal nested paths with ancestor upgrades" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(n:D) RETURN n.payload.rank,n.payload.body IS NOT NULL",
        "MATCH(n:D) RETURN n.payload.rank,n.payload",
        "MATCH(n:D) RETURN n.payload.child.rank,n.payload.child,n.payload.other IS NULL",
    }, 0..) |query, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        if (i == 1) {
            try std.testing.expectEqual(@as(usize, 0), demand.projection.len);
            continue;
        }
        const projection = demand.projection[0];
        try std.testing.expect(projection.mode == .children);
        try std.testing.expectEqualStrings("payload", projection.name);
        try std.testing.expectEqual(@as(usize, 2), projection.children.len);
        if (i == 0) {
            try std.testing.expectEqualStrings("body", projection.children[0].name);
            try std.testing.expect(projection.children[0].mode == .presence);
            try std.testing.expect(projection.children[1].mode == .value);
        } else {
            try std.testing.expectEqualStrings("child", projection.children[0].name);
            try std.testing.expect(projection.children[0].mode == .value);
        }
    }
}

test "read demand retains ancestor presence validation alongside nested values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(n:D) RETURN n.payload IS NULL,n.payload.child IS NULL,n.payload.child.rank", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
    try std.testing.expect(demand.projection[0].validate);
    try std.testing.expect(demand.projection[0].children[0].validate);
}

test "read demand retains intermediate table properties of returned variable paths" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const mapped: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "A", .table = "start" }, .{ .name = "C", .table = "middle" }, .{ .name = "B", .table = "finish" },
    }, .relationships = &.{
        .{ .name = "R", .stored_type = "r", .index = "links", .source_label = "A", .target_label = "C" },
        .{ .name = "S", .stored_type = "s", .index = "links", .source_label = "C", .target_label = "B" },
    } };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH p=(a:A)-[:R|S*2]->(b:B) RETURN p", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
    for ([_][]const u8{ "start", "middle", "finish" }) |table| {
        try std.testing.expect(demand.table(table).?.nodes.fields == null);
        try std.testing.expect(demand.table(table).?.labels == null);
    }
}

test "read demand preserves correlated bindings across optional pattern constraints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const mapped: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "D", .table = "docs" }, .{ .name = "C", .table = "categories" },
    }, .relationships = &.{} };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(d:D) OPTIONAL MATCH(d:C) RETURN sum(d.score)", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
    try expectNames(&.{"score"}, demand.table("docs").?.nodes.fields.?);
    try expectNames(&.{}, demand.table("categories").?.nodes.fields.?);
}
