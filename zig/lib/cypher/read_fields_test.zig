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

test "read demand scopes list locals while retaining their outer inputs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(d:D) RETURN [d IN [1,2] | d]",
        "MATCH(d:D) RETURN [d IN [{payload:{rank:7}}] | d.payload.rank]",
        "MATCH(d:D) RETURN all(d IN [1,2] WHERE d>0)",
        "MATCH(d:D) RETURN reduce(d=0,x IN [1,2] | d+x)",
        "MATCH(d:D) RETURN reduce(x=0,d IN [1,2] | x+d)",
        "MATCH(d:D) RETURN [d IN [1] | [d IN [2] | d]]",
        "MATCH(d:D) WITH d{answer:42} WHERE d.rank IS NULL RETURN d.answer",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand;
        try expectNames(&.{}, demand.table("docs").?.nodes.fields.?);
    }
    for ([_][]const u8{
        "MATCH(d:D) RETURN [d IN d.items | d]",
        "MATCH(d:D) RETURN all(d IN d.items WHERE d>0)",
        "MATCH(d:D) RETURN reduce(d=d.items,x IN [1] | d)",
        "MATCH(d:D) RETURN [d IN [1] | d],d.items",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand;
        try expectNames(&.{"items"}, demand.table("docs").?.nodes.fields.?);
    }
}

test "read demand propagates property lineage through WITH and upgrades observers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(d:D) WITH d.payload AS p RETURN sum(p.rank)",
        "MATCH(d:D) WITH d.payload AS p WITH p AS q RETURN sum(q.rank)",
        "MATCH(d:D) WITH d.payload AS p WITH p.child AS q RETURN sum(q.rank)",
        "MATCH(d:D) WITH d.payload AS p RETURN p.rank IS NULL",
        "MATCH(d:D) WITH d.payload AS p WITH * RETURN p.rank",
    }, 0..) |query, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        const tree = demand.projection[0];
        try std.testing.expect(tree.mode == .children);
        const leaf = if (i == 2) tree.children[0].children[0] else tree.children[0];
        try std.testing.expectEqualStrings("rank", leaf.name);
        try std.testing.expect(if (i == 3) leaf.mode == .presence else leaf.mode == .value);
    }
    for ([_][]const u8{
        "MATCH(d:D) WITH d.payload AS p RETURN p,p.rank",
        "MATCH(d:D) WITH DISTINCT d.payload AS p RETURN p.rank",
        "MATCH(d:D) WITH d.payload AS p,count(*) AS c RETURN p.rank,c",
        "MATCH(d:D) WITH d.payload AS p RETURN p.rank ORDER BY p",
        "MATCH(d:D) WITH d.payload AS p RETURN *",
        "MATCH(d:D) WITH d.payload AS p RETURN 1",
        "MATCH(d:D) WITH d.payload AS p RETURN size(p)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        try std.testing.expectEqual(@as(usize, 0), demand.projection.len);
    }
}

test "read demand infers endpoint domains without narrowing correlated optional bindings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const mapped: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "D", .table = "docs" }, .{ .name = "C", .table = "categories" }, .{ .name = "X", .table = "other" },
    }, .relationships = &.{
        .{ .name = "R", .stored_type = "r", .index = "links", .source_label = "D", .target_label = "C" },
        .{ .name = "S", .stored_type = "s", .index = "links", .source_label = "C", .target_label = "X" },
    } };
    for ([_][]const u8{
        "MATCH(d:D)-[:R]->(c) RETURN c,sum(d.score)",
        "MATCH(c)<-[:R]-(d:D) RETURN c,sum(d.score)",
        "MATCH(d:D)-[:R*1..3]->(c) RETURN c,sum(d.score)",
        "MATCH(d:D)-[:R]->(c)-[:S]->(x:X) RETURN c,sum(d.score)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
        try expectNames(&.{"score"}, demand.table("docs").?.nodes.fields.?);
        try std.testing.expect(demand.table("categories").?.nodes.fields == null);
        try expectNames(&.{}, demand.table("other").?.nodes.fields.?);
    }
    for ([_][]const u8{
        "MATCH(d:D)-[:R*0..1]->(c) RETURN c",
        "MATCH(d:D)-[:R]-(c) RETURN c",
        "MATCH(c:D) OPTIONAL MATCH(d:D)-[:R]->(c) RETURN c",
        "MATCH(c) OPTIONAL MATCH(d:D)-[:R]->(c) RETURN c",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
        try std.testing.expect(demand.table("docs").?.nodes.fields == null);
    }
}

test "read demand forwards relationship property aliases without widening node fields" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(d:D)-[r:R]->(e:D) WITH r.payload AS p RETURN sum(p.rank)", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand;
    try expectNames(&.{}, demand.table("docs").?.nodes.fields.?);
    try expectNames(&.{"payload"}, demand.relationships.fields.?);
    const tree = demand.relationships.projection[0];
    try std.testing.expect(tree.mode == .children);
    try std.testing.expectEqualStrings("rank", tree.children[0].name);
}

test "read demand normalizes literal bracket selectors and nested map projections" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(d:D) RETURN sum(d['payload']['rank'])",
        "MATCH(d:D) RETURN sum(d.payload['rank'])",
        "MATCH(d:D) WITH d['payload'] AS p RETURN sum(p.rank)",
        "MATCH(d:D) RETURN d.payload{.rank}",
        "MATCH(d:D) WITH d.payload AS p RETURN p{.rank}",
        "MATCH(d:D) RETURN d['payload']{.rank}",
        "MATCH(d:D) WITH d.payload RETURN `d.payload`.rank",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        const tree = demand.projection[0];
        try std.testing.expect(tree.mode == .children);
        try std.testing.expectEqualStrings("rank", tree.children[0].name);
        try std.testing.expect(tree.children[0].mode == .value);
    }
    for ([_][]const u8{
        "MATCH(d:D) RETURN count(d['payload']['rank'])",
        "MATCH(d:D) RETURN d['payload'].rank IS NULL",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try std.testing.expect(demand.projection[0].children[0].mode == .presence);
    }
    for ([_][]const u8{
        "MATCH(d:D) RETURN d.payload{}",
        "MATCH(d:D) RETURN d.payload{answer:42}",
        "MATCH(d:D) WITH d.payload AS p RETURN p{answer:42}",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        try std.testing.expect(demand.projection[0].mode == .children);
        try std.testing.expectEqual(@as(usize, 0), demand.projection[0].children.len);
    }
    {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, "MATCH(d:D) RETURN d.payload{},d.payload IS NULL", .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try std.testing.expect(demand.projection[0].mode == .children);
        try std.testing.expect(demand.projection[0].validate);
    }
    for ([_][]const u8{
        "MATCH(d:D) RETURN d.payload{.*,.rank}",
        "MATCH(d:D) WITH d.payload AS p RETURN p{.*}",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        try std.testing.expectEqual(@as(usize, 0), demand.projection.len);
    }
}

test "read demand exports CALL lineage and retains complete value barriers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "CALL { MATCH(d:D) RETURN d } RETURN sum(d.payload.rank)",
        "CALL { MATCH(d:D) RETURN d.payload AS p } RETURN sum(p.rank)",
        "MATCH(d:D) CALL { WITH d RETURN d.payload AS p } RETURN sum(p.rank)",
        "CALL { CALL { MATCH(d:D) RETURN d } RETURN d AS x } RETURN sum(x.payload.rank)",
        "CALL { MATCH(d:D) RETURN d.payload } RETURN sum(`d.payload`.rank)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        try std.testing.expect(demand.projection[0].mode == .children);
        try std.testing.expectEqualStrings("rank", demand.projection[0].children[0].name);
    }
    for ([_][]const u8{
        "CALL { MATCH(d:D) RETURN DISTINCT d } RETURN d.payload.rank",
        "CALL { MATCH(d:D) RETURN d,count(*) AS c } RETURN d.payload.rank,c",
        "CALL { MATCH(d:D) RETURN d ORDER BY d } RETURN d.payload.rank",
        "CALL { MATCH(d:D) RETURN d } RETURN d",
    }, 0..) |query, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        if (i == 3) try std.testing.expect(demand.fields == null) else try expectNames(&.{"payload"}, demand.fields.?);
    }
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "CALL { MATCH(d:D) RETURN d.payload AS p } RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
    try expectNames(&.{"payload"}, demand.fields.?);
    try std.testing.expectEqual(@as(usize, 0), demand.projection.len);
}

test "read demand keeps identity grouping narrow and complete entity returns observable" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(d:D) WITH DISTINCT d RETURN count(*)",
        "MATCH(d:D) WITH d,count(*) AS n RETURN count(*)",
        "MATCH(d:D) WITH d ORDER BY d RETURN count(*)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        try expectNames(&.{}, (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes.fields.?);
    }
}

test "read demand partitions relationship metadata by physical identity domain" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mapped = graph;
    mapped.relationships = &.{
        .{ .name = "R", .stored_type = "r", .index = "links", .source_label = "D", .target_label = "D" },
        .{ .name = "S", .stored_type = "s", .index = "links", .source_label = "D", .target_label = "D" },
        .{ .name = "Alias", .stored_type = "r", .index = "links", .source_label = "D", .target_label = "D" },
    };
    for ([_][]const u8{
        "MATCH(a:D)-[r:R]->(b:D),(a)-[s:S]->(b) RETURN s,sum(r.rank)",
        "MATCH(a:D)-[r:R]->(b:D),(a)-[s:Alias]->(b) RETURN s,sum(r.rank)",
        "MATCH p=(a:D)-[:R]->(b:D) RETURN p",
    }, 0..) |query, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
        const r = demand.relationship("docs", "links", "r");
        const s = demand.relationship("docs", "links", "s");
        if (i == 0) {
            try expectNames(&.{"rank"}, r.fields.?);
            try std.testing.expect(s.fields == null);
        } else {
            try std.testing.expect(r.fields == null);
            try expectNames(&.{}, s.fields.?);
        }
    }
}

test "read demand separates predicate representatives from observed property values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(d:D) RETURN labels(d)",
        "MATCH(d:D) RETURN labels(d),d.body IS NULL",
        "MATCH(d:D) RETURN labels(d),d.body",
    }, 0..) |query, i| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"body"}, demand.fields.?);
        if (i != 2) try std.testing.expect(demand.projection[0].mode == .predicate) else try std.testing.expect(demand.projection.len == 0);
    }
}

test "owner count derives its own borrowed membership demand" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input: root.backend.CountInput = .{ .node = .{ .span = .{}, .alias = "d", .labels = &.{"Wide"}, .properties = &.{} } };
    var membership = graph;
    membership.labels = graph.labels[1..];
    // Count membership deliberately excludes D although the retained catalog
    // relationships still name it. Count demand must not resolve those edges.
    const projection = (try @import("owner_count.zig").Projection.init(a, membership, input)).?;
    const demand = projection.demand.table("docs").?.nodes;
    try expectNames(&.{"body"}, demand.fields.?);
    try std.testing.expect(demand.projection[0].mode == .predicate);
}

test "read demand propagates observations through computed selectors containers and scopes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH(d:D) RETURN d=d,d IS NOT NULL,[d]=[d]",
        "MATCH(d:D) WHERE d=d RETURN count(*)",
        "MATCH(d:D) WITH coalesce(d,null) AS x RETURN antfly.documentKey(x)",
        "MATCH(d:D) WITH coalesce(d,null) AS d WITH coalesce(d,d) AS d RETURN antfly.documentKey(d)",
        "MATCH(d:D) WITH DISTINCT [d] AS xs RETURN count(*)",
        "MATCH(d:D) WITH DISTINCT {node:d} AS x RETURN count(*)",
        "MATCH(d:D) WITH [d] AS xs UNWIND xs AS x RETURN antfly.documentKey(x)",
        "MATCH(d:D) WITH CASE WHEN true THEN d ELSE null END AS x RETURN antfly.documentKey(x)",
        "MATCH(d:D) CALL { WITH d RETURN coalesce(d,null) AS x } RETURN antfly.documentKey(x)",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand;
        try expectNames(&.{}, demand.table("docs").?.nodes.fields.?);
    }
    for ([_][]const u8{
        "MATCH(d:D) WITH coalesce(d,null) AS x RETURN x.payload.rank",
        "MATCH(d:D) WITH coalesce(d.payload,null) AS x RETURN x.rank",
        "MATCH(d:D) WITH [d] AS xs RETURN head(xs).payload.rank",
        "MATCH(d:D) WITH {node:d} AS x RETURN x.node.payload.rank",
        "MATCH(d:D) WITH [d] AS xs UNWIND xs AS x RETURN x.payload.rank",
        "MATCH(d:D) WITH [d] AS xs RETURN xs[0].payload.rank",
        "MATCH(d:D) WITH CASE WHEN true THEN d ELSE null END AS x RETURN x.payload.rank",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
        try std.testing.expectEqualStrings("rank", demand.projection[0].children[0].name);
    }
    for ([_][]const u8{
        "MATCH(d:D) WITH coalesce(d,null) AS x RETURN x",
        "MATCH(d:D) WITH [d] AS xs RETURN xs",
        "MATCH(d:D) WITH {node:d} AS x RETURN x",
        "MATCH(d:D) WITH [d] AS xs UNWIND xs AS x RETURN x",
    }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expect((try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes.fields == null);
    }
}

test "read demand key shapes retain independent scalar and nested value overrides" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "MATCH(d:D) RETURN keys(d)", "MATCH(d:D) RETURN size(keys(coalesce(d,null)))", "MATCH(d:D) RETURN keys(properties(d))" }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{}, demand.fields.?);
        try std.testing.expect(root.read_fields.rootKeys(demand.projection));
    }
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(d:D) RETURN keys(d),d.rank,keys(d.payload),d.payload.rank,labels(d)", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
    try std.testing.expect(root.read_fields.rootKeys(demand.projection));
    const nested = for (demand.projection) |item| {
        if (std.mem.eql(u8, item.name, "payload")) break item;
    } else unreachable;
    try std.testing.expect(nested.keys);
    try std.testing.expectEqualStrings("rank", nested.children[0].name);
}

test "read demand retains computed binding domains through optional label constraints" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var mapped = graph;
    mapped.labels = &.{ .{ .name = "D", .table = "docs" }, .{ .name = "C", .table = "categories" } };
    mapped.relationships = &.{};
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(a, "MATCH(d:D),(c:C) WITH coalesce(c,d) AS x OPTIONAL MATCH(x:D) WITH x.payload AS p RETURN p.rank", .{}, &diagnostic);
    defer compiled.deinit();
    const demand = (try root.read_fields.analyze(a, compiled.query, mapped)).demand;
    for ([_][]const u8{ "docs", "categories" }) |table| {
        const entity = demand.table(table).?.nodes;
        try expectNames(&.{"payload"}, entity.fields.?);
        try std.testing.expectEqualStrings("rank", entity.projection[0].children[0].name);
    }
}

test "read demand opaque JSON selectors retain only their owning property" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{ "MATCH(d:D) RETURN d.payload[0]", "MATCH(d:D) RETURN d.payload[$key]", "MATCH(d:D) WITH coalesce(d,null) AS x RETURN x.payload[0].rank" }) |query| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, query, .{}, &diagnostic);
        defer compiled.deinit();
        const demand = (try root.read_fields.analyze(a, compiled.query, graph)).demand.table("docs").?.nodes;
        try expectNames(&.{"payload"}, demand.fields.?);
    }
}

test "read demand native authorized projection retains key shapes and value overrides" {
    const Hooks = struct {
        allow: bool = true,
        fn bind(_: *anyopaque, _: root.catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn project(ptr: *anyopaque, _: std.mem.Allocator, node: *const root.value.Node) !?*const root.value.Node {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return if (self.allow) node else null;
        }
        fn relationship(_: *anyopaque, _: std.mem.Allocator, item: *const root.value.Relationship) !?*const root.value.Relationship {
            return item;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var hooks: Hooks = .{};
    const Adapter = root.native.Adapter(struct {}, void, void);
    var adapter: Adapter = .{
        .tables = &.{},
        .ttl_now_ns = 1,
        .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} },
        .authority = .{ .ptr = &hooks, .bind = Hooks.bind, .checkpoint = Hooks.checkpoint, .project = Hooks.project, .project_stored = Hooks.project, .project_relationship = Hooks.relationship, .storage_projection = true },
        .read_demand = .{ .tables = &.{.{ .name = "docs", .labels = &.{"D"}, .nodes = .{ .fields = &.{"score"}, .projection = &.{.{ .name = "", .mode = .children, .keys = true, .root = true }} } }} },
    };
    const node = (try adapter.node(a, "docs", "d1", "{\"body\":true,\"score\":7,\"nil\":null}")).?;
    try std.testing.expectEqual(@as(usize, 2), node.properties.len);
    try std.testing.expectEqual(@as(i64, 7), (root.value.Value{ .node = node }).property("score").integer);
    try std.testing.expect((root.value.Value{ .node = node }).property("body").boolean);
    hooks.allow = false;
    try std.testing.expect(try adapter.node(a, "docs", "d1", "{\"body\":true}") == null);
}
