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
