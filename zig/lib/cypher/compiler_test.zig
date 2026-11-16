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
const lexer = @import("lexer.zig");

test "graph lexer handles Unicode scalars, paired surrogates and SQL positional parameters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var diagnostic: lexer.Diagnostic = .{};
    const tokens = try lexer.scan(arena.allocator(), "RETURN '\\u0041\\uD83D\\uDE00', $1, $é, $`odd name`", 100, &diagnostic);
    try std.testing.expectEqualStrings("A😀", tokens[1].text);
    try std.testing.expectEqualStrings("1", tokens[3].text);
    try std.testing.expectEqualStrings("é", tokens[5].text);
    try std.testing.expectEqualStrings("odd name", tokens[7].text);
    try std.testing.expectError(error.InvalidCypherSyntax, lexer.scan(arena.allocator(), "RETURN '\\uD800'", 100, &diagnostic));
    try std.testing.expectError(error.InvalidCypherSyntax, lexer.scan(arena.allocator(), "RETURN '\\U00110000'", 100, &diagnostic));
}
const compiler = @import("compiler.zig");
fn prepare(text: []const u8) !compiler.Compiled {
    var diagnostic: compiler.Diagnostic = .{};
    return compiler.compile(std.testing.allocator, text, .{}, &diagnostic) catch |err| {
        std.debug.print("{s} at {}..{}: {s}\n", .{ text, diagnostic.span.start, diagnostic.span.end, diagnostic.message });
        return err;
    };
}
test "graph grammar constructs scoped pipelines, directions, parameters, and optional predicates" {
    var query = try prepare("MATCH p=(a:Document {id:$id})-[r:CITES|MENTIONS*0..3]->(b:Person) WHERE b.age >= $age OPTIONAL MATCH (b)<-[:KNOWS]-(c) WHERE c.active = true WITH b, count(c) AS n ORDER BY n DESC LIMIT 20 RETURN b.name AS name, n");
    defer query.deinit();
    try std.testing.expectEqual(@as(usize, 4), query.query.clauses.len);
    const match = query.query.clauses[0].operation.match;
    try std.testing.expectEqualStrings("p", match.paths[0].alias.?);
    try std.testing.expectEqual(@as(u32, 0), match.paths[0].edges[0].min_hops);
    try std.testing.expectEqual(@as(?u32, 3), match.paths[0].edges[0].max_hops);
    try std.testing.expectEqual(compiler.ast.PathMode.relationship_simple, match.mode);
    try std.testing.expectEqual(compiler.ast.Direction.in, query.query.clauses[1].operation.match.paths[0].edges[0].direction);
    try std.testing.expect(query.query.clauses[1].operation.match.optional);
    try std.testing.expect(query.query.clauses[1].operation.match.predicate != null);
    try std.testing.expectEqual(@as(usize, 2), query.parameters.len);
}
test "graph grammar supports writes, subqueries, search calls, and unions" {
    for ([_][]const u8{
        "CREATE (a:Person {name:'Ada'})-[r:KNOWS {weight:1.5}]->(b:Person {name:'Bob'}) RETURN a, r, b",
        "MATCH (a:Person) MERGE (a)-[r:KNOWS]->(b:Person {id:$id}) ON CREATE SET r.weight = 1 ON MATCH SET r.weight = 2 RETURN b",
        "MATCH (a) SET a.name = $name REMOVE a.old DETACH DELETE a",
        "UNWIND [1,2,3] AS x RETURN x UNION ALL RETURN 4 AS x",
        "CALL antfly.search('documents',{query:$question,mode:'hybrid',limit:20}) YIELD node AS seed, score MATCH (seed)-[:MENTIONS]->(p:Person) RETURN p.name, max(score) AS relevance ORDER BY relevance DESC",
        "MATCH (a) CALL { WITH a MATCH (a)-[:R]->(b) RETURN b } RETURN a,b",
        "EXPLAIN MATCH p=shortestPath((a)-[:R*1..5]->(b)) RETURN p",
        "MATCH (a) WHERE EXISTS { MATCH (a)-[:R]->(b) } RETURN a",
        "RETURN CASE WHEN $x IS NULL THEN 'missing' ELSE $x END AS value",
        "PROFILE MATCH REPEATABLE ELEMENTS (a)-[:R*1..3]->(b) RETURN b",
    }) |text| {
        var query = try prepare(text);
        defer query.deinit();
    }
}
test "graph diagnostics reject malformed queries and preserve source spans" {
    var diagnostic: compiler.Diagnostic = .{};
    try std.testing.expectError(error.InvalidCypherSyntax, compiler.compile(std.testing.allocator, "MATCH (a)-[:R]-> RETURN a", .{}, &diagnostic));
    try std.testing.expect(diagnostic.span.start >= 16);
    try std.testing.expectError(error.InvalidCypherSyntax, compiler.compile(std.testing.allocator, "RETURN 'unterminated", .{}, &diagnostic));
    try std.testing.expectEqual(@as(usize, 7), diagnostic.span.start);
    try std.testing.expectError(error.CypherLimitExceeded, compiler.compile(std.testing.allocator, "RETURN 1", .{ .max_bytes = 2 }, &diagnostic));
}

test "compiler bounds expression trees built without recursive parsing" {
    const a = std.testing.allocator;
    for ([_][]const u8{ ".field", "+1" }) |suffix| {
        var source: std.ArrayList(u8) = .empty;
        defer source.deinit(a);
        try source.appendSlice(a, if (suffix[0] == '.') "RETURN $x" else "RETURN 1");
        for (0..128) |_| try source.appendSlice(a, suffix);
        var diagnostic: compiler.Diagnostic = .{};
        try std.testing.expectError(error.CypherLimitExceeded, compiler.compile(a, source.items, .{}, &diagnostic));
        try std.testing.expectEqualStrings("Cypher expression tree depth budget exceeded", diagnostic.message);
    }
    var query = try prepare("RETURN $x.field.field");
    defer query.deinit();
    try std.testing.expectEqual(@as(usize, 3), query.query.clauses[0].operation.project.value.items[0].expression.depth);
}
test "compiler owns values after input buffers are released" {
    const a = std.testing.allocator;
    const source = try a.dupe(u8, "MATCH (`some node` {title:'a\\nb'}) RETURN `some node`.title, $q");
    var query = try prepare(source);
    a.free(source);
    defer query.deinit();
    try std.testing.expectEqualStrings("some node", query.query.clauses[0].operation.match.paths[0].nodes[0].alias.?);
    try std.testing.expectEqualStrings("a\nb", query.query.clauses[0].operation.match.paths[0].nodes[0].properties[0].value.value.literal.string);
}

test "comparison chains preserve operands and list slices require a bound" {
    var query = try prepare("RETURN 1<2<3<4, (1<2)<3, [1,2,3][-2..]");
    defer query.deinit();
    const items = query.query.clauses[0].operation.project.value.items;
    try std.testing.expectEqual(@as(usize, 4), items[0].expression.value.comparison.operands.len);
    try std.testing.expectEqual(@as(usize, 3), items[0].expression.value.comparison.operators.len);
    try std.testing.expect(items[1].expression.value == .binary);
    try std.testing.expect(items[1].expression.value.binary.left.value == .binary);
    try std.testing.expect(items[2].expression.value.slice.end == null);
    var diagnostic: compiler.Diagnostic = .{};
    try std.testing.expectError(error.InvalidCypherSyntax, compiler.compile(std.testing.allocator, "RETURN [1,2][..]", .{}, &diagnostic));
}

test "integer literals preserve both signed boundaries" {
    var query = try prepare("RETURN -9223372036854775808, 9223372036854775807");
    defer query.deinit();
    const items = query.query.clauses[0].operation.project.value.items;
    try std.testing.expectEqual(std.math.minInt(i64), items[0].expression.value.literal.integer);
    try std.testing.expectEqual(std.math.maxInt(i64), items[1].expression.value.literal.integer);
    var diagnostic: compiler.Diagnostic = .{};
    try std.testing.expectError(error.InvalidCypherNumber, compiler.compile(std.testing.allocator, "RETURN 9223372036854775808", .{}, &diagnostic));
    try std.testing.expectError(error.InvalidCypherNumber, compiler.compile(std.testing.allocator, "RETURN -9223372036854775809", .{}, &diagnostic));
}
