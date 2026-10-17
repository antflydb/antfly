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
const c = @import("compiler.zig");
const v = @import("value.zig");
const catalog = @import("catalog.zig");
const b = @import("backend.zig");
const runtime = @import("runtime.zig");
const graph: catalog.Graph = .{ .name = "knowledge", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "Document", .table = "docs" }, .{ .name = "Person", .table = "people" } }, .relationships = &.{ .{ .name = "CITES", .index = "links", .source_label = "Document", .target_label = "Document", .stored_type = "cites" }, .{ .name = "MENTIONS", .index = "links", .source_label = "Document", .target_label = "Person", .stored_type = "mentions" } } };
const Fixture = struct {
    cancel_after: usize = std.math.maxInt(usize),
    checkpoints: usize = 0,
    deny_b: bool = false,
    const nodes = [_]v.Node{
        .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "title", .value = .{ .string = "Alpha" } }, .{ .name = "rank", .value = .{ .integer = 1 } } } },
        .{ .identity = .{ .table = "docs", .key = "b" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "b" } }, .{ .name = "title", .value = .{ .string = "Beta" } }, .{ .name = "rank", .value = .{ .integer = 2 } } } },
        .{ .identity = .{ .table = "docs", .key = "c" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "c" } }, .{ .name = "title", .value = .{ .string = "Gamma" } }, .{ .name = "rank", .value = .{ .integer = 3 } } } },
        .{ .identity = .{ .table = "people", .key = "a" }, .labels = &.{"Person"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "name", .value = .{ .string = "Ada" } } } },
    };
    const edges = [_]v.Relationship{
        .{ .identity = .{ .table = "docs", .key = "ab1" }, .index = "links", .type = "CITES", .source = nodes[0].identity, .target = nodes[1].identity, .properties = &.{.{ .name = "weight", .value = .{ .integer = 1 } }} },
        .{ .identity = .{ .table = "docs", .key = "ab2" }, .index = "links", .type = "CITES", .source = nodes[0].identity, .target = nodes[1].identity, .properties = &.{.{ .name = "weight", .value = .{ .integer = 2 } }} },
        .{ .identity = .{ .table = "docs", .key = "ba" }, .index = "links", .type = "CITES", .source = nodes[1].identity, .target = nodes[0].identity },
        .{ .identity = .{ .table = "docs", .key = "bc" }, .index = "links", .type = "CITES", .source = nodes[1].identity, .target = nodes[2].identity },
        .{ .identity = .{ .table = "docs", .key = "ap" }, .index = "links", .type = "MENTIONS", .source = nodes[0].identity, .target = nodes[3].identity },
    };
    fn bind(_: *anyopaque, _: std.mem.Allocator, _: catalog.Graph) !void {}
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.checkpoints += 1;
        if (self.checkpoints > self.cancel_after) return error.Canceled;
    }
    fn scan(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        var output: std.ArrayList(*const v.Node) = .empty;
        for (&nodes) |*node| {
            if (!std.mem.eql(u8, node.identity.table, table) or (self.deny_b and node.identity.eql(nodes[1].identity))) continue;
            if (cursor) |after| if (std.mem.order(u8, node.identity.key, after) != .gt) continue;
            if (output.items.len == limit) break;
            try output.append(a, node);
        }
        const next = if (output.items.len == limit) try a.dupe(u8, output.items[output.items.len - 1].identity.key) else null;
        return .{ .items = try output.toOwnedSlice(a), .next = next, .unfiltered = true };
    }
    fn get(ptr: *anyopaque, _: std.mem.Allocator, identity: v.Identity) !?*const v.Node {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        for (&nodes) |*node| if (node.identity.eql(identity)) return if (self.deny_b and node.identity.eql(nodes[1].identity)) null else node;
        return null;
    }
    fn expand(_: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
        var output: std.ArrayList(*const v.Relationship) = .empty;
        for (&edges) |*edge| if (std.mem.eql(u8, mapping.name, edge.type) and ((direction != .in and edge.source.eql(identity)) or (direction != .out and edge.target.eql(identity)))) try output.append(a, edge);
        return .{ .items = try output.toOwnedSlice(a) };
    }
    fn search(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: b.SearchInput) !b.SearchPage {
        return .{ .seeds = &.{.{ .node = &nodes[0], .score = 0.9 }}, .examined = 1 };
    }
    fn backend(self: *Fixture) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .search = search } };
    }
};
fn check(text: []const u8, expected: []const u8) !void {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.rows, .{});
    defer std.testing.allocator.free(json);
    try std.testing.expectEqualStrings(expected, json);
}
test "Cypher regex predicates use whole strings and preserve dynamic NULL arguments" {
    try check("MATCH (n:Document) WHERE n.title =~ 'A.*' RETURN n.id", "[[\"a\"]]");
    try check("RETURN 'abc' =~ 'b', 'AbC' =~ '(?i)abc', null =~ '.*'", "[[false,true,null]]");
    try check("UNWIND [1,'abc',null] AS x RETURN x =~ 'abc'", "[[null],[true],[null]]");
    try check("RETURN 1 CONTAINS '1', 'abc' STARTS WITH 1, null ENDS WITH 'a'", "[[null,null,null]]");
    var diagnostic: c.Diagnostic = .{};
    var invalid = try c.compile(std.testing.allocator, "MATCH (n:Person) WHERE n.name =~ '(' RETURN n", .{}, &diagnostic);
    defer invalid.deinit();
    try std.testing.expectError(error.CypherInvalidRegex, @import("binder.zig").validate(std.testing.allocator, graph, invalid.query));
}

test "WITH filters retain input scope for filtering and expose only projected variables afterward" {
    try check("MATCH (n:Document) WITH n.title AS title WHERE n.rank>1 RETURN title ORDER BY title", "[[\"Beta\"],[\"Gamma\"]]");
    try check("WITH 1 AS x WITH 2 AS x WHERE x=2 RETURN x", "[[2]]");
    try check("WITH 1 AS x WITH x+1 AS y WHERE x=1 AND y=2 RETURN y", "[[2]]");
    try check("MATCH (n:Document) WITH n.title AS title WHERE n.rank IS NULL RETURN title", "[]");
    for ([_][]const u8{
        "MATCH (n:Document) WITH n.title AS title WHERE n.rank>1 RETURN n",
        "MATCH (n:Document) WITH DISTINCT n.title AS title WHERE n.rank>1 RETURN title",
        "MATCH (n:Document) WITH n.title AS title,count(*) AS count WHERE n.rank>1 RETURN title",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(error.UnboundCypherVariable, @import("binder.zig").validate(std.testing.allocator, graph, compiled.query));
    }
}

test "graph runtime matches parallel relationships, optional nulls, and cross-table identities" {
    try check("MATCH (a:Document {id:'a'})-[r:CITES]->(b:Document) RETURN b.title, r.weight ORDER BY r.weight", "[[\"Beta\",1],[\"Beta\",2]]");
    try check("MATCH (a:Document {id:'c'}) OPTIONAL MATCH (a)-[:MENTIONS]->(p:Person) RETURN a.title, p.name", "[[\"Gamma\",null]]");
    try check("MATCH (a:Document)-[:MENTIONS]->(p:Person) RETURN a.id=p.id AS same_key, a=p AS same_node", "[[true,false]]");
}
test "pattern comprehensions retain local scope and authorized traversal budgets" {
    try check("WITH 1 AS p MATCH (a:Document {id:'a'}) RETURN [p=(a)-[:CITES]->(b) | length(p)],p", "[[[1,1],1]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN [p=(a)-[:CITES]->(c) | length(p)],length(p)", "[[[1,1],1],[[1,1],1]]");
    for ([_][]const u8{
        "MATCH (a:Document) RETURN [(a)-[:CITES]->(b) | b.id], b",
        "MATCH (a:Document) RETURN [(a)-[:CITES]->(b) | count(b)]",
    }, [_]anyerror{ error.UnboundCypherVariable, error.CypherAggregateScope }) |text, expected| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(expected, @import("binder.zig").validate(std.testing.allocator, graph, compiled.query));
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document {id:'a'}) RETURN [(a)-[:CITES*1..2]->(b) | b.id]", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{ .deny_b = true };
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.rows[0][0].list.len);
    fixture.deny_b = false;
    try std.testing.expectError(error.CypherEdgeBudgetExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .explored_edges = 1 }));
    fixture.cancel_after = 0;
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}
test "Cypher paths revisit nodes but never repeat a relationship in one MATCH" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*2..2]->(b:Document {id:'a'}) RETURN length(p)", "[[2],[2]]");
    try check("MATCH (a:Document {id:'a'})-[r:CITES]->(b)-[r]->(a) RETURN a", "[]");
    try check("MATCH (a:Document {id:'a'})-[r:CITES]->(b), (a)-[r]->(b) RETURN a", "[]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..0]->(b:Document) RETURN length(p), a=b", "[[0,true]]");
}
test "graph aggregation pipelines, null truth, and unions execute" {
    try check("MATCH (a:Document)-[:CITES]->(b) WITH b.id AS id, count(*) AS n RETURN id,n ORDER BY id", "[[\"a\",1],[\"b\",2],[\"c\",1]]");
    try check("MATCH (a:Document {id:'missing'}) RETURN count(*) AS n, collect(a) AS nodes", "[[0,[]]]");
    try check("UNWIND [42, 'hello', null, true, {name:'Alice'}, [1,2,3]] AS x RETURN x ORDER BY x", "[[{\"name\":\"Alice\"}],[[1,2,3]],[\"hello\"],[true],[42],[null]]");
    try check("UNWIND [{b:0,a:1},{a:0,b:9},{c:0,a:0},{a:1}] AS x RETURN x ORDER BY x", "[[{\"a\":1}],[{\"a\":0,\"b\":9}],[{\"b\":0,\"a\":1}],[{\"c\":0,\"a\":0}]]");
    try check("UNWIND [3,1,2,2] AS x RETURN DISTINCT x ORDER BY x SKIP 1 LIMIT 2", "[[2],[3]]");
    try check("RETURN NULL AND false, NULL OR true, NULL IN [], 1 IN [null,1], 2 IN [null,1]", "[[false,true,false,true,null]]");
    try check("RETURN [null]=[null], [null,1]=[null,2], {x:null}={x:null}, [null] IN [[null]], {x:null} IN [{y:null}]", "[[null,false,null,null,false]]");
    try check("UNWIND [1,2,3] AS x RETURN {total:sum(x), count:count(*)}", "[[{\"total\":6,\"count\":3}]]");
    try check("RETURN 1 AS x UNION RETURN 1 AS x UNION ALL RETURN 2 AS x", "[[1],[2]]");
}
test "search seeds scope graph expansion and preserve scores" {
    try check("CALL antfly.search('docs',{query:'question',limit:1}) YIELD node AS seed, score MATCH (seed)-[:MENTIONS]->(p:Person) RETURN p.name, max(score) AS relevance", "[[\"Ada\",0.9]]");
}
test "hidden intermediate nodes and execution budgets fail closed" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document {id:'a'})-[:CITES*2..2]->(c:Document {id:'c'}) RETURN c", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{ .deny_b = true };
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    fixture.deny_b = false;
    try std.testing.expectError(error.CypherEdgeBudgetExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .explored_edges = 1 }));
    fixture.cancel_after = 0;
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

const FixedAllocation = struct {
    backing: std.mem.Allocator,
    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn resize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn remap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(ptr: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.backing.rawFree(bytes, alignment, ra);
    }
};
fn allocationCase(backing: std.mem.Allocator) !void {
    // Arena growth must make deterministic allocations for fail-index coverage.
    var fixed: FixedAllocation = .{ .backing = backing };
    const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "MATCH p=(a:Document {id:'a'})-[:CITES*0..2]->(b) WITH b, collect(p) AS paths RETURN b{.title}, paths, [(b)-[:CITES]->(next) | next.id] AS neighbors, reduce(s=0,x IN [1,2] | s+x) AS total, all(x IN [true,null] WHERE x) AS predicate ORDER BY b.title", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
    defer result.deinit();
    const json = try std.json.Stringify.valueAlloc(a, result, .{});
    defer a.free(json);
}
test "Cypher compiler, traversal, aggregation and serialization release every failed allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}

test "EXPLAIN exposes bound columns without executing scans or mutations" {
    const a = std.testing.allocator;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "EXPLAIN CREATE (n:Document {id:'new'}) RETURN n.id AS id", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    try std.testing.expectEqualStrings("id", result.columns[0]);
    try std.testing.expectEqual(@as(usize, 0), result.stats.scanned_anchors);
    try std.testing.expectEqual(@as(usize, 0), result.stats.mutations);
    try std.testing.expectEqual(@as(usize, 2), result.stages.len);
}

test "Cypher scalar signatures preserve Unicode positions, conversions and range bounds" {
    try check("RETURN range(3,1,-1), range(1,3,-1), reverse('aé🌱'), substring('aé🌱z',1,2)", "[[[3,2,1],[],\"🌱éa\",\"é🌱\"]]");
    try check("RETURN keys({a:1,b:2}), reverse([1,2]), trim(' a '), toString(true), toInteger('3.5'), toFloat('2.5'), toBoolean('FALSE'), toInteger('no')", "[[[\"a\",\"b\"],[2,1],\"a\",\"true\",3,2.5,false,null]]");
    try check("MATCH (n:Document {id:'a'}) RETURN antfly.documentKey(n), size('aé🌱'), size([1,2])", "[[\"a\",3,2]]");
    for ([_][]const u8{ "RETURN length([1,2])", "MATCH p=(n:Document) RETURN size(p)" }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN id(n)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherLegacyIdUnavailable, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.checkpoints);
}

test "Cypher grouping never reads an arbitrary ungrouped source row" {
    try check("MATCH (n:Document) RETURN n.rank AS rank, count(*) + n.rank AS total ORDER BY rank", "[[1,2],[2,3],[3,4]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN count(*) + n.rank AS total", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherInvalidGrouping, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.checkpoints);
}

test "shortest selectors choose a path independently for each endpoint pair" {
    try check("MATCH p=shortestPath((a:Document {id:'a'})-[:CITES*1..3]->(b)) RETURN b.id, length(p) ORDER BY b.id", "[[\"a\",2],[\"b\",1],[\"c\",2]]");
    try check("MATCH p=allShortestPaths((a:Document {id:'a'})-[:CITES*1..3]->(b)) RETURN b.id, length(p) ORDER BY b.id", "[[\"a\",2],[\"a\",2],[\"b\",1],[\"b\",1],[\"c\",2],[\"c\",2]]");
}

test "list comprehensions own lexical scopes and follow three-valued predicates" {
    try check("WITH 99 AS x RETURN [x IN [1,null,2] WHERE x > 1 | x*2], x", "[[[4],99]]");
    try check("RETURN [x IN [1,2] | x+1], [x IN [1,2]], [x IN null | x]", "[[[2,3],[1,2],null]]");
    try check("UNWIND [1,2] AS x RETURN [y IN collect(x) | y*2]", "[[[2,4]]]");
}

test "list predicates and reductions preserve nulls and lexical bindings" {
    try check("RETURN all(x IN [] WHERE x), any(x IN [] WHERE x), none(x IN [] WHERE x), single(x IN [] WHERE x)", "[[true,false,true,false]]");
    try check("RETURN all(x IN [null,false] WHERE x), any(x IN [null,true] WHERE x), none(x IN [null,true] WHERE x), single(x IN [null,true,true] WHERE x)", "[[false,true,false,false]]");
    try check("RETURN all(x IN [null,true] WHERE x), any(x IN [null,false] WHERE x), none(x IN [null,false] WHERE x), single(x IN [null,true] WHERE x)", "[[null,null,null,null]]");
    try check("RETURN all(x IN null WHERE x), reduce(s=9,x IN null | s+x), reduce(s=9,x IN [] | s+x)", "[[null,null,9]]");
    try check("WITH 99 AS x, 7 AS s RETURN reduce(s=0,x IN [1,2,3] | s+x), x, s, any(x IN [1,2] WHERE x=2)", "[[6,99,7,true]]");
    try check("UNWIND [1,2,3] AS x RETURN reduce(s=0,y IN collect(x) | s+y), all(y IN collect(x) WHERE y>0)", "[[6,true]]");
    try check("RETURN reduce(s=[],x IN [1,2] | s+[x*2]), all(x IN [1,2] WHERE any(y IN [x] WHERE y=x))", "[[[2,4],true]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*1..2]->(b) WHERE all(r IN relationships(p) WHERE coalesce(r.weight,1)>0) RETURN count(*)", "[[6]]");
}

test "correlated EXISTS executes read pipelines and rejects every write branch" {
    try check("MATCH (n:Document) WHERE EXISTS { MATCH (n)-[:CITES]->(m) WITH m WHERE m.rank>2 RETURN m } RETURN n.id", "[[\"b\"]]");
    try check("RETURN EXISTS { UNWIND [] AS x RETURN x }, EXISTS { UNWIND [] AS x RETURN count(*) AS total }", "[[false,true]]");
    try check("RETURN EXISTS { UNWIND [1,2] AS x WITH x WHERE x>1 }", "[[true]]");
    for ([_][]const u8{
        "RETURN EXISTS { CREATE (n:Document) RETURN n }",
        "RETURN EXISTS { CALL { CREATE (n:Document) } RETURN 1 }",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherExistsMutation, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.checkpoints);
    }
}

test "map projections shape nodes relationships and maps with scoped expressions" {
    try check("WITH {a:1,b:2} AS m, 4 AS extra RETURN m{.a,.missing,extra,c:extra+1} AS selected, m{a:3, .*} AS first, m{.*,a:3} AS second", "[[{\"a\":1,\"missing\":null,\"extra\":4,\"c\":5},{\"a\":3,\"b\":2},{\"a\":3,\"b\":2}]]");
    try check("MATCH (n:Document {id:'a'}) RETURN n{.title,.missing}, n['rank']", "[[{\"title\":\"Alpha\",\"missing\":null},1]]");
    try check("MATCH (n:Document {id:'a'})-[r:CITES]->(m) RETURN r{.*}, r['weight'] ORDER BY r['weight']", "[[{\"weight\":1},1],[{\"weight\":2},2]]");
    try check("WITH null AS n RETURN n{.*,value:1} AS filled, n{} AS empty", "[[null,null]]");
    try check("MATCH (n:Document) RETURN collect(n{.title}) AS documents", "[[[{\"title\":\"Alpha\"},{\"title\":\"Beta\"},{\"title\":\"Gamma\"}]]]");
}

test "pinned Neo4j reference fixtures replay through the Antfly graph runtime" {
    const a = std.testing.allocator;
    const Case = struct { query: []const u8, rows: std.json.Value, reference_error: ?[]const u8 = null };
    const Campaign = struct { version: u32, reference: []const u8, image: []const u8, setup: []const u8, cases: []const Case };
    var campaign = try std.json.parseFromSlice(Campaign, a, @embedFile("fixtures/neo4j_read.json"), .{});
    defer campaign.deinit();
    for (campaign.value.cases) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = c.compile(a, case.query, .{}, &diagnostic) catch |err| {
            std.debug.print("reference query {s}: {s} at {}..{}\n", .{ case.query, diagnostic.message, diagnostic.span.start, diagnostic.span.end });
            return err;
        };
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        const expected = try v.Value.fromJson(region.allocator(), case.rows);
        try std.testing.expectEqual(expected.list.len, result.rows.len);
        for (expected.list, result.rows) |row, actual| try std.testing.expect(v.Value.eql(row, .{ .list = actual }));
    }
}

test "owner search RPC retains typed node identity and score through traversal" {
    const rpc = @import("rpc.zig");
    const Channel = struct {
        fixture: Fixture = .{},
        fn call(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return (rpc.Owner{ .backend = self.fixture.backend(), .graph = graph, .table = "docs" }).execute(a, body);
        }
    };
    var channel: Channel = .{};
    var client: rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Channel.call }, .incarnation = graph.incarnation, .revision = graph.revision };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "CALL antfly.search('docs', {limit:1}) YIELD node AS a, score MATCH (a)-[:CITES]->(b) RETURN b.title,score ORDER BY b.title", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, client.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    for (result.rows) |row| {
        try std.testing.expectEqualStrings("Beta", row[0].string);
        try std.testing.expectEqual(@as(f64, 0.9), row[1].number);
    }
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try std.testing.expectError(error.Forbidden, client.backend().vtable.search.?(client.backend().ptr, arena.allocator(), "people", .{ .parameters = .{ .map = &.{} } }));
}

test "terminal node LIMIT bounds physical scans but ordering and aggregates remain exhaustive" {
    var diagnostic: c.Diagnostic = .{};
    var fixture: Fixture = .{};
    var limited = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN n.title LIMIT 1", .{}, &diagnostic);
    defer limited.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &limited, &.{}, .{ .scanned_anchors = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.stats.scanned_anchors);
    try std.testing.expectEqualStrings("Alpha", result.rows[0][0].string);
    try check("MATCH (n:Document) WHERE n.id <> 'a' RETURN n.title SKIP 1 LIMIT 1", "[[\"Gamma\"]]");
    try check("MATCH (n:Document) RETURN n.title ORDER BY n.title DESC LIMIT 1", "[[\"Gamma\"]]");
    try check("MATCH (n:Document) RETURN count(*) LIMIT 1", "[[3]]");
    try check("MATCH (n:Document) RETURN n.title LIMIT 0", "[]");
}

test "RPC runtime checkpoints cancel pure computation without a data page" {
    const rpc = @import("rpc.zig");
    const Channel = struct {
        fn call(_: *anyopaque, _: std.mem.Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedDataRead;
        }
    };
    var fixture: Fixture = .{ .cancel_after = 5 };
    var client: rpc.Client = .{ .channel = .{ .ptr = &fixture, .call = Channel.call, .checkpoint = Fixture.checkpoint }, .incarnation = graph.incarnation, .revision = graph.revision };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,1000) AS x RETURN x", .{}, &diagnostic);
    defer compiled.deinit();
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, client.backend(), graph, &compiled, &.{}, .{}));
}

test "document-key anchors use authorized point reads and preserve qualified identity" {
    var diagnostic: c.Diagnostic = .{};
    var fixture: Fixture = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) WHERE antfly.documentKey(n)='b' RETURN n.title LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .scanned_anchors = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.stats.scanned_anchors);
    try std.testing.expectEqualStrings("Beta", result.rows[0][0].string);
    fixture.deny_b = true;
    var hidden = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .scanned_anchors = 1 });
    defer hidden.deinit();
    try std.testing.expectEqual(@as(usize, 0), hidden.rows.len);
    try check("MATCH (n) WHERE antfly.documentKey(n)='a' RETURN count(n)", "[[2]]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n)=null RETURN n", "[]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n)=42 RETURN n", "[]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n)='missing' OR n.id='a' RETURN n.id", "[[\"a\"]]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n)='b' AND n.title='Beta' RETURN n.id", "[[\"b\"]]");
    try check("MATCH (n:Document)-[:CITES]->(m) WHERE antfly.documentKey(n)='a' RETURN m.id", "[[\"b\"],[\"b\"]]");
}

test "search resolves logical graph names and charges examined candidates" {
    var fixture: Fixture = .{};
    var mapped = graph;
    mapped.labels = &.{ .{ .name = "Document", .table = "docs", .logical_table = "renamed_docs" }, graph.labels[1] };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "CALL antfly.search('renamed_docs', {limit:1}) YIELD node,score RETURN node.id,score", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), mapped, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.stats.search_calls);
    try std.testing.expectEqual(@as(usize, 1), result.stats.search_candidates);
    try std.testing.expectEqual(@as(usize, 1), result.stats.scanned_anchors);
    try std.testing.expectEqualStrings("a", result.rows[0][0].string);
    try std.testing.expectError(error.CypherAnchorBudgetExceeded, runtime.execute(std.testing.allocator, fixture.backend(), mapped, &compiled, &.{}, .{ .scanned_anchors = 0 }));
    mapped.labels = &.{ .{ .name = "Document", .table = "docs", .logical_table = "renamed_docs", .namespace = "private" }, graph.labels[1] };
    try std.testing.expectError(error.CypherSearchTableOutsideGraph, runtime.execute(std.testing.allocator, fixture.backend(), mapped, &compiled, &.{}, .{}));
    var scoped = try c.compile(std.testing.allocator, "CALL antfly.search({namespace:'private',table:'renamed_docs'}, {limit:1}) YIELD node RETURN node.id", .{}, &diagnostic);
    defer scoped.deinit();
    var scoped_result = try runtime.execute(std.testing.allocator, fixture.backend(), mapped, &scoped, &.{}, .{});
    defer scoped_result.deinit();
    try std.testing.expectEqualStrings("a", scoped_result.rows[0][0].string);
}

test "transaction overlay reads staged entities and rolls back before durable commit" {
    const Txn = @import("transaction.zig");
    const Host = struct {
        committed: usize = 0,
        sequence: usize = 0,
        fail: bool = false,
        fn identity(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, _: bool) !v.Identity {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.sequence += 1;
            return .{ .table = table, .key = try std.fmt.allocPrint(a, "new-{d}", .{self.sequence}) };
        }
        fn commit(ptr: *anyopaque, _: std.mem.Allocator, mutations: []const b.Mutation) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail) return error.CypherTransactionConflict;
            self.committed += mutations.len;
        }
    };
    var fixture: Fixture = .{};
    var host: Host = .{};
    var overlay: Txn.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &host, .new_identity = Host.identity, .commit = Host.commit } };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "CREATE (n:Document {id:'new',title:'Created'}) WITH n MATCH (m:Document {id:'new'}) SET m.title='Updated' RETURN m.title", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqualStrings("Updated", result.rows[0][0].string);
    try std.testing.expectEqual(@as(usize, 2), host.committed);
    host.fail = true;
    overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &host, .new_identity = Host.identity, .commit = Host.commit } };
    try std.testing.expectError(error.CypherTransactionConflict, runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{}));
    try std.testing.expect(!overlay.active);
    try std.testing.expectEqual(@as(usize, 0), overlay.mutations.items.len);
    try std.testing.expectEqual(@as(usize, 2), host.committed);
}

test "Unicode casing and trimming preserve Cypher scalar semantics" {
    try check("RETURN toLower('ÉCOLE'),toUpper('Straße'),trim('　hi '),ltrim(' hi '),rtrim(' hi ')", "[[\"école\",\"STRASSE\",\"hi\",\"hi \",\" hi\"]]");
    try check("RETURN toLower(null),toUpper(null),trim(null),ltrim(null),rtrim(null)", "[[null,null,null,null,null]]");
    try check("UNWIND ['ÉCOLE','école','Else'] AS title WITH title WHERE toLower(title)='école' RETURN count(*)", "[[2]]");
}

test "scalar property access rejects non-containers and preserves missing values" {
    try check("RETURN null.foo,{}.foo,{foo:42}.foo", "[[null,null,42]]");
    for ([_][]const u8{ "WITH 42 AS x RETURN x.foo", "WITH true AS x RETURN x.foo", "WITH 'text' AS x RETURN x.foo", "WITH [1] AS x RETURN x.foo" }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "numeric scalars and aggregation preserve Cypher precision and spelling" {
    try check("RETURN toInteger('9007199254740993.0'),toInteger('9223372036854775807.0'),toInteger('-9223372036854775808.0')", "[[9007199254740993,9223372036854775807,-9223372036854775808]]");
    try check("UNWIND [1,1.0] AS x RETURN collect(DISTINCT toString(x))", "[[[\"1\",\"1.0\"]]]");
    try check("RETURN toString(1e20),toString(0.001),toString(-0.0),toString(1e-323)", "[[\"1.0E20\",\"0.001\",\"-0.0\",\"9.9E-324\"]]");
    try check("UNWIND [9223372036854775807,9223372036854775807] AS x RETURN avg(x)=toFloat(9223372036854775807)", "[[true]]");
    try check("UNWIND [1e308,1e308] AS x RETURN avg(x)=1e308", "[[true]]");
    try check("UNWIND [1e308,-1e308] AS x RETURN avg(x)=0.0", "[[true]]");
    try check("UNWIND [null,1,2.0] AS x RETURN avg(x)", "[[1.5]]");
    try check("UNWIND [] AS x RETURN avg(x)", "[[null]]");
}

test "bound result schemas survive empty wildcard projections and align trail cells" {
    for ([_][]const u8{
        "MATCH (n:Document) WHERE false RETURN *",
        "MATCH (n:Document) WHERE false RETURN * UNION ALL MATCH (n:Document) WHERE false RETURN *",
        "CALL { MATCH (n:Document) WHERE false RETURN * } RETURN *",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 0), result.rows.len);
        try std.testing.expectEqual(@as(usize, 1), result.columns.len);
        try std.testing.expectEqualStrings("n", result.columns[0]);
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[r:CITES]->(b)-[s:CITES]->(c) RETURN *", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expect(result.rows.len > 0);
    for (result.rows) |row| for (result.columns, row) |name, cell| {
        if (std.mem.eql(u8, name, "r") or std.mem.eql(u8, name, "s")) {
            try std.testing.expect(cell == .relationship);
        } else if (std.mem.eql(u8, name, "p")) {
            try std.testing.expect(cell == .path);
        } else try std.testing.expect(cell == .node);
    };
}

test "UNION deduplicates wildcard and explicit graph columns by name" {
    const pattern = "MATCH p=(a:Document {id:'a'})-[r:CITES]->(b)-[s:CITES]->(c) ";
    for ([_][]const u8{
        pattern ++ "RETURN * UNION " ++ pattern ++ "RETURN a,b,c,r,s,p",
        pattern ++ "RETURN a,b,c,r,s,p UNION " ++ pattern ++ "RETURN *",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 4), result.rows.len);
        try std.testing.expectEqualStrings("c", result.columns[2]);
        for (result.rows) |row| {
            try std.testing.expect(row[2] == .node);
            try std.testing.expect(row[3] == .relationship);
        }
    }
}

test "boolean and float conversions follow Neo4j scalar input rules" {
    try check("RETURN toBoolean(0),toBoolean(1),toBoolean(-1),toBoolean(' true '),toBoolean(' false '),toBoolean('bad')", "[[false,true,true,true,false,null]]");
    try check("RETURN toBoolean('\\u2003TRUE\\u3000'),toBoolean('\\u00a0true\\u00a0')", "[[true,null]]");
    try check("RETURN toFloat(' 1.5 '),toFloat('1.5d'),toFloat('2F'),toFloat('0x1.8p1'),toFloat(' .5e1 ') = 5.0", "[[1.5,1.5,2,3,true]]");
    try check("RETURN toFloat('1_0'),toFloat('0x1'),toFloat('1d2'),toFloat('\\u20031.5\\u2003')", "[[null,null,null,null]]");
}

test "string addition formats scalar operands and preserves list precedence" {
    try check("RETURN 'row-'+1,1+'-row','v='+true,false+'!', 'v='+1.5,'n='+null", "[[\"row-1\",\"1-row\",\"v=true\",\"false!\",\"v=1.5\",null]]");
    try check("RETURN 'x'+[1],[1]+'x','v='+1e20", "[[[\"x\",1],[1,\"x\"],\"v=1.0E20\"]]");
}

test "inline MATCH properties use complete node relationship and path bindings" {
    try check("MATCH (n:Document {id:n.id}) RETURN n.id ORDER BY n.id", "[[\"a\"],[\"b\"],[\"c\"]]");
    try check("MATCH (n:Document {id:n.id}) RETURN n.id LIMIT 1", "[[\"a\"]]");
    try check("MATCH (a:Document {id:b.id}),(b:Document) RETURN a.id,b.id ORDER BY a.id", "[[\"a\",\"a\"],[\"b\",\"b\"],[\"c\",\"c\"]]");
    try check("MATCH ({rank:b.rank}),(b:Document) RETURN b.id ORDER BY b.id", "[[\"a\"],[\"b\"],[\"c\"]]");
    try check("MATCH (a:Document {id:'a'})-[r:CITES {weight:r.weight}]->(b {rank:b.rank}) RETURN r.weight ORDER BY r.weight", "[[1],[2]]");
    try check("MATCH (a:Document {id:'a'})-[r:CITES {weight:b.rank}]->(b) RETURN r.weight", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[r:CITES*1..2 {weight:length(p)}]->(b) RETURN length(p)", "[[1]]");
    try check("MATCH (a:Document {id:'a'}) OPTIONAL MATCH (a)-[r:CITES {weight:b.rank+10}]->(b) RETURN a.id,b,r", "[[\"a\",null,null]]");
}

test "inline nested patterns wait for outer MATCH aliases" {
    try check("MATCH (a:Document {rank:CASE WHEN EXISTS { MATCH (b) WHERE b.rank=1 } THEN 1 ELSE 2 END}),(b:Document) RETURN a.id,b.id ORDER BY a.id,b.id", "[[\"a\",\"a\"],[\"b\",\"b\"],[\"b\",\"c\"]]");
    try check("MATCH (a:Document {rank:size([(b)-[:CITES]->(x)|x])}),(b:Document) RETURN a.id,b.id ORDER BY a.id,b.id", "[[\"b\",\"a\"],[\"b\",\"b\"]]");
    try check("MATCH (n:Document {rank:CASE WHEN EXISTS { MATCH (n) WHERE n.rank=1 } THEN 1 ELSE 0 END}) RETURN n.id LIMIT 1", "[[\"a\"]]");
}

test "shortest paths evaluate resolved inline nested predicates before selection" {
    try check("MATCH p=shortestPath((a:Document {id:'a'})-[r:CITES*1..3 {weight:CASE WHEN EXISTS { MATCH (b) WHERE b.rank=2 } THEN 2 ELSE 1 END}]->(b)) RETURN b.id,length(p)", "[[\"b\",1]]");
}

test "shortest selectors resolve later MATCH predicates and preserve surrounding bags" {
    const selected = "p=shortestPath((a:Document {id:'a'})-[:CITES*1..3]->(b:Document {id:'b',rank:CASE WHEN length(p)=x.rank THEN 2 ELSE 0 END}))";
    try check("MATCH " ++ selected ++ ",(x:Document {rank:3}) RETURN length(p)", "[[3]]");
    try check("MATCH (x:Document {rank:3})," ++ selected ++ " RETURN length(p)", "[[3]]");
    try check("MATCH (x:Document {rank:3}) MATCH " ++ selected ++ " RETURN length(p)", "[[3]]");
    try check("OPTIONAL MATCH " ++ selected ++ ",(x:Document {rank:3}) RETURN length(p)", "[[3]]");
    try check("MATCH p=allShortestPaths((a:Document {id:'a'})-[:CITES*1..3]->(b:Document {id:'b',rank:CASE WHEN length(p)=x.rank THEN 2 ELSE 0 END})),(x:Document {rank:3}) RETURN length(p)", "[[3],[3]]");
    try check("MATCH p=shortestPath((a:Document {id:'a'})-[:CITES*1..3]->(b:Document {id:'b'})),(:Document) RETURN count(*)", "[[3]]");
    try check("UNWIND [1,1] AS x MATCH p=shortestPath((a:Document {id:'a'})-[:CITES*1..3]->(b:Document {id:'b'})),(:Document) RETURN count(*)", "[[6]]");
    try check("MATCH p=shortestPath((a:Document {id:'a'})-[:CITES*1..3]->(b:Document {id:'b'})) WHERE length(p)=3 RETURN length(p)", "[]");
}

test "shortest selection groups independent patterns within a linear checkpoint budget" {
    for ([_][]const u8{ "shortestPath", "allShortestPaths" }) |selector| {
        const query = try std.fmt.allocPrint(std.testing.allocator, "MATCH p={s}((a:Document)-[:CITES*0..0]->(b)),(c:Document),(d:Document),(e:Document),(f:Document),(g:Document),(h:Document) RETURN count(*)", .{selector});
        defer std.testing.allocator.free(query);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{ .cancel_after = 100_000 };
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2187), result.rows[0][0].integer);
    }
}

test "computed containers cannot exceed the transport depth before result traversal" {
    for ([_][]const u8{
        "RETURN reduce(acc = [], x IN range(1,10000) | [acc]) AS nested",
        "RETURN reduce(acc = {}, x IN range(1,10000) | {nested:acc}) AS nested",
        "RETURN size(reduce(acc = [], x IN range(1,10000) | [acc])) AS size",
        "MATCH (n:Document) RETURN reduce(a=n,x IN range(1,64)|[a]) AS nested",
        "MATCH p=(n:Document) RETURN reduce(a=p,x IN range(1,64)|[a]) AS nested",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherValueDepthExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "shared computed containers compare and hash without expanding their DAG" {
    try check("WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a RETURN a=a, a<a, a<=a", "[[true,false,true]]");
    try check("WITH reduce(a=[null],x IN range(1,40)|[a,a]) AS a RETURN a=a, a<a", "[[null,null]]");
    try check("WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a UNWIND [a,a] AS x RETURN count(DISTINCT x)", "[[1]]");
    try check("WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a UNWIND [a,a] AS x RETURN count(*) ORDER BY collect(x)", "[[2]]");
}

test "nonidentical shared values reserve bounded work before equality ordering or output" {
    for ([_][]const u8{
        "WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a, reduce(b=[1],x IN range(1,40)|[b,b]) AS b RETURN a=b",
        "WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a, reduce(b=[1],x IN range(1,40)|[b,b]) AS b RETURN a<b",
        "WITH reduce(a=[1],x IN range(1,40)|[a,a]) AS a RETURN a",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{ .cancel_after = 20_000 };
        try std.testing.expectError(error.CypherStateBudgetExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "hash aggregation and deduplication preserve numeric map NULL and collision semantics" {
    try check("UNWIND [1,1.0,2,null,null] AS x RETURN x,count(*) ORDER BY x", "[[1,2],[2,1],[null,2]]");
    try check("UNWIND [{a:1,b:null},{b:null,a:1.0}] AS x RETURN count(DISTINCT x)", "[[1]]");
    try check("RETURN 1 AS x UNION RETURN 1.0 AS x UNION RETURN null AS x UNION RETURN null AS x", "[[1],[null]]");
    // FLOAT equality must never merge a distinct, nonrepresentable INTEGER.
    try check("UNWIND [9007199254740992,9007199254740993,9007199254740992.0] AS x RETURN count(DISTINCT x)", "[[2]]");
    try check("UNWIND [0,0.0,-0.0] AS x RETURN count(DISTINCT x)", "[[1]]");
}

test "unique grouping DISTINCT aggregates projections and UNION use linear work" {
    for ([_][]const u8{
        "UNWIND range(1,4000) AS x RETURN x,count(*)",
        "UNWIND range(9223372036854771800,9223372036854775799) AS x RETURN x,count(*)",
        "UNWIND range(1,4000) AS x RETURN DISTINCT x",
        "UNWIND range(1,4000) AS x RETURN count(DISTINCT x)",
        "UNWIND range(1,4000) AS x RETURN x UNION RETURN 0 AS x",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{ .cancel_after = 200_000 };
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 100_000 });
        defer result.deinit();
        try std.testing.expect(result.stats.intermediate_states < 100_000);
    }
}

test "repeated string predicates pay each scan while immutable Unicode functions reuse analysis" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const text = try arena.allocator().alloc(u8, 400_000);
    @memset(text, 'x');
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var cached = try c.compile(std.testing.allocator, "RETURN reduce(a=0,i IN range(1,1000)|a+size(ltrim($s)))", .{}, &diagnostic);
    defer cached.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &cached, &.{.{ .name = "s", .value = .{ .string = text } }}, .{ .intermediate_states = 30_000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 400_000_000), result.rows[0][0].integer);
    for ([_][]const u8{
        "RETURN reduce(a=0,i IN range(1,1000)|a+CASE WHEN $s CONTAINS 'z' THEN 1 ELSE 0 END)",
        "RETURN reduce(a=0,i IN range(1,1000)|a+CASE WHEN $s STARTS WITH $s THEN 1 ELSE 0 END)",
        "RETURN reduce(a=0,i IN range(1,1000)|a+size(substring($s,1)))",
    }) |query| {
        var scanned = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer scanned.deinit();
        try std.testing.expectError(error.CypherStateBudgetExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &scanned, &.{.{ .name = "s", .value = .{ .string = text } }}, .{ .intermediate_states = 30_000 }));
    }
}

test "terminal reducers and top-k retain bounded computed values and stable ties" {
    try check("MATCH (n:Document) RETURN count(*),sum(n.rank),avg(n.rank),min(n.id),max(n.id)", "[[3,6,2,\"a\",\"c\"]]");
    try check("MATCH (n:Document) RETURN n.id ORDER BY n.rank DESC SKIP 1 LIMIT 1", "[[\"b\"]]");
    try check("UNWIND [3,1,2,1] AS x RETURN x ORDER BY x LIMIT 3", "[[1],[1],[2]]");
    try check("MATCH (n:Document) WHERE n.rank<0 RETURN count(*),sum(n.rank),avg(n.rank),min(n.id),max(n.id)", "[[0,0,null,null,null]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,5000) AS x RETURN {x:x} AS m ORDER BY m.x DESC SKIP 2 LIMIT 3", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 2 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    for (result.rows, 0..) |row, index| try std.testing.expectEqual(@as(i64, @intCast(4998 - index)), row[0].map[0].value.integer);
}

test "shared-table scans preserve the union of different label predicates" {
    var aliases = graph;
    aliases.labels = &.{
        .{ .name = "Document", .table = "docs", .predicate = &.{.{ .name = "rank", .value = .{ .integer = 1 } }} },
        .{ .name = "Other", .table = "docs", .predicate = &.{.{ .name = "rank", .value = .{ .integer = 2 } }} },
    };
    aliases.relationships = &.{};
    for ([_][]const u8{ "MATCH (n) RETURN count(n)", "MATCH (n) RETURN count(n) LIMIT 1" }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), aliases, &compiled, &.{}, .{ .scanned_anchors = 3 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 3), result.stats.scanned_anchors);
    }
}

test "paged expansion batches and caches endpoint lookups without dropping parallel edges" {
    const Batched = struct {
        fixture: Fixture = .{},
        calls: usize = 0,
        fn batch(ptr: *anyopaque, a: std.mem.Allocator, identities: []const v.Identity) ![]const ?*const v.Node {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const nodes = try a.alloc(?*const v.Node, identities.len);
            for (identities, nodes) |identity, *node| node.* = try Fixture.get(&self.fixture, a, identity);
            return nodes;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return Fixture.scan(&self.fixture, a, name, cursor, limit);
        }
        fn get(ptr: *anyopaque, a: std.mem.Allocator, identity: v.Identity) !?*const v.Node {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return Fixture.get(&self.fixture, a, identity);
        }
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return Fixture.expand(&self.fixture, a, mapping, identity, direction, cursor, limit);
        }
    };
    var fixture: Batched = .{};
    var table = fixture.fixture.backend().vtable.*;
    table.get_many = Batched.batch;
    table.scan = Batched.scan;
    table.get = Batched.get;
    table.expand = Batched.expand;
    table.checkpoint = Batched.checkpoint;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES]->(b) WHERE antfly.documentKey(a)='a' RETURN b.id", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &table }, graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
}

test "shortest paths preserve rebound route aliases and bound-target frontier limits" {
    try check("MATCH (a:Document {id:'a'})-[r:CITES]->(b) WHERE r.weight=2 WITH a,b,r MATCH p=shortestPath((a)-[r:CITES]->(b)) RETURN r.weight", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[r:CITES]->(b) WHERE r.weight=2 WITH a,b,p MATCH p=shortestPath((a)-[:CITES]->(b)) RETURN length(p)", "[[1]]");
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document) WHERE antfly.documentKey(a)='a' MATCH (b:Document) WHERE antfly.documentKey(b)='b' MATCH p=allShortestPaths((a)-[:CITES*1..3]->(b)) RETURN length(p)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .scanned_anchors = 2, .explored_edges = 2 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(usize, 2), result.stats.explored_edges);
}

test "streaming aggregation composes WITH keys and aggregate expressions" {
    try check("MATCH (n:Document) WITH count(*) AS c RETURN c+1", "[[4]]");
    try check("MATCH (n:Document) WITH n.rank%2 AS k,count(*) AS c RETURN sum(c)", "[[3]]");
    try check("MATCH (n:Document) WITH n WITH n.rank AS r WHERE r>1 RETURN count(*)+1,sum(r)*2", "[[3,10]]");
    try check("MATCH (n:Document) RETURN n.rank%2 AS bucket,count(*)+10 AS total ORDER BY bucket", "[[0,11],[1,12]]");
    try check("MATCH (n:Document) RETURN {total:count(*),avg:avg(n.rank)}", "[[{\"total\":3,\"avg\":2}]]");
    try check("MATCH (n:Document) WHERE n.rank<0 WITH n RETURN count(*)+1", "[[1]]");
    try check("MATCH (n:Document) WITH n WHERE n.rank<0 RETURN count(*)", "[[0]]");
    try check("MATCH (n:Document) WITH n.rank AS r RETURN r%2 AS b,sum(r) AS s ORDER BY s DESC LIMIT 1", "[[1,4]]");
    try check("MATCH (n:Document) RETURN n.rank%2 AS b,sum(n.rank) AS s ORDER BY sum(n.rank) DESC", "[[1,4],[0,2]]");
}

test "nested owner RPC scans keep grouped pipeline memory independent of input size" {
    const Large = struct {
        count: usize,
        fn bind(_: *anyopaque, _: std.mem.Allocator, _: catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn get(_: *anyopaque, _: std.mem.Allocator, _: v.Identity) !?*const v.Node {
            return null;
        }
        fn expand(_: *anyopaque, _: std.mem.Allocator, _: catalog.Relationship, _: v.Identity, _: c.ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
            return .{ .items = &.{} };
        }
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const start = if (after) |key| try std.fmt.parseInt(usize, key, 10) else 0;
            const end = start + @min(limit, self.count - start);
            const nodes = try a.alloc(*const v.Node, end - start);
            for (nodes, start..) |*out, index| {
                const key = try std.fmt.allocPrint(a, "n{d}", .{index});
                const node = try a.create(v.Node);
                const properties = try a.dupe(v.Entry, &.{ .{ .name = "id", .value = .{ .string = key } }, .{ .name = "score", .value = .{ .integer = @intCast(index) } } });
                node.* = .{ .identity = .{ .table = "docs", .key = key }, .labels = &.{"Document"}, .properties = properties };
                out.* = node;
            }
            return .{ .items = nodes, .next = if (end < self.count) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn backend(self: *@This()) b.Backend {
            return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand } };
        }
    };
    const definition: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs" }}, .relationships = &.{} };
    const Link = struct {
        child: b.Backend,
        fn call(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return (@import("rpc.zig").Owner{ .backend = self.child, .graph = definition, .table = "docs" }).execute(a, body);
        }
    };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) WITH n.score AS s RETURN s%2 AS bucket,count(*) AS total ORDER BY bucket", .{}, &diagnostic);
    defer compiled.deinit();
    var previous: usize = 0;
    var previous_physical: usize = 0;
    for ([_]usize{ 2000, 10000 }) |count| {
        var fixture: Large = .{ .count = count };
        var first_link: Link = .{ .child = fixture.backend() };
        var first_client: @import("rpc.zig").Client = .{ .incarnation = 1, .revision = 1, .channel = .{ .ptr = &first_link, .call = Link.call } };
        var second_link: Link = .{ .child = first_client.backend() };
        var second_client: @import("rpc.zig").Client = .{ .incarnation = 1, .revision = 1, .channel = .{ .ptr = &second_link, .call = Link.call } };
        var physical: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
        var request_arena = std.heap.ArenaAllocator.init(physical.allocator());
        defer request_arena.deinit();
        var result = try runtime.execute(request_arena.allocator(), second_client.backend(), definition, &compiled, &.{}, .{ .memory_bytes = 8 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(count / 2)), result.rows[0][1].integer);
        if (previous != 0) try std.testing.expect(result.stats.memory_bytes <= previous + (1 << 20));
        previous = result.stats.memory_bytes;
        if (previous_physical != 0) try std.testing.expect(physical.peak <= previous_physical + (1 << 20));
        previous_physical = physical.peak;
    }
}

test "streamed traversal reductions preserve bags, scopes, ordering and limits" {
    try check("MATCH (a:Document {id:'a'}) MATCH (a)-[:CITES]->(b) RETURN count(*),sum(b.rank)", "[[2,4]]");
    try check("MATCH (a:Document {id:'a'}) MATCH (a)-[:CITES]->(b) WITH b.rank AS rank RETURN rank,count(*)", "[[2,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*1..2]->(b) RETURN b.id,length(p) ORDER BY b.id,length(p) DESC LIMIT 3", "[[\"a\",2],[\"a\",2],[\"b\",1]]");
    try check("MATCH (a:Document {id:'a'}) MATCH (a)-[:CITES]->(b) RETURN * LIMIT 1", "[[{\"$type\":\"node\",\"identity\":{\"table\":\"docs\",\"key\":\"a\"},\"labels\":[\"Document\"],\"properties\":{\"id\":\"a\",\"title\":\"Alpha\",\"rank\":1}},{\"$type\":\"node\",\"identity\":{\"table\":\"docs\",\"key\":\"b\"},\"labels\":[\"Document\"],\"properties\":{\"id\":\"b\",\"title\":\"Beta\",\"rank\":2}}]]");
    try check("UNWIND [1,1] AS x MATCH (a:Document {id:'a'})-[:CITES]->(b) RETURN x,count(*)", "[[1,4]]");
    try check("MATCH (a:Document {id:'c'})-[:CITES]->(b) RETURN count(*),sum(b.rank)", "[[0,0]]");
}

test "shortest routes stream bounded projections and compound reductions" {
    try check("MATCH p=allShortestPaths((a:Document {id:'a'})-[:CITES*1..4]->(b)) RETURN min(length(p)),count(*)", "[[1,6]]");
    try check("MATCH p=shortestPath((a:Document {id:'a'})-[:CITES*1..4]-(b)) RETURN b.id,length(p) ORDER BY b.id LIMIT 3", "[[\"a\",2],[\"b\",1],[\"c\",2]]");
    try check("MATCH p=allShortestPaths((a:Document {id:'a'})-[:CITES*1..4]->(b)) RETURN length(p) LIMIT 1", "[[1]]");
}

test "DAG count agrees with materialized selectors for directed and undirected cycles" {
    for ([_][]const u8{ "shortestPath", "allShortestPaths" }) |selector| {
        for ([_][]const u8{ "->", "-" }) |direction| {
            const fast = try std.fmt.allocPrint(std.testing.allocator, "MATCH p={s}((a:Document {{id:'a'}})-[:CITES*1..4]{s}(b)) RETURN count(*)", .{ selector, direction });
            defer std.testing.allocator.free(fast);
            const slow = try std.fmt.allocPrint(std.testing.allocator, "MATCH p={s}((a:Document {{id:'a'}})-[:CITES*1..4]{s}(b)) WITH p RETURN count(*)", .{ selector, direction });
            defer std.testing.allocator.free(slow);
            var d: c.Diagnostic = .{};
            var fast_compiled = try c.compile(std.testing.allocator, fast, .{}, &d);
            defer fast_compiled.deinit();
            var slow_compiled = try c.compile(std.testing.allocator, slow, .{}, &d);
            defer slow_compiled.deinit();
            var fixture: Fixture = .{};
            var actual = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &fast_compiled, &.{}, .{});
            defer actual.deinit();
            var expected = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &slow_compiled, &.{}, .{});
            defer expected.deinit();
            try std.testing.expectEqual(expected.rows[0][0].integer, actual.rows[0][0].integer);
        }
    }
}

test "endpoint planning preserves ordered path and relationship bindings" {
    try check("MATCH p=(a:Document)-[r:CITES*2..2]->(b:Document {id:'c'}) RETURN a.id,[n IN nodes(p)|n.id],[e IN r|e.weight] ORDER BY a.id,r[0].weight", "[[\"a\",[\"a\",\"b\",\"c\"],[1,null]],[\"a\",[\"a\",\"b\",\"c\"],[2,null]]]");
    try check("MATCH (b:Document {id:'c'}) MATCH p=(a)-[r:CITES*2..2]->(b) RETURN a.id,[n IN nodes(p)|n.id] ORDER BY r[0].weight", "[[\"a\",[\"a\",\"b\",\"c\"]],[\"a\",[\"a\",\"b\",\"c\"]]]");
}

test "EXISTS demand preserves pagination and global aggregation" {
    try check("RETURN EXISTS { MATCH (a:Document {id:'a'})-[:CITES]->(b) RETURN b SKIP 1 }, EXISTS { MATCH (a:Document {id:'a'})-[:CITES]->(b) RETURN b SKIP 2 }, EXISTS { MATCH (a:Document) RETURN a LIMIT 0 }", "[[true,false,false]]");
    try check("RETURN EXISTS { MATCH (a:Document {id:'missing'}) RETURN count(*) }, EXISTS { MATCH (a:Document {id:'missing'}) RETURN count(*) LIMIT 0 }, EXISTS { MATCH (a:Document {id:'missing'}) RETURN a }", "[[true,false,false]]");
    try check("RETURN EXISTS { MATCH (a:Document {id:'a'})-[:CITES]->(b) RETURN DISTINCT b SKIP 1 }", "[[false]]");
}

test "lower bound operators agree with exact cyclic route selection" {
    try check("MATCH p=allShortestPaths((a:Document {id:'a'})-[*2..3]->(b)) RETURN count(*)", "[[8]]");
    for ([_][]const u8{ "", "REPEATABLE ELEMENTS ", "DIFFERENT RELATIONSHIPS " }) |mode| {
        for ([_][]const u8{ "shortestPath", "allShortestPaths" }) |selector| {
            for ([_][]const u8{ "->", "-" }) |direction| {
                const fast = try std.fmt.allocPrint(std.testing.allocator, "MATCH {s}p={s}((a:Document {{id:'a'}})-[:CITES*2..4]{s}(b)) RETURN count(*)", .{ mode, selector, direction });
                defer std.testing.allocator.free(fast);
                // The later pattern forces the history-preserving selector.
                const exact = try std.fmt.allocPrint(std.testing.allocator, "MATCH {s}p={s}((a:Document {{id:'a'}})-[:CITES*2..4]{s}(b)),(:Person) RETURN count(*)", .{ mode, selector, direction });
                defer std.testing.allocator.free(exact);
                var diagnostic: c.Diagnostic = .{};
                var compiled = try c.compile(std.testing.allocator, fast, .{}, &diagnostic);
                defer compiled.deinit();
                var reference = try c.compile(std.testing.allocator, exact, .{}, &diagnostic);
                defer reference.deinit();
                var fixture: Fixture = .{};
                var actual = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
                defer actual.deinit();
                var expected = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &reference, &.{}, .{});
                defer expected.deinit();
                try std.testing.expectEqual(expected.rows[0][0].integer, actual.rows[0][0].integer);
            }
        }
    }
}

test "lazy lower bound probes preserve cyclic trail history and multiplicity" {
    try check("MATCH (a:Document {id:'a'}),(c:Document {id:'c'}) MATCH p=allShortestPaths((a)-[:CITES*3..8]->(c)) RETURN length(p)", "[[4],[4]]");
    try check("MATCH (a:Document {id:'a'}),(c:Document {id:'c'}) MATCH p=allShortestPaths((a)-[:CITES*6..8]->(c)) RETURN count(*)", "[[0]]");
    try check("MATCH (a:Document {id:'a'}),(c:Document {id:'c'}) MATCH REPEATABLE ELEMENTS p=allShortestPaths((a)-[:CITES*6..8]->(c)) RETURN count(*)", "[[8]]");
    try check("MATCH (a:Document {id:'a'}) MATCH p=allShortestPaths((a)-[:CITES*2..8]->(a)) RETURN count(*)", "[[2]]");
    try check("MATCH (a:Document {id:'a'}) MATCH p=allShortestPaths((a)-[:CITES*3..8]->(a)) RETURN count(*)", "[[0]]");
}

test "property liveness retains complete entities at unknown consumers" {
    const Case = struct { query: []const u8, fields: ?[]const []const u8 };
    for ([_]Case{
        .{ .query = "CALL antfly.search('docs',{query:'needle'}) YIELD score RETURN sum(score)", .fields = &.{} },
        .{ .query = "CALL antfly.search('docs',{query:'needle'}) YIELD node AS n RETURN n.title", .fields = &.{"title"} },
        .{ .query = "CALL antfly.search('docs',{query:'needle'}) YIELD node AS n WITH n AS m RETURN m.rank", .fields = &.{"rank"} },
        .{ .query = "CALL antfly.search('docs',{query:'needle'}) YIELD node AS n RETURN n", .fields = null },
        .{ .query = "MATCH (n:Document) RETURN count(*)", .fields = &.{} },
        .{ .query = "MATCH (n:Document {id:'a'}) RETURN n.rank", .fields = &.{ "id", "rank" } },
        .{ .query = "MATCH (n:Document) WHERE n.rank>1 RETURN labels(n),antfly.documentKey(n)", .fields = &.{"rank"} },
        .{ .query = "MATCH (n:Document) RETURN n", .fields = null },
        .{ .query = "MATCH (n:Document) RETURN properties(n)", .fields = null },
        .{ .query = "MATCH (n:Document) WITH n AS m RETURN m.title", .fields = &.{"title"} },
        .{ .query = "MATCH (n:Document) WITH n AS m WITH m AS k WHERE k.rank>1 RETURN k.title", .fields = &.{ "rank", "title" } },
        .{ .query = "MATCH (n:Document) WITH * RETURN n.title", .fields = &.{"title"} },
        .{ .query = "MATCH (n:Document) WITH n AS m RETURN m", .fields = null },
        .{ .query = "MATCH (n:Document) WITH n AS m RETURN properties(m)", .fields = null },
        .{ .query = "MATCH (n:Document) WITH n AS m RETURN m[$field]", .fields = null },
        .{ .query = "MATCH (n:Document) WITH n AS m RETURN *", .fields = null },
        .{ .query = "MATCH (n:Document) WITH n.title AS n RETURN n", .fields = &.{"title"} },
        .{ .query = "MATCH (n:Document) WITH n AS m RETURN m.title UNION ALL MATCH (m:Document) RETURN m.rank", .fields = &.{ "rank", "title" } },
        .{ .query = "MATCH p=(n:Document)-[:CITES]->(m) RETURN nodes(p)", .fields = null },
        .{ .query = "MATCH (n:Document) RETURN n[$field]", .fields = null },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const actual = try @import("read_fields.zig").required(arena.allocator(), compiled.query, graph);
        try std.testing.expectEqual(case.fields == null, actual == null);
        if (case.fields) |fields| {
            try std.testing.expectEqual(fields.len, actual.?.len);
            for (fields, actual.?) |expected, field| try std.testing.expectEqualStrings(expected, field);
        }
    }
}

test "interior anchors preserve routes aliases uniqueness and deferred constraints" {
    try check("MATCH p=(a:Document)-[l:CITES]->(m:Document {id:'b'})-[r:CITES]->(z:Document) RETURN a.id,z.id,[n IN nodes(p)|n.id],l.weight ORDER BY a.id,z.id,l.weight", "[[\"a\",\"a\",[\"a\",\"b\",\"a\"],1],[\"a\",\"a\",[\"a\",\"b\",\"a\"],2],[\"a\",\"c\",[\"a\",\"b\",\"c\"],1],[\"a\",\"c\",[\"a\",\"b\",\"c\"],2]]");
    try check("MATCH p=(a:Document)-[l:CITES*1..2]->(m:Document {id:'b'})-[r:CITES]->(z:Document {id:'c'}) RETURN [n IN nodes(p)|n.id],[e IN l|e.weight] ORDER BY length(p),l[0].weight,l[1].weight", "[[[\"a\",\"b\",\"c\"],[1]],[[\"a\",\"b\",\"c\"],[2]],[[\"b\",\"a\",\"b\",\"c\"],[null,1]],[[\"b\",\"a\",\"b\",\"c\"],[null,2]]]");
    try check("MATCH (a)-[:CITES]-(m:Document {id:'c'})-[:CITES]-(b) RETURN count(*)", "[[0]]");
    try check("MATCH REPEATABLE ELEMENTS (a)-[:CITES]-(m:Document {id:'c'})-[:CITES]-(b) RETURN count(*)", "[[1]]");
    try check("MATCH p=(a {id:z.id})-[:CITES]->(m:Document {id:'b'})-[:CITES]->(z) RETURN [n IN nodes(p)|n.id]", "[[[\"a\",\"b\",\"a\"]],[[\"a\",\"b\",\"a\"]]]");
}

test "document key seeks outrank arbitrarily many candidate predicates" {
    const Filtered = struct {
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, _: []const b.PropertyFilter, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            return Fixture.scan(ptr, a, table, after, limit);
        }
    };
    var source: std.ArrayList(u8) = .empty;
    defer source.deinit(std.testing.allocator);
    try source.appendSlice(std.testing.allocator, "MATCH (a:Document)-[:CITES]->(b:Document {");
    for (0..1001) |i| {
        const field = try std.fmt.allocPrint(std.testing.allocator, "{s}f{d}:0", .{ if (i == 0) "" else ",", i });
        defer std.testing.allocator.free(field);
        try source.appendSlice(std.testing.allocator, field);
    }
    try source.appendSlice(std.testing.allocator, "}) WHERE antfly.documentKey(a)='a' RETURN count(*)");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, source.items, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const base = fixture.backend();
    var vtable = base.vtable.*;
    vtable.scan_where = Filtered.scan;
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = base.ptr, .vtable = &vtable }, graph, &compiled, &.{}, .{ .scanned_anchors = 1 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 0), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 1), result.stats.scanned_anchors);
}

test "streaming distinct preserves typed keys filters pagination and grouping" {
    try check("MATCH (n:Document) RETURN DISTINCT n.rank%2 AS bucket ORDER BY bucket", "[[0],[1]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank%2 AS bucket SKIP 1 LIMIT 1", "[[0]]");
    try check("MATCH (n:Document) WITH DISTINCT n.rank%2 AS bucket RETURN count(*)", "[[2]]");
    try check("MATCH (n:Document) WITH DISTINCT n.rank%2 AS bucket WHERE bucket=1 RETURN count(*)", "[[1]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.missing LIMIT 1", "[[null]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank%2 AS bucket,count(*) ORDER BY bucket", "[[0,1],[1,2]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank%2 AS bucket SKIP 1 LIMIT 0", "[]");
}

test "streaming pattern joins preserve equality bags and shared uniqueness" {
    try check("MATCH (a:Document),(b:Document) WHERE a.rank=b.rank RETURN count(*)", "[[3]]");
    try check("MATCH (a:Document),(b:Document) WHERE a.rank=b.rank RETURN a.id,b.id ORDER BY a.id", "[[\"a\",\"a\"],[\"b\",\"b\"],[\"c\",\"c\"]]");
    try check("MATCH (a:Document),(b:Document) WHERE a.rank=b.rank AND a.id<>b.id RETURN count(*)", "[[0]]");
    try check("MATCH (a:Document),(b:Document) WHERE a.missing=b.missing RETURN count(*)", "[[0]]");
    try check("MATCH (a:Document)-[:CITES]->(b),(b)-[:CITES]->(c) RETURN count(*)", "[[6]]");
}

test "node pull cursor owns parameters and releases each page" {
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) WHERE n.rank>$rank RETURN n.id AS id", .{}, &diagnostic);
    defer compiled.deinit();
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, fixture.backend(), graph, compiled.query, &.{.{ .name = "rank", .value = .{ .integer = 1 } }}, .{ .scanned_anchors = 3, .memory_bytes = 1 << 20 })).?;
    defer cursor.close();
    var page_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer page_arena.deinit();
    const first = try cursor.next(page_arena.allocator(), 1);
    try std.testing.expectEqual(@as(usize, 1), first.len);
    try std.testing.expectEqualStrings("b", first[0][0].value.string);
    _ = page_arena.reset(.free_all);
    const second = try cursor.next(page_arena.allocator(), 1);
    try std.testing.expectEqualStrings("c", second[0][0].value.string);
    _ = page_arena.reset(.free_all);
    try std.testing.expectEqual(@as(usize, 0), (try cursor.next(page_arena.allocator(), 1)).len);
}

test "oversized unindexed join cache declines without failing the query" {
    const Wide = struct {
        const payload: [256 << 10]u8 = @splat('x');
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, after: ?[]const u8, demand: usize) !b.Page(*const v.Node) {
            var page = try Fixture.scan(ptr, a, table, after, demand);
            const nodes = try a.alloc(*const v.Node, page.items.len);
            for (page.items, nodes) |node, *out| {
                const copy = try a.create(v.Node);
                copy.* = node.*;
                const fields = try a.alloc(v.Entry, node.properties.len + 1);
                @memcpy(fields[0..node.properties.len], node.properties);
                fields[node.properties.len] = .{ .name = "payload", .value = .{ .string = &payload } };
                copy.properties = fields;
                out.* = copy;
            }
            page.items = nodes;
            return page;
        }
    };
    var fixture: Fixture = .{};
    const backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.scan = Wide.scan;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document),(b:Document) WHERE a.rank=b.rank RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = backend.ptr, .vtable = &vtable }, graph, &compiled, &.{}, .{ .memory_bytes = 4 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 3), result.rows[0][0].integer);
    try std.testing.expect(result.stats.scanned_anchors > 9);
}

test "streaming distinct aggregates preserve nulls numeric equality groups and min max lifetime" {
    try check("MATCH (n:Document) RETURN count(DISTINCT n.rank%2),sum(DISTINCT n.rank%2),avg(DISTINCT n.rank%2),min(DISTINCT n.rank%2),max(DISTINCT n.rank%2)", "[[2,1,0.5,0,1]]");
    try check("MATCH (n:Document) RETURN count(DISTINCT n.rank),count(DISTINCT n.rank%2),sum(DISTINCT n.rank)", "[[3,2,6]]");
    try check("MATCH (n:Document) RETURN count(DISTINCT n.missing),sum(DISTINCT n.missing),avg(DISTINCT n.missing)", "[[0,0,null]]");
    try check("MATCH (n:Document) WHERE n.rank<0 RETURN count(DISTINCT n.rank)", "[[0]]");
    try check("MATCH (n:Document) RETURN n.rank%2 AS bucket,count(DISTINCT n.rank%2) ORDER BY bucket", "[[0,1],[1,1]]");
    // Duplicate integer/float keys and nested structures share GROUP BY equality.
    try check("UNWIND [1,1.0,2,null,[1],[1.0],{x:1},{x:1.0}] AS x MATCH (n:Document) RETURN count(DISTINCT x)", "[[4]]");
    try check("MATCH (n:Document) WITH n.rank%2 AS x RETURN min(DISTINCT toString(x)),max(DISTINCT toString(x))", "[[\"0\",\"1\"]]");
}

test "pull traversal preserves bags constraints path directions and page ownership" {
    const Paged = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, demand: usize) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            const start = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(all.items.len, start + demand);
            return .{ .items = all.items[start..end], .next = if (end < all.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    const queries = [_][]const u8{
        "MATCH p=(a:Document)-[r:CITES]->(b:Document) RETURN a.id,b.id,r.weight,nodes(p)",
        "MATCH p=(a:Document)-[r:CITES*0..3]->(b:Document) RETURN a.id,b.id,r,p",
        "MATCH REPEATABLE ELEMENTS p=(a:Document)-[r:CITES*0..3]->(b:Document) RETURN a.id,b.id,r,p",
        "MATCH p=(a:Document)-[r:CITES*1..2]-(b:Document) RETURN a.id,b.id,r,p",
        "MATCH (a:Document {rank:b.rank-1})-[r:CITES {weight:b.rank-1}]->(b:Document) RETURN a.id,b.id",
        "MATCH p=(a:Document)-[r:CITES*1..2]->(b:Document) WHERE antfly.documentKey(b)='c' RETURN a.id,b.id,r,p",
        "MATCH (a:Document)-[r:CITES]->(b:Document)-[s:CITES]->(c:Document) RETURN a.id,b.id,c.id,r,s",
    };
    for (queries) |query| for ([_]usize{ 1, 3, 128 }) |page_size| {
        var fixture: Fixture = .{};
        const backend = fixture.backend();
        var table = backend.vtable.*;
        table.expand = Paged.expand;
        const paged: b.Backend = .{ .ptr = backend.ptr, .vtable = &table };
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var expected = try runtime.execute(std.testing.allocator, paged, graph, &compiled, &.{}, .{});
        defer expected.deinit();
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, paged, graph, compiled.query, &.{}, .{ .memory_bytes = 2 << 20 })).?;
        defer cursor.close();
        var retained = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer retained.deinit();
        const a = retained.allocator();
        var page_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer page_arena.deinit();
        var actual_json: std.ArrayList([]const u8) = .empty;
        while (true) {
            const rows = try cursor.next(page_arena.allocator(), page_size);
            if (rows.len == 0) break;
            try std.testing.expect(rows.len <= page_size);
            for (rows) |row| {
                const values = try a.alloc(v.Value, row.len);
                for (row, values) |entry, *value| value.* = entry.value;
                try actual_json.append(a, try std.json.Stringify.valueAlloc(a, values, .{}));
            }
            _ = page_arena.reset(.free_all);
        }
        const expected_json = try a.alloc([]const u8, expected.rows.len);
        for (expected.rows, expected_json) |row, *json| json.* = try std.json.Stringify.valueAlloc(a, row, .{});
        const Order = struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        };
        std.mem.sort([]const u8, expected_json, {}, Order.less);
        std.mem.sort([]const u8, actual_json.items, {}, Order.less);
        try std.testing.expectEqual(expected_json.len, actual_json.items.len);
        for (expected_json, actual_json.items) |left, right| try std.testing.expectEqualStrings(left, right);
    };
}

test "optional streaming joins preserve null extension before downstream filters and reducers" {
    try check("OPTIONAL MATCH (a:Document),(b:Document) WHERE a.rank=b.rank RETURN count(*)", "[[3]]");
    try check("MATCH (a:Document) OPTIONAL MATCH (b:Document) WHERE a.rank=b.rank RETURN count(b)", "[[3]]");
    try check("MATCH (a:Document) OPTIONAL MATCH (b:Person) WHERE a.id=b.id WITH b WHERE b IS NULL RETURN count(*)", "[[2]]");
    try check("OPTIONAL MATCH (a:Document),(b:Person) WHERE a.rank<0 RETURN count(*),count(a),collect(a)", "[[1,0,[]]]");
    try check("OPTIONAL MATCH (a:Document),(b:Person) WHERE a.rank<0 WITH a WHERE a IS NOT NULL RETURN count(*)", "[[0]]");
    try check("MATCH (a:Document) MATCH (b:Document) WHERE a.rank=b.rank RETURN count(*)", "[[3]]");
}

test "pull traversal charges cumulative edge budgets and closes suspended frames" {
    const Paged = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, demand: usize) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            const start = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(all.items.len, start + demand);
            return .{ .items = all.items[start..end], .next = if (end < all.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    for ([_]bool{ false, true }) |exhaust| {
        var fixture: Fixture = .{};
        const backend = fixture.backend();
        var table = backend.vtable.*;
        table.expand = Paged.expand;
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES]->(b:Document) RETURN b.id", .{}, &diagnostic);
        defer compiled.deinit();
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = backend.ptr, .vtable = &table }, graph, compiled.query, &.{}, .{ .explored_edges = 1 })).?;
        defer cursor.close();
        var page = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), (try cursor.next(page.allocator(), 1)).len);
        _ = page.reset(.free_all);
        if (exhaust) try std.testing.expectError(error.CypherEdgeBudgetExceeded, cursor.next(page.allocator(), 1));
    }
}

test "pull traversal drains more than one SQL sized page" {
    const Wide = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, demand: usize) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            if (all.items.len == 0) return .{ .items = &.{} };
            const start = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(@as(usize, 153), start + demand);
            const items = try a.alloc(*const v.Relationship, end - start);
            @memset(items, all.items[0]);
            return .{ .items = items, .next = if (end < 153) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    var fixture: Fixture = .{};
    const backend = fixture.backend();
    var table = backend.vtable.*;
    table.expand = Wide.expand;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES]->(b:Document) RETURN b.id", .{}, &diagnostic);
    defer compiled.deinit();
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = backend.ptr, .vtable = &table }, graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    var page = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer page.deinit();
    var total: usize = 0;
    while (!cursor.done) {
        const rows = try cursor.next(page.allocator(), 128);
        total += rows.len;
        _ = page.reset(.free_all);
    }
    try std.testing.expectEqual(@as(usize, 306), total);
}

test "pull cursor owns a stable backend binding across suspended routes" {
    const Bound = struct {
        fixture: Fixture = .{},
        a: ?std.mem.Allocator = null,
        binds: usize = 0,
        fn bind(ptr: *anyopaque, a: std.mem.Allocator, _: catalog.Graph) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.a = a;
            self.binds += 1;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, after: ?[]const u8, demand: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return Fixture.scan(&self.fixture, a, name, after, demand);
        }
        fn get(ptr: *anyopaque, a: std.mem.Allocator, identity: v.Identity) !?*const v.Node {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const node = (try Fixture.get(&self.fixture, a, identity)) orelse return null;
            // Overlays retain imported nodes using the allocator passed at bind.
            return (try (v.Value{ .node = node }).clone(self.a.?)).node;
        }
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, demand: usize) !b.Page(*const v.Relationship) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const all = try Fixture.expand(&self.fixture, a, mapping, identity, direction, null, demand);
            const start = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(all.items.len, start + demand);
            return .{ .items = all.items[start..end], .next = if (end < all.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    var provider: Bound = .{};
    const backend: b.Backend = .{ .ptr = &provider, .vtable = &.{ .bind = Bound.bind, .checkpoint = Bound.checkpoint, .scan = Bound.scan, .get = Bound.get, .expand = Bound.expand } };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES]->(b:Document) RETURN b.id", .{}, &diagnostic);
    defer compiled.deinit();
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    var page = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer page.deinit();
    var total: usize = 0;
    while (!cursor.done) {
        total += (try cursor.next(page.allocator(), 1)).len;
        _ = page.reset(.free_all);
    }
    try std.testing.expectEqual(@as(usize, 4), total);
    try std.testing.expectEqual(@as(usize, 1), provider.binds);
}

test "pull cursor releases binding allocations when backend initialization fails" {
    const Failing = struct {
        fn bind(_: *anyopaque, a: std.mem.Allocator, _: catalog.Graph) !void {
            _ = try a.alloc(u8, 1 << 20);
            return error.CypherCancelled;
        }
        fn startup(_: *anyopaque) b.StartupWork {
            return .{ .scanned_anchors = 2, .explored_edges = 1, .intermediate_states = 3 };
        }
    };
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var table = backend.vtable.*;
    table.bind = Failing.bind;
    backend.vtable = &table;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES]->(b:Document) RETURN b.id", .{}, &diagnostic);
    defer compiled.deinit();
    try std.testing.expectError(error.CypherCancelled, runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, &.{}, .{}));
    table.bind = Fixture.bind;
    table.startup_work = Failing.startup;
    try std.testing.expectError(error.CypherAnchorBudgetExceeded, runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, &.{}, .{ .scanned_anchors = 1 }));
    try std.testing.expectError(error.CypherEdgeBudgetExceeded, runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, &.{}, .{ .explored_edges = 0 }));
}

test "lazy overlay scans preserve opaque native tokens and staged bags" {
    const Opaque = struct {
        fn scan(_: *anyopaque, a: std.mem.Allocator, _: []const u8, after: ?[]const u8, demand: usize) !b.Page(*const v.Node) {
            const Token = struct { offset: usize };
            const start = if (after) |text| (try std.json.parseFromSliceLeaky(Token, a, text, .{})).offset else 0;
            const end = @min(@as(usize, 3), start + demand);
            const items = try a.alloc(*const v.Node, end - start);
            for (items, start..) |*item, index| item.* = &Fixture.nodes[index];
            return .{ .items = items, .next = if (end < 3) try std.json.Stringify.valueAlloc(a, Token{ .offset = end }, .{}) else null };
        }
        fn identity(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: bool) !v.Identity {
            return error.CypherMutationsUnavailable;
        }
        fn commit(_: *anyopaque, _: std.mem.Allocator, _: []const b.Mutation) !void {
            return error.CypherMutationsUnavailable;
        }
    };
    var fixture: Fixture = .{};
    var base = fixture.backend();
    var table = base.vtable.*;
    table.scan = Opaque.scan;
    base.vtable = &table;
    var overlay: @import("transaction.zig").Overlay = .{ .lazy = true, .base = base, .host = .{ .ptr = &fixture, .new_identity = Opaque.identity, .commit = Opaque.commit } };
    const backend = overlay.backend();
    var retained = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer retained.deinit();
    const a = retained.allocator();
    try backend.vtable.bind(backend.ptr, a, graph);
    try backend.vtable.begin.?(backend.ptr);
    const changed: v.Node = .{ .identity = Fixture.nodes[1].identity, .labels = &.{"Document"}, .properties = &.{.{ .name = "id", .value = .{ .string = "B" } }} };
    const added: v.Node = .{ .identity = .{ .table = "docs", .key = "d" }, .labels = &.{"Document"}, .properties = &.{.{ .name = "id", .value = .{ .string = "d" } }} };
    try overlay.stageReadDocument(a, changed.identity, &changed);
    try overlay.stageReadDocument(a, added.identity, &added);
    try overlay.stageReadDocument(a, Fixture.nodes[2].identity, null);
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN n.id", .{}, &diagnostic);
    defer compiled.deinit();
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    var page = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer page.deinit();
    var ids: std.StringHashMapUnmanaged(void) = .empty;
    var total: usize = 0;
    while (!cursor.done) {
        for (try cursor.next(page.allocator(), 1)) |row| {
            total += 1;
            try ids.put(a, try a.dupe(u8, row[0].value.string), {});
        }
        _ = page.reset(.free_all);
    }
    try std.testing.expectEqual(@as(usize, 3), total);
    try std.testing.expect(ids.contains("a") and ids.contains("B") and ids.contains("d"));
}

test "streamed UNWIND retains null scalar bag scope and optional semantics" {
    try check("MATCH (n:Document) UNWIND [1,1,null] AS x RETURN count(*),count(x),sum(n.rank)", "[[9,6,18]]");
    try check("MATCH (n:Document) UNWIND null AS x RETURN count(*)", "[[0]]");
    try check("MATCH (n:Document) UNWIND 7 AS x WITH n.rank AS rank,x WHERE rank>1 UNWIND [x,x+1] AS y RETURN count(*),sum(y)", "[[4,30]]");
    try check("MATCH (n:Document) UNWIND [n.rank,n.rank] AS x WITH DISTINCT x RETURN collect(x)", "[[[1,2,3]]]");
    try check("OPTIONAL MATCH (n:Document {id:'absent'}) UNWIND [] AS x RETURN count(*)", "[[0]]");
    try check("MATCH (n:Document) UNWIND [n.rank,0] AS x RETURN x ORDER BY x DESC SKIP 1 LIMIT 2", "[[2],[1]]");
    try check("MATCH (n:Document) UNWIND [n.rank,n.rank+10] AS x RETURN x LIMIT 1", "[[1]]");
}

test "multi-pattern pull cursor preserves full MATCH state across tiny pages" {
    const Paged = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, demand: usize) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, demand);
            const start = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(all.items.len, start + demand);
            return .{ .items = all.items[start..end], .next = if (end < all.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    const queries = [_][]const u8{
        "MATCH (a:Document),(b:Document) RETURN a.id,b.id",
        "MATCH (a:Document),(b:Document) WHERE a.rank=b.rank RETURN a.id,b.id",
        "MATCH (a:Document {rank:b.rank}),(b:Document) RETURN a.id,b.id",
        "MATCH (a:Document)-[:CITES]->(b),(b)-[:CITES]->(c) RETURN a.id,b.id,c.id",
        "MATCH (a:Document)-[r:CITES]->(b),(a)-[r]->(b) RETURN a.id",
        "MATCH p=(a:Document)-[:CITES*0..2]->(b),(b:Document) RETURN length(p),a.id,b.id",
        "MATCH p=(a:Document)-[l:CITES]->(m:Document)-[r:CITES]->(z:Document) WHERE antfly.documentKey(m)='b' RETURN a.id,z.id,[n IN nodes(p)|n.id],l.weight",
        "MATCH p=(a:Document)-[l:CITES*0..2]->(m:Document)-[r:CITES]->(z:Document) WHERE antfly.documentKey(m)='b' RETURN [n IN nodes(p)|n.id],[e IN l|e.weight]",
        "MATCH p=(a {id:z.id})-[:CITES]->(m:Document)-[:CITES]->(z) WHERE antfly.documentKey(m)='b' RETURN [n IN nodes(p)|n.id]",
        "MATCH REPEATABLE ELEMENTS p=(a)-[:CITES]-(m:Document)-[:CITES]-(b) WHERE antfly.documentKey(m)='c' RETURN [n IN nodes(p)|n.id]",
        "MATCH p=(a:Document)-[l:CITES]->(m:Document)-[r:CITES]->(n:Document)-[:CITES]->(z:Document) WHERE antfly.documentKey(n)='b' RETURN [n IN nodes(p)|n.id],l.weight,r.weight",
    };
    for (queries) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var expected = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer expected.deinit();
        const base = fixture.backend();
        var table = base.vtable.*;
        table.expand = Paged.expand;
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = base.ptr, .vtable = &table }, graph, compiled.query, &.{}, .{})).?;
        defer cursor.close();
        try std.testing.expectEqual(@as(?u64, @intCast(expected.rows.len)), try cursor.countRows());
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var position: usize = 0;
        while (!cursor.done) {
            const rows = try cursor.next(arena.allocator(), 1);
            for (rows) |row| {
                try std.testing.expect(position < expected.rows.len);
                for (row, expected.rows[position]) |entry, value| try std.testing.expect(v.Value.eql(entry.value, value));
                position += 1;
            }
            _ = arena.reset(.free_all);
        }
        try std.testing.expectEqual(expected.rows.len, position);
    }
}

test "ALL path count agrees with exact route enumeration for bounds and uniqueness" {
    const patterns = [_][]const u8{
        "(a:Document)-[:CITES*0..3]->(b:Document)",
        "(a:Document)-[:CITES]->(b:Document)-[:CITES]->(c:Document)",
        "(a:Document)-[r:CITES]->(b:Document)-[r:CITES]->(c:Document)",
        "(a:Document)-[:CITES*1..2]->(b:Document {rank:3})",
        "(a:Document)-[:CITES*0..1]->(b:Document)-[:CITES*0..2]->(c:Document)",
    };
    for ([_][]const u8{ "", "REPEATABLE ELEMENTS ", "DIFFERENT RELATIONSHIPS " }) |mode| {
        for (patterns) |pattern| {
            const text = try std.fmt.allocPrint(std.testing.allocator, "MATCH {s}{s} RETURN count(*)", .{ mode, pattern });
            defer std.testing.allocator.free(text);
            const exact_text = try std.fmt.allocPrint(std.testing.allocator, "MATCH {s}{s} UNWIND [1] AS x RETURN count(*)", .{ mode, pattern });
            defer std.testing.allocator.free(exact_text);
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
            defer compiled.deinit();
            var exact_compiled = try c.compile(std.testing.allocator, exact_text, .{}, &diagnostic);
            defer exact_compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            var exact = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &exact_compiled, &.{}, .{});
            defer exact.deinit();
            try std.testing.expectEqual(exact.rows[0][0].integer, result.rows[0][0].integer);
        }
    }
}

test "pull planning preserves correlated interior anchors" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (m:Document),(a:Document)-[:CITES]->(m)-[:CITES]->(z:Document) RETURN a.id,z.id", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const Paged = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, demand: usize) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, demand);
            const start = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(all.items.len, start + demand);
            return .{ .items = all.items[start..end], .next = if (end < all.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    const base = fixture.backend();
    var callbacks = base.vtable.*;
    callbacks.expand = Paged.expand;
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = base.ptr, .vtable = &callbacks }, graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var rows: usize = 0;
    while (!cursor.done) {
        rows += (try cursor.next(arena.allocator(), 1)).len;
        _ = arena.reset(.free_all);
    }
    try std.testing.expectEqual(@as(usize, 6), rows);
}

const cypher = @import("root.zig");
const hydration_graph: cypher.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "H", .table = "docs" }}, .relationships = &.{.{ .name = "R", .index = "links", .source_label = "H", .target_label = "H", .stored_type = "r" }} };
const hydration_source: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .properties = &.{.{ .name = "id", .value = .{ .string = "a" } }} };
const hydration_target: v.Node = .{ .identity = .{ .table = "docs", .key = "b" }, .properties = &.{.{ .name = "score", .value = .{ .integer = 1 } }} };
const HydrationFixture = struct {
    edges: []const *const v.Relationship,
    gets: usize = 0,
    batches: usize = 0,
    hidden: bool = false,
    fn cast(p: *anyopaque) *@This() {
        return @ptrCast(@alignCast(p));
    }
    fn bind(_: *anyopaque, _: std.mem.Allocator, _: cypher.catalog.Graph) !void {}
    fn checkpoint(_: *anyopaque) !void {}
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, after: ?[]const u8, _: usize) !b.Page(*const v.Node) {
        return .{ .items = if (after == null) &.{&hydration_source} else &.{} };
    }
    fn get(p: *anyopaque, _: std.mem.Allocator, id: v.Identity) !?*const v.Node {
        cast(p).gets += 1;
        return if (id.eql(hydration_target.identity)) &hydration_target else null;
    }
    fn many(p: *anyopaque, a: std.mem.Allocator, ids: []const v.Identity) ![]const ?*const v.Node {
        cast(p).batches += 1;
        const out = try a.alloc(?*const v.Node, ids.len);
        for (ids, out) |id, *item| item.* = if (!cast(p).hidden and id.eql(hydration_target.identity)) (try (v.Value{ .node = &hydration_target }).clone(a)).node else null;
        return out;
    }
    fn expand(p: *anyopaque, a: std.mem.Allocator, _: cypher.catalog.Relationship, id: v.Identity, _: cypher.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        if (!id.eql(hydration_source.identity)) return .{ .items = &.{} };
        const edges = cast(p).edges;
        const start = if (after) |s| try std.fmt.parseUnsigned(usize, s, 10) else 0;
        const end = @min(edges.len, start + limit);
        return .{ .items = edges[start..end], .next = if (end < edges.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
    }
    fn backend(self: *@This()) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .get_many = many, .expand = expand } };
    }
};
test "pull traversal batches repeated endpoints across suspended pages" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const edges = try a.alloc(*const v.Relationship, 1024);
    for (edges, 0..) |*item, i| {
        const edge = try a.create(v.Relationship);
        edge.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "e{d}", .{i}) }, .index = "links", .type = "R", .source = hydration_source.identity, .target = hydration_target.identity };
        item.* = edge;
    }
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var compiled = try cypher.compiler.compile(a, "MATCH (a:H {id:'a'})-[:R]->(b:H) RETURN b.score", .{}, &diagnostic);
    defer compiled.deinit();
    var native: HydrationFixture = .{ .edges = edges };
    var result = try cypher.runtime.execute(a, native.backend(), hydration_graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 8), native.batches);
    var sql: HydrationFixture = .{ .edges = edges };
    const cursor = (try cypher.runtime.NodeCursor.open(a, sql.backend(), hydration_graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    var page = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer page.deinit();
    var rows: usize = 0;
    while (!cursor.done) {
        rows += (try cursor.next(page.allocator(), 128)).len;
        _ = page.reset(.free_all);
    }
    try std.testing.expectEqual(@as(usize, 8), sql.batches);
    try std.testing.expectEqual(@as(usize, 0), sql.gets);
    try std.testing.expectEqual(@as(usize, 1024), rows);
    // SQL postimage overlays must forward batches and retain null alignment.
    const Host = struct {
        fn identity(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: bool) !v.Identity {
            return error.UnexpectedWrite;
        }
        fn commit(_: *anyopaque, _: std.mem.Allocator, _: []const b.Mutation) !void {
            return error.UnexpectedWrite;
        }
    };
    for ([_]bool{ false, true }) |hidden| {
        var fixture: HydrationFixture = .{ .edges = edges, .hidden = hidden };
        var overlay: cypher.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Host.identity, .commit = Host.commit }, .lazy = true };
        const backend = overlay.backend();
        try backend.vtable.bind(backend.ptr, a, hydration_graph);
        try backend.vtable.begin.?(backend.ptr);
        defer backend.vtable.rollback.?(backend.ptr);
        const pull = (try runtime.NodeCursor.open(a, backend, hydration_graph, compiled.query, &.{}, .{})).?;
        defer pull.close();
        var total: usize = 0;
        while (!pull.done) {
            total += (try pull.next(page.allocator(), 128)).len;
            _ = page.reset(.free_all);
        }
        try std.testing.expectEqual(@as(usize, if (hidden) 0 else 1024), total);
        try std.testing.expectEqual(@as(usize, 8), fixture.batches);
        try std.testing.expectEqual(@as(usize, 0), fixture.gets);
    }
}

test "foldable scalar predicates produce typed seeks without suppressing residual errors" {
    const Indexed = struct {
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, filters: []const b.PropertyFilter, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            try std.testing.expectEqual(@as(usize, 1), filters.len);
            try std.testing.expectEqualStrings("rank", filters[0].property);
            const value = filters[0].value;
            try std.testing.expect(value == .integer);
            try std.testing.expect(value.integer == -1 or value.integer == 1);
            return .{ .items = if (value.integer == 1) &.{&Fixture.nodes[0]} else &.{} };
        }
    };
    for ([_][]const u8{
        "MATCH (n:Document) WHERE n.rank=-1 RETURN n.rank",
        "MATCH (n:Document) WHERE n.rank=1-2 RETURN n.rank",
        "MATCH (n:Document) WHERE n.rank=-(-1) RETURN n.rank",
        "MATCH (n:Document {rank:$base-2}) RETURN n.rank",
    }, 0..) |text, index| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        const base = fixture.backend();
        var callbacks = base.vtable.*;
        callbacks.scan_where = Indexed.scan;
        const backend: b.Backend = .{ .ptr = base.ptr, .vtable = &callbacks };
        const parameters = &.{v.Entry{ .name = "base", .value = .{ .integer = 3 } }};
        var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, parameters, .{ .scanned_anchors = 1 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (index < 2) 0 else 1), result.rows.len);
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, parameters, .{ .scanned_anchors = 1 })).?;
        defer cursor.close();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        try std.testing.expectEqual(result.rows.len, (try cursor.next(arena.allocator(), 128)).len);
    }
    for ([_][]const u8{ "MATCH (n:Document) WHERE n.rank=1/0 RETURN n", "MATCH (n:Document) WHERE n.rank=9223372036854775807+1 RETURN n" }, [_]anyerror{ error.CypherDivisionByZero, error.CypherNumericOverflow }) |text, expected| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(expected, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "weighted endpoint reductions agree with exact cyclic and acyclic route bags" {
    const Acyclic = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var output: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |relationship| if (!std.mem.eql(u8, relationship.identity.key, "ba")) try output.append(a, relationship);
            return .{ .items = output.items };
        }
    };
    for ([_]bool{ false, true }) |acyclic| for ([_][]const u8{ "", "REPEATABLE ELEMENTS ", "DIFFERENT RELATIONSHIPS " }) |mode| for ([_][]const u8{
        "RETURN min(CASE WHEN b.rank>0 THEN b.rank ELSE 1/0 END)",
        "RETURN DISTINCT b{.rank, title:toLower(b.title)} ORDER BY b{.rank, title:toLower(b.title)}",
        "RETURN sum(reduce(total=0,x IN [b.rank,b.rank] | total+x))",
        "RETURN DISTINCT [a IN [b.rank] WHERE a>0 | {value:a}] AS value ORDER BY value",
        "RETURN min(size([x IN [b.rank,b.rank][0..1] | x]))",
        "RETURN min(CASE WHEN a.rank>0 THEN a.rank ELSE b.rank END)",
        "RETURN DISTINCT b.rank AS endpoint ORDER BY endpoint",
        "RETURN DISTINCT b.rank ORDER BY b.rank",
        "RETURN sum(a.rank),min(a.rank),max(a.rank)",
        "RETURN a.rank,count(*) ORDER BY a.rank",
        "RETURN count(b),count(b.missing),count(*)",
        "WHERE b.rank>=2 RETURN count(b)",
        "RETURN b.rank,count(b),count(b.missing),count(*) ORDER BY b.rank",
        "RETURN b.rank AS endpoint,count(*) AS total,sum(b.rank),min(b.rank),max(b.rank) ORDER BY endpoint",
        "RETURN count(DISTINCT b),sum(DISTINCT b.rank),min(DISTINCT b.rank),max(b.rank)",
        "RETURN sum(b.missing),min(b.missing),max(b.missing),avg(b.missing)",
        "WHERE a.rank<b.rank RETURN a.rank,b.rank,count(*) ORDER BY a.rank,b.rank",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const text = try std.fmt.allocPrint(a, "MATCH {s}(a:Document)-[:CITES*0..3]->(b:Document) {s}", .{ mode, tail });
        const split = std.mem.indexOf(u8, tail, "RETURN").?;
        const exact = try std.fmt.allocPrint(a, "MATCH {s}(a:Document)-[:CITES*0..3]->(b:Document) {s} WITH * ORDER BY b.rank {s}", .{ mode, tail[0..split], tail[split..] });
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var reference = try c.compile(a, exact, .{}, &diagnostic);
        defer reference.deinit();
        var fixture: Fixture = .{};
        const base = fixture.backend();
        var callbacks = base.vtable.*;
        if (acyclic) callbacks.expand = Acyclic.expand;
        const backend: b.Backend = .{ .ptr = base.ptr, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &reference, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected.rows, .{}), try std.json.Stringify.valueAlloc(a, result.rows, .{}));
    };
}

test "saturated path counts never become approximate SUM or AVG weights" {
    for ([_][]const u8{ "sum(1e-30)", "avg(1e-30)" }) |reduction| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN {s}", .{reduction});
        defer std.testing.allocator.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherStateBudgetExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 20000 }));
    }
    try check("MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN min(1),max(1),count(DISTINCT b)", "[[1,1,3]]");
    try check("MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN DISTINCT b.rank ORDER BY b.rank", "[[1],[2],[3]]");
}

test "presence cursors retain saturated endpoints while exact cursors keep the route fallback" {
    const Paged = struct {
        fn drain(cursor: *runtime.NodeCursor, a: std.mem.Allocator) !void {
            while (!cursor.done) _ = try cursor.next(a, 128);
        }
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            const offset = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(page.items.len, offset + limit);
            return .{ .items = page.items[offset..end], .next = if (end < page.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    for ([_]runtime.Multiplicity{ .exact, .presence }) |multiplicity| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, "MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN b.rank AS rank", .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        callbacks.expand = Paged.expand;
        const cursor = (try runtime.NodeCursor.open(a, .{ .ptr = &fixture, .vtable = &callbacks }, graph, compiled.query, &.{}, .{ .intermediate_states = 20000 })).?;
        defer cursor.close();
        try std.testing.expect(try cursor.enableWeights(multiplicity));
        if (multiplicity == .exact) {
            try std.testing.expectError(error.CypherStateBudgetExceeded, Paged.drain(cursor, a));
        } else {
            var seen = [_]bool{ false, false, false };
            while (!cursor.done) {
                const rows = try cursor.next(a, 128);
                for (rows, cursor.last_weights.items) |row, weight| {
                    try std.testing.expectEqual(@as(u64, 1), weight);
                    seen[@intCast(row[0].value.integer - 1)] = true;
                }
            }
            try std.testing.expectEqual([_]bool{ true, true, true }, seen);
        }
    }
}

test "mutation RPC scratch is bounded across large postimages and returns owned entities" {
    const rpc = @import("rpc.zig");
    const Channel = struct {
        fn call(_: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            try std.testing.expect(body.len > 8192);
            return a.dupe(u8,
                \\{"values":[{"type":"node","value":{"identity":{"table":"docs","key":"a"},"labels":["Document"],"properties":{"type":"map","value":{"title":{"type":"string","value":"Alpha"}}}}}]}
            );
        }
    };
    var state: u8 = 0;
    var client: rpc.Client = .{ .channel = .{ .ptr = &state, .call = Channel.call }, .incarnation = 1, .revision = 1 };
    var budget: @import("budget.zig") = .{ .backing = std.testing.allocator, .limit = 2 << 20 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    const payload: [8192]u8 = @splat('x');
    const node: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"}, .properties = &.{.{ .name = "payload", .value = .{ .string = &payload } }} };
    var retained: std.ArrayList(*const v.Node) = .empty;
    const backend = client.backend();
    for (0..200) |_| {
        try retained.append(a, try backend.vtable.normalize_node.?(backend.ptr, a, &node, true));
        try retained.append(a, (try backend.vtable.project_postimage.?(backend.ptr, a, &node)).?);
        try std.testing.expectEqual(@as(usize, 0), (try backend.vtable.document_relationships.?(backend.ptr, a, &node)).len);
    }
    for (retained.items) |owned| {
        try std.testing.expectEqualStrings("a", owned.identity.key);
        try std.testing.expectEqualStrings("Document", owned.labels[0]);
        try std.testing.expectEqualStrings("Alpha", (v.Value{ .node = owned }).property("title").string);
    }
    try std.testing.expect(budget.peak < 2 << 20);
}

test "mixed domains and unbounded DAG reductions preserve exact path bags" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |dag| for ([_][]const u8{
        "(a:Document)-[:CITES*0..2]->()-[:MENTIONS*0..1]->(b)",
        "(a:Document)-[:CITES|MENTIONS*0..1]->()-[:CITES*0..2]->(b)",
        "(a:Document)-[:CITES*0..1]->()<-[:CITES*0..1]-(b)",
        "(a:Document)-[:CITES*0..2]->()<-[:MENTIONS*0..1]-(b)",
        "(a:Document)-[:CITES*0..1]->()-[:CITES|MENTIONS*0..1]->()<-[:MENTIONS*0..1]-(b)",
        "(a:Document)-[:CITES*0..]->(b)",
        "(a:Document)-[:CITES*1..]->()-[:MENTIONS*0..]->(b)",
        "(a:Document)-[:CITES*2..]->(b)",
        "(a:Document)-[:CITES*3..]->(b)",
        "(a:Document)-[:CITES*2..]->()<-[:CITES*0..1]-(b)",
    }) |pattern| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        const base = fixture.backend();
        var callbacks = base.vtable.*;
        if (dag) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = base.ptr, .vtable = &callbacks };
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "MATCH {s} RETURN b.rank,count(*),sum(b.rank) ORDER BY b.rank", .{pattern}), .{}, &diagnostic);
        defer compiled.deinit();
        var exact = try c.compile(a, try std.fmt.allocPrint(a, "MATCH {s} WITH * ORDER BY b.rank RETURN b.rank,count(*),sum(b.rank) ORDER BY b.rank", .{pattern}), .{}, &diagnostic);
        defer exact.deinit();
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        if (!dag and std.mem.eql(u8, pattern, "(a:Document)-[:CITES|MENTIONS*0..1]->()-[:CITES*0..2]->(b)"))
            try std.testing.expect(result.stats.explored_edges <= 13);
        var reference = try runtime.execute(a, backend, graph, &exact, &.{}, .{});
        defer reference.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, reference.rows, .{}), try std.json.Stringify.valueAlloc(a, result.rows, .{}));
    };
}

test "weighted cursor negotiates endpoint bags and falls back for cyclic trails" {
    const Dag = struct {
        fn page(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize, acyclic: bool) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (all.items) |edge| if (!acyclic or !std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            const offset = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(out.items.len, offset + limit);
            return .{ .items = out.items[offset..end], .next = if (end < out.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            return page(ptr, a, mapping, identity, direction, after, limit, true);
        }
        fn cyclic(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            return page(ptr, a, mapping, identity, direction, after, limit, false);
        }
    };
    for ([_]bool{ false, true }) |dag| for ([_]usize{ 1, 128 }) |demand| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, "MATCH (a:Document)-[:CITES*0..3]->(b:Document) WHERE b.rank IN [1,2,3] RETURN b.rank AS r", .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        const base = fixture.backend();
        var callbacks = base.vtable.*;
        callbacks.expand = if (dag) Dag.expand else Dag.cyclic;
        const backend: b.Backend = .{ .ptr = base.ptr, .vtable = &callbacks };
        var reference = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer reference.deinit();
        const cursor = (try runtime.NodeCursor.open(a, backend, graph, compiled.query, &.{}, .{})).?;
        defer cursor.close();
        try std.testing.expect(try cursor.enableWeights(.exact));
        var weights = [_]u64{ 0, 0, 0 };
        var compressed = false;
        while (!cursor.done) {
            const rows = try cursor.next(a, demand);
            try std.testing.expectEqual(rows.len, cursor.last_weights.items.len);
            for (rows, cursor.last_weights.items) |row, weight| {
                weights[@intCast(row[0].value.integer - 1)] += weight;
                compressed = compressed or weight > 1;
            }
        }
        var expected = [_]u64{ 0, 0, 0 };
        for (reference.rows) |row| expected[@intCast(row[0].integer - 1)] += 1;
        try std.testing.expectEqualSlices(u64, &expected, &weights);
        if (dag) try std.testing.expect(compressed);
    };
}

test "bound pair reads and counts use seeks through owner RPC and pull cursors" {
    const rpc = @import("rpc.zig");
    const Pair = struct {
        fn broad(_: *anyopaque, _: std.mem.Allocator, _: catalog.Relationship, _: v.Identity, _: c.ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
            return error.UnexpectedAdjacencyScan;
        }
        fn between(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, peer: v.Identity, direction: c.ast.Direction, _: []const b.PropertyFilter, after: ?[]const u8, demand: usize, max_examined: usize) !b.Page(*const v.Relationship) {
            const all = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (all.items) |edge| if ((edge.source.eql(identity) and edge.target.eql(peer)) or (edge.target.eql(identity) and edge.source.eql(peer))) try out.append(a, edge);
            const offset = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(out.items.len, offset + demand);
            if (end - offset > max_examined) return error.CypherEdgeBudgetExceeded;
            return .{ .items = out.items[offset..end], .examined = end - offset, .next = if (end < out.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn call(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            const backend: *b.Backend = @ptrCast(@alignCast(ptr));
            return (rpc.Owner{ .backend = backend.*, .graph = graph, .table = "docs" }).execute(a, body);
        }
    };
    var fixture: Fixture = .{};
    const base = fixture.backend();
    var callbacks = base.vtable.*;
    callbacks.expand = Pair.broad;
    callbacks.expand_between = Pair.between;
    var backend: b.Backend = .{ .ptr = base.ptr, .vtable = &callbacks };
    var client: rpc.Client = .{ .channel = .{ .ptr = &backend, .call = Pair.call }, .incarnation = graph.incarnation, .revision = graph.revision };
    for ([_]b.Backend{ backend, client.backend() }) |source| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, "MATCH (a:Document {title:'Alpha'}),(b:Document {title:'Beta'}) MATCH (a)-[:CITES]->(b) RETURN count(*)", .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(a, source, graph, &compiled, &.{}, .{ .explored_edges = 2 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 2), result.stats.explored_edges);
        var pull = try c.compile(a, "MATCH (a:Document {title:'Alpha'}),(b:Document {title:'Beta'}),(a)-[:CITES]->(b) RETURN b.rank", .{}, &diagnostic);
        defer pull.deinit();
        const cursor = (try runtime.NodeCursor.open(a, source, graph, pull.query, &.{}, .{ .explored_edges = 2 })).?;
        defer cursor.close();
        var total: usize = 0;
        while (!cursor.done) total += (try cursor.next(a, 1)).len;
        try std.testing.expectEqual(@as(usize, 2), total);
    }
}

test "domain uniqueness distinguishes disjoint trails overlapping domains and node-simple paths" {
    const Domains = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            var physical = mapping;
            physical.name = "CITES";
            const page = try Fixture.expand(ptr, a, physical, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |value| {
                if (std.mem.eql(u8, value.identity.key, "ba")) continue;
                const copy = try a.create(v.Relationship);
                copy.* = value.*;
                copy.type = mapping.name;
                copy.identity.key = try std.fmt.allocPrint(a, "{s}:{s}", .{ mapping.name, value.identity.key });
                try out.append(a, copy);
            }
            return .{ .items = out.items };
        }
    };
    const domains: catalog.Graph = .{ .name = "domains", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs" }}, .relationships = &.{
        .{ .name = "CITES", .index = "links", .stored_type = "r", .source_label = "Document", .target_label = "Document" },
        .{ .name = "MENTIONS", .index = "links", .stored_type = "s", .source_label = "Document", .target_label = "Document" },
    } };
    for ([_][]const u8{ "CITES", "CITES|MENTIONS" }, [_][3]i64{ .{ 4, 0, 4 }, .{ 6, 0, 8 } }) |types, counts| {
        for ([_]c.ast.PathMode{ .relationship_simple, .node_simple, .repeatable }, counts) |mode, expected| {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            var diagnostic: c.Diagnostic = .{};
            const tail = if (std.mem.eql(u8, types, "CITES")) "MENTIONS" else "CITES";
            var compiled = try c.compile(a, try std.fmt.allocPrint(a, "MATCH (a:Document {{id:'a'}})-[:{s}]->()<-[:{s}]-(b:Document) RETURN count(*)", .{ types, tail }), .{}, &diagnostic);
            defer compiled.deinit();
            @constCast(compiled.query.clauses)[0].operation.match.mode = mode;
            var fixture: Fixture = .{};
            var callbacks = fixture.backend().vtable.*;
            callbacks.expand = Domains.expand;
            var result = try runtime.execute(a, .{ .ptr = &fixture, .vtable = &callbacks }, domains, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(expected, result.rows[0][0].integer);
        }
    }
}

test "DISTINCT ordering resolves projected expressions without exposing discarded values" {
    try check("MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY [n IN [{rank:0}]|n.rank] DESC,n.rank", "[[1],[2],[3]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank AS n ORDER BY [x IN [1]|n.rank] DESC", "[[3],[2],[1]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY reduce(n={rank:0},x IN [1]|n).rank DESC,n.rank", "[[1],[2],[3]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY n.rank DESC", "[[3],[2],[1]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY abs(n.rank-2),n.rank", "[[2],[1],[3]]");
    try check("MATCH (n:Document) RETURN DISTINCT n.rank AS n ORDER BY n.rank DESC", "[[3],[2],[1]]");
    try check("MATCH (n:Document) WITH DISTINCT n.rank ORDER BY n.rank DESC RETURN `n.rank`", "[[3],[2],[1]]");
    try check("UNWIND [3,1,2] AS n RETURN DISTINCT n ORDER BY n DESC", "[[3],[2],[1]]");
    try check("UNWIND [3,1,2] AS n RETURN DISTINCT * ORDER BY n DESC", "[[3],[2],[1]]");
    for ([_][]const u8{
        "MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY n.title",
        "MATCH (n:Document) RETURN DISTINCT n.rank+1 ORDER BY n.rank",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(error.UnboundCypherVariable, @import("binder.zig").validate(std.testing.allocator, graph, compiled.query));
    }
}

test "optional compressed bags preserve per-input null extension and dead binding effects" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |dag| for ([_][]const u8{
        "OPTIONAL MATCH (a:Document)-[:CITES*0..3]->(b:Document)",
        "OPTIONAL MATCH (a:Document)-[:CITES*1..3]->(b:Document)",
        "OPTIONAL MATCH (a:Document)-[:CITES*2..3]->(b:Document)",
        "MATCH (a:Document) OPTIONAL MATCH (a)-[:CITES*2..3]->(b:Document)",
        "OPTIONAL MATCH (a:Document)-[:CITES*1..3]->(b:Document) WHERE false",
        "OPTIONAL MATCH (a:Document {id:'absent'})-[:CITES*1..3]->(b:Document)",
    }) |prefix| for ([_][]const u8{
        "RETURN count(*),count(a),count(b),sum(a.rank),sum(b.rank),avg(b.rank),min(b.rank)",
        "WITH a,b RETURN count(*),sum(b.rank),min(b.rank)",
        "WITH a.rank AS dead,b RETURN count(*),sum(b.rank)",
        "WITH DISTINCT a,b RETURN count(*),sum(b.rank)",
        "WITH b AS a WITH a AS dead,a.rank AS score RETURN count(*),sum(score)",
        "WITH a,b WHERE b.rank>1 RETURN count(*),sum(b.rank)",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s} {s}", .{ prefix, tail }), .{}, &diagnostic);
        defer compiled.deinit();
        var exact = try c.compile(a, try std.fmt.allocPrint(a, "{s} WITH * ORDER BY a.rank,b.rank {s}", .{ prefix, tail }), .{}, &diagnostic);
        defer exact.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        if (dag) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &exact, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqual(expected.rows.len, result.rows.len);
        for (expected.rows, result.rows) |er, ar| for (er, ar) |ev, av| {
            if (ev == .number) try std.testing.expectApproxEqAbs(ev.number, av.number, 1e-12) else try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, ev, .{}), try std.json.Stringify.valueAlloc(a, av, .{}));
        };
    };
    for ([_][]const u8{
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WITH a,1/0 AS dead,b RETURN sum(b.rank)",
        "OPTIONAL MATCH (a:Document)-[:CITES*0..2]->(b:Document) WHERE false WITH 1/0 AS dead,b RETURN sum(b.rank)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "zero aggregate demand skips partition planning and graph work" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES*1..]->(b:Document) RETURN sum(a.rank),sum(b.rank) LIMIT 0", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 64, .scanned_anchors = 0, .explored_edges = 0 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
}

test "endpoint pipeline bags and aggregate partitions agree with ordered route execution" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |acyclic| for ([_][]const u8{ "", "REPEATABLE ELEMENTS " }) |mode| for ([_][]const u8{
        "WITH b RETURN count(*),sum(b.rank),avg(b.rank)",
        "WITH b.rank AS score RETURN count(*),sum(score),avg(score)",
        "WITH b AS a WITH a.rank AS b WHERE b>=2 RETURN count(*),sum(b)",
        "WITH * RETURN min(b.rank),max(b.rank)",
        "WITH DISTINCT b RETURN count(*),sum(b.rank),avg(b.rank)",
        "WITH DISTINCT * RETURN count(*),sum(b.rank)",
        "WITH b WHERE b.rank>1 RETURN count(*),sum(b.rank)",
        "UNWIND [b.rank,b.rank,null] AS score RETURN count(*),count(score),sum(score)",
        "UNWIND [b.rank,b.rank] AS score WITH DISTINCT score RETURN count(*),sum(score)",
        "WITH DISTINCT b UNWIND [b.rank,b.rank] AS score RETURN count(*),sum(score)",
        "WITH b RETURN b.rank,count(*) ORDER BY b.rank",
        "WITH b RETURN min(CASE WHEN b.rank>0 THEN b.rank ELSE 1/0 END)",
        "WITH b WHERE false RETURN count(*),sum(b.rank),avg(b.rank)",
        "RETURN sum(a.rank),sum(b.rank),count(*),avg(a.rank),avg(b.rank)",
        "RETURN sum(a.missing),sum(b.missing),count(a.missing),count(b.missing)",
        "RETURN sum(a.rank)+sum(b.rank) AS total ORDER BY total",
        "RETURN count(DISTINCT a),sum(DISTINCT b.rank),min(a.rank),max(b.rank)",
        "RETURN sum(a.rank+b.rank)",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const prefix = try std.fmt.allocPrint(a, "MATCH {s}p=(a:Document)-[:CITES*0..3]->(b:Document) ", .{mode});
        const text = try std.fmt.allocPrint(a, "{s}{s}", .{ prefix, tail });
        // ORDER BY is an intentional materialization barrier, so this remains
        // an independent exact route oracle after UNWIND becomes compressible.
        const reference = try std.fmt.allocPrint(a, "{s}WITH * ORDER BY a.rank,b.rank {s}", .{ prefix, tail });
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var exact = try c.compile(a, reference, .{}, &diagnostic);
        defer exact.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        if (acyclic) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &exact, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqual(expected.rows.len, result.rows.len);
        for (expected.rows, result.rows) |expected_row, actual_row| {
            for (expected_row, actual_row) |expected_value, actual_value| {
                if (expected_value == .number) {
                    try std.testing.expectApproxEqAbs(expected_value.number, actual_value.number, 1e-12);
                } else try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected_value, .{}), try std.json.Stringify.valueAlloc(a, actual_value, .{}));
            }
        }
    };
}

test "multi-root DAG reductions share physical pages and preserve zero-hop bags" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var output: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |relationship| if (!std.mem.eql(u8, relationship.identity.key, "ba")) try output.append(a, relationship);
            return .{ .items = output.items };
        }
    };
    for ([_][]const u8{
        "(a:Document)-[:CITES*0..]->(b:Document)",
        "(a:Document)-[:CITES*1..]->(b:Document)",
        "(a:Document)-[:CITES*2..]->(b:Document)",
        "(a:Document)-[:CITES*3..]->(b:Document)",
        "(a:Document)-[:CITES*2..3]->(b:Document)",
        "(a:Document)-[:CITES*0..3]->(b:Document)",
        "(a:Document)-[:CITES*1..3]->(b:Document)",
        "(a:Document {id:'a'})-[:CITES*0..3]->(b:Document)",
        "(a:Document {id:'a'})-[:CITES*1..3]->(b:Document)",
        "(a:Document {id:'a'})-[:CITES*2..3]->(b:Document)",
        "(a:Document {id:'a'})-[:CITES*1..1]->(b:Document)",
        "(a:Document)-[:CITES]->(middle:Document)-[:CITES]->(b:Document)",
        "(a:Document)-[:CITES*0..1]->(middle:Document)-[:CITES*0..1]->(b:Document)",
    }) |pattern| for ([_][]const u8{
        "RETURN count(*),sum(b.rank),avg(b.rank),min(b.rank),max(b.rank)",
        "RETURN a.rank,b.rank,count(*) ORDER BY a.rank,b.rank",
        "RETURN a.rank,count(*) ORDER BY a.rank",
        "RETURN sum(a.rank)",
        "WHERE b.rank>=2 RETURN count(*),sum(b.rank)",
        "RETURN DISTINCT b{.rank} AS node ORDER BY node",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const split = std.mem.indexOf(u8, tail, "RETURN").?;
        const text = try std.fmt.allocPrint(a, "MATCH {s} {s}", .{ pattern, tail });
        const exact = try std.fmt.allocPrint(a, "MATCH {s} {s} WITH * ORDER BY b.rank {s}", .{ pattern, tail[0..split], tail[split..] });
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var reference = try c.compile(a, exact, .{}, &diagnostic);
        defer reference.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{ .explored_edges = 3 });
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &reference, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqual(expected.rows.len, result.rows.len);
        for (expected.rows, result.rows) |expected_row, actual_row| {
            try std.testing.expectEqual(expected_row.len, actual_row.len);
            for (expected_row, actual_row) |expected_cell, actual_cell| {
                if (expected_cell == .number) {
                    try std.testing.expectApproxEqAbs(expected_cell.number, actual_cell.number, 1e-12);
                } else try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected_cell, .{}), try std.json.Stringify.valueAlloc(a, actual_cell, .{}));
            }
        }
        try std.testing.expect(result.stats.explored_edges <= 3);
    };
}

test "soft graph cache and anchor admission failure retain exact fallback" {
    const Large = struct {
        const payload: [100 << 10]u8 = @splat('x');
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            var page = try Fixture.scan(ptr, a, table, after, limit);
            const nodes = try a.alloc(*const v.Node, page.items.len);
            for (page.items, nodes) |node, *out| {
                // Retain the first root, then fail admission on the next one.
                // Flushing that prefix must neither lose nor repeat its paths.
                if (std.mem.eql(u8, node.identity.key, "a")) {
                    out.* = node;
                    continue;
                }
                const owned = try a.create(v.Node);
                owned.* = node.*;
                const properties = try a.alloc(v.Entry, node.properties.len + 1);
                @memcpy(properties[0..node.properties.len], node.properties);
                properties[node.properties.len] = .{ .name = "payload", .value = .{ .string = &payload } };
                owned.properties = properties;
                out.* = owned;
            }
            page.items = nodes;
            return page;
        }
        fn scanAll(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const result = try scan(ptr, a, table, after, limit);
            const nodes = try a.dupe(*const v.Node, result.items);
            for (nodes) |*node| if (std.mem.eql(u8, node.*.identity.key, "a")) {
                const owned = try a.create(v.Node);
                owned.* = node.*.*;
                const properties = try a.alloc(v.Entry, owned.properties.len + 1);
                @memcpy(properties[0..owned.properties.len], owned.properties);
                properties[owned.properties.len] = .{ .name = "payload", .value = .{ .string = &payload } };
                owned.properties = properties;
                node.* = owned;
            };
            return .{ .items = nodes, .next = result.next, .examined = result.examined, .unfiltered = result.unfiltered };
        }
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var output: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |relationship| {
                if (std.mem.eql(u8, relationship.identity.key, "ba")) continue;
                const owned = try a.create(v.Relationship);
                owned.* = relationship.*;
                owned.properties = &.{.{ .name = "payload", .value = .{ .string = &payload } }};
                try output.append(a, owned);
            }
            return .{ .items = output.items };
        }
    };
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.scan = Large.scan;
    callbacks.expand = Large.expand;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES*0..]->(b:Document) RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 8), result.rows[0][0].integer);
    // Cursor seed admission must also flush an already-retained prefix when
    // the next full record exceeds its soft allowance. Scan pages stay narrow.
    var projected = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES*0..]->(b:Document) RETURN b.rank", .{}, &diagnostic);
    defer projected.deinit();
    for ([_]bool{ false, true }) |first_root_large| {
        var cursor_callbacks = callbacks;
        if (first_root_large) cursor_callbacks.scan = Large.scanAll;
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = &fixture, .vtable = &cursor_callbacks }, graph, projected.query, &.{}, .{ .memory_bytes = 1 << 20 })).?;
        defer cursor.close();
        try std.testing.expect(try cursor.enableWeights(.exact));
        var weights = [_]u64{ 0, 0, 0 };
        while (!cursor.done) {
            var page = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer page.deinit();
            const rows = try cursor.next(page.allocator(), 1);
            for (rows, cursor.last_weights.items) |row, weight| weights[@intCast(row[0].value.integer - 1)] += weight;
        }
        try std.testing.expectEqualSlices(u64, &.{ 1, 3, 4 }, &weights);
    }
}

test "consumer orientation preserves anchors with unknown relative populations" {
    const Guard = struct {
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            if (std.mem.eql(u8, table, "people")) return error.UnexpectedPopulationScan;
            return Fixture.scan(ptr, a, table, after, limit);
        }
    };
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.scan = Guard.scan;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:MENTIONS*1..]->(b:Person) RETURN sum(a.rank)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
}
