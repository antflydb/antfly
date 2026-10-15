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
        return .{ .items = try output.toOwnedSlice(a), .next = next };
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
