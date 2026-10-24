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
    var result = runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}) catch |err| {
        std.debug.print("query failed ({s}): {s}\n", .{ @errorName(err), text });
        return err;
    };
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

test "finite document-key sets use distinct authorized point reads" {
    for ([_][]const u8{
        "antfly.documentKey(n) IN ['b','a','b',null,42,'missing']",
        "antfly.documentKey(n)='b' OR antfly.documentKey(n) IN ['a','missing']",
        "antfly.documentKey(n) IN $keys",
        "antfly.documentKey(n) IN $keys AND n.rank>0",
    }) |predicate| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "MATCH (n:Document) WHERE {s} RETURN n.id ORDER BY n.id", .{predicate});
        defer std.testing.allocator.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        for ([_]bool{ false, true }) |denied| {
            var fixture: Fixture = .{ .deny_b = denied };
            var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "keys", .value = .{ .list = &.{ .{ .string = "b" }, .{ .string = "a" }, .{ .string = "b" }, .null, .{ .integer = 42 }, .{ .string = "missing" } } } }}, .{ .scanned_anchors = 3 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 3), result.stats.scanned_anchors);
            try std.testing.expectEqual(@as(usize, if (denied) 1 else 2), result.rows.len);
            try std.testing.expectEqualStrings("a", result.rows[0][0].string);
        }
    }
    try check("MATCH (n) WHERE antfly.documentKey(n) IN ['a','a'] RETURN count(*)", "[[2]]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n) IN [] RETURN count(*)", "[[0]]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n) IN null RETURN count(*)", "[[0]]");
    try check("MATCH (n:Document) WHERE antfly.documentKey(n) IN ['missing'] OR n.id='a' RETURN n.id", "[[\"a\"]]");
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

test "row dependent string predicates pay each scan while immutable Unicode functions reuse analysis" {
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
        "RETURN reduce(a=0,i IN range(1,1000)|a+CASE WHEN $s CONTAINS toString(i) THEN 1 ELSE 0 END)",
        "RETURN reduce(a=0,i IN range(1,1000)|a+CASE WHEN ($s+toString(i)) STARTS WITH $s THEN 1 ELSE 0 END)",
        "RETURN reduce(a=0,i IN range(1,1000)|a+size(substring($s,i)))",
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
            try std.testing.expect(identities.len <= 128);
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
    var keys_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer keys_arena.deinit();
    var keys: [130]v.Value = undefined;
    for (&keys, 0..) |*key, index| key.* = .{ .string = if (index < 3) ([_][]const u8{ "a", "b", "c" })[index] else try std.fmt.allocPrint(keys_arena.allocator(), "missing-{d}", .{index}) };
    var key_query = try c.compile(std.testing.allocator, "MATCH (n:Document) WHERE antfly.documentKey(n) IN $keys RETURN count(*)", .{}, &diagnostic);
    defer key_query.deinit();
    const parameters = &.{v.Entry{ .name = "keys", .value = .{ .list = &keys } }};
    fixture.calls = 0;
    var key_result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &table }, graph, &key_query, parameters, .{ .scanned_anchors = 130 });
    defer key_result.deinit();
    try std.testing.expectEqual(@as(i64, 3), key_result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 130), key_result.stats.scanned_anchors);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
    fixture.calls = 0;
    try std.testing.expectError(error.CypherAnchorBudgetExceeded, runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &table }, graph, &key_query, parameters, .{ .scanned_anchors = 0 }));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
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

test "promoted path counts preserve exact SUM and normalized AVG weights" {
    for ([_][]const u8{ "sum(1e-30)", "avg(1e-30)" }) |reduction| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN {s}", .{reduction});
        defer std.testing.allocator.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 20000 });
        defer result.deinit();
        // Independent adjacency-matrix oracle: three undirected parallel a/b
        // edges and one b/c edge, starting once from each document.
        var counts = [_]u128{ 1, 1, 1 };
        for (0..63) |_| {
            const next = [_]u128{ counts[1] * 3, counts[0] * 3 + counts[2], counts[1] };
            counts = next;
        }
        const total: f128 = @floatFromInt(counts[0] + counts[1] + counts[2]);
        const expected: f64 = if (std.mem.eql(u8, reduction, "sum(1e-30)")) @floatCast(total * 1e-30) else 1e-30;
        try std.testing.expectApproxEqRel(expected, result.rows[0][0].number, 1e-14);
    }
    try check("MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN min(1),max(1),count(DISTINCT b)", "[[1,1,3]]");
    try check("MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN size(collect(DISTINCT b.rank))", "[[3]]");
    try check("MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*63..63]-(b:Document) RETURN DISTINCT b.rank ORDER BY b.rank", "[[1],[2],[3]]");
}

test "presence cursors cap endpoints while exact cursors promote producer counts" {
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
    for ([_]runtime.Multiplicity{ .exact, .presence, .capped }) |multiplicity| {
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
            var promoted = false;
            while (!cursor.done) {
                const rows = try cursor.next(a, 128);
                try std.testing.expectEqual(rows.len, cursor.last_counts.items.len);
                for (cursor.last_counts.items) |count| promoted = promoted or count.exact() == null;
            }
            try std.testing.expect(promoted);
        } else {
            var seen = [_]bool{ false, false, false };
            while (!cursor.done) {
                const rows = try cursor.next(a, 128);
                for (rows, cursor.last_weights.items) |row, weight| {
                    if (multiplicity == .presence) try std.testing.expectEqual(@as(u64, 1), weight) else try std.testing.expect(weight > 0 and weight <= @as(u64, std.math.maxInt(i64)) + 1);
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

test "cursor planning allocations retain their moved arena and release estimates" {
    const Estimates = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            const first = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(page.items.len, first + limit);
            return .{ .items = page.items[first..end], .next = if (end < page.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn estimate(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const b.PropertyFilter, limit: usize) !?usize {
            return if (limit >= 1) @as(usize, 1) else null;
        }
    };
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.estimate_where = Estimates.estimate;
    callbacks.expand = Estimates.expand;
    const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WHERE b.rank>=2 RETURN b.rank", .{}, &diagnostic);
    defer compiled.deinit();
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, backend, graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    try std.testing.expect(try cursor.enableWeights(.exact));
    var total: u64 = 0;
    while (true) {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const rows = try cursor.next(arena.allocator(), 1);
        if (rows.len == 0) break;
        for (cursor.last_weights.items) |weight| total += weight;
    }
    var exact = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{});
    defer exact.deinit();
    try std.testing.expectEqual(exact.rows.len, total);
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
        try std.testing.expect(result.stages.len != 0);
        try std.testing.expectEqualStrings("match", result.stages[0].operation);
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

test "compressed admission and ordered bags agree with exact trails and preserve eager errors" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |dag| for ([_][]const u8{ "MATCH", "OPTIONAL MATCH" }) |verb| for ([_][]const u8{
        "",                              " WHERE a.rank>=0",              " WHERE b.rank>=0",            " WHERE a.rank<0",                                    " WHERE a.id IS NOT NULL",
        " WHERE a.rank>1 AND b.rank>=2", " WHERE b.rank>=2 AND a.rank>1", " WHERE a.rank<2 OR b.rank>2", " WHERE EXISTS { MATCH (b)-[:CITES]->(d:Document) }", " WHERE NOT EXISTS { MATCH (b)-[:CITES]->(d:Document) }",
    }) |predicate| for ([_][]const u8{
        "WITH b WHERE EXISTS { MATCH (b)-[:CITES]->(d:Document) } RETURN count(*),sum(b.rank)",
        "WITH b,[(b)-[:CITES]->(d:Document) | d.rank] AS xs UNWIND xs AS x RETURN count(*),sum(x)",
        "WITH b ORDER BY b.rank LIMIT 3 CALL { WITH b MATCH (b)-[:CITES]->(d:Document) RETURN d } RETURN count(*),sum(d.rank)",
        "WITH b ORDER BY b.rank LIMIT 3 CALL { WITH b OPTIONAL MATCH (b)-[:CITES]->(d:Document) RETURN d } RETURN count(*),count(d)",
        "WITH b ORDER BY b.rank LIMIT 3 CALL { WITH b MATCH (b)-[:CITES]->(d:Document) } RETURN count(*)",
        "WITH b ORDER BY b.rank LIMIT 3 MATCH (b)<-[:CITES*0..3]-(d:Document) RETURN min(d.rank)+max(d.rank),sum(DISTINCT d.rank)",
        "WITH b ORDER BY b.rank RETURN min(b.rank)+max(b.rank)",
        "WITH b ORDER BY b.rank RETURN count(DISTINCT b)+1",
        "WITH b MATCH (b)-[:CITES*0..2]->(d:Document) WITH d AS b MATCH (b)-[:CITES*0..2]->(e:Document) RETURN count(*),min(e.rank),count(DISTINCT e)",
        "RETURN sum(size([(b)-[:CITES*0..2]->(d:Document) | d.rank]))",
        "RETURN sum(size([p=(b)-[:CITES*0..2]->(d:Document) | length(p)]))",
        "CALL { WITH b MATCH (b)-[:CITES]->(d:Document) RETURN d } RETURN count(*),sum(d.rank)",
        "RETURN count(*),sum(b.rank),min(a.rank)",
        "WITH b ORDER BY b.rank DESC RETURN count(*),min(b.rank),max(b.rank)",
        "WITH b ORDER BY b.rank SKIP 1 LIMIT 3 RETURN b.rank ORDER BY b.rank",
        "WITH b WHERE b.rank>0 WITH b ORDER BY b.rank DESC SKIP 2 LIMIT 4 RETURN count(*),sum(b.rank)",
        "WITH b.rank AS score ORDER BY score DESC LIMIT 4 RETURN score",
        "WITH b ORDER BY b.missing,b.rank DESC LIMIT 4 RETURN b.rank",
        "WITH b ORDER BY b.rank LIMIT 0 RETURN count(*)",
        "WITH b ORDER BY b.rank LIMIT 3 MATCH (b)-[:CITES]->(d:Document) RETURN count(*),sum(d.rank)",
        "WITH b ORDER BY b.rank LIMIT 3 OPTIONAL MATCH (b)-[:CITES]->(d:Document) RETURN count(*),count(d)",
        "WITH b ORDER BY b.rank LIMIT 3 WITH DISTINCT * RETURN count(*),sum(b.rank)",
        "WITH b ORDER BY b.rank LIMIT 3 MATCH (b)<-[:CITES*0..3]-(d:Document) RETURN count(*),sum(d.rank)",
        "WITH b ORDER BY b.rank LIMIT 3 OPTIONAL MATCH (b)<-[:CITES*0..3]-(d:Document) WHERE d.rank<0 RETURN count(*),count(d)",
        "WITH b ORDER BY b.rank LIMIT 3 MATCH p=(b)<-[:CITES*0..3]-(d:Document) RETURN count(*),sum(length(p))",
        "WITH b ORDER BY b.rank LIMIT 3 MATCH (b)<-[:CITES*0..3]-(d:Document) WITH DISTINCT * RETURN count(*)",
        "WITH b ORDER BY b.rank LIMIT 3 MATCH (b)<-[:CITES*0..3]-(d:Document) WITH d ORDER BY d.rank SKIP 1 WITH DISTINCT d RETURN count(*)",
        "WITH b ORDER BY b.rank LIMIT 3 WITH b WHERE EXISTS { MATCH (b)-[:CITES]->(d:Document) } RETURN count(*),sum(b.rank)",
        "WITH b ORDER BY b.rank LIMIT 3 RETURN count(EXISTS { MATCH (b)-[:CITES]->(d:Document) RETURN count(*) })",
        "WITH b ORDER BY b.rank LIMIT 3 WITH b,[(b)-[:CITES]->(d:Document) | d.rank] AS xs UNWIND xs AS x RETURN count(*),sum(x)",
        "WITH b ORDER BY b.rank LIMIT 3 RETURN [(b)-[:CITES]->(d:Document) | d.rank]",
        "WITH b ORDER BY b.rank LIMIT 3 UNWIND [(b)-[:CITES]->(d:Document) | d.rank] AS x RETURN count(*),sum(x)",
        "WITH b ORDER BY b.rank LIMIT 3 RETURN EXISTS { MATCH (b)-[:CITES]->(d:Document) }",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        const prefix = try std.fmt.allocPrint(a, "{s} (a:Document)-[:CITES*0..3]->(b:Document){s}", .{ verb, predicate });
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s} {s}", .{ prefix, tail }), .{}, &diagnostic);
        defer compiled.deinit();
        // SKIP 0 is an explicit selection barrier, including when a later
        // order-insensitive consumer makes a pure sort unobservable.
        var exact = try c.compile(a, try std.fmt.allocPrint(a, "{s} WITH * ORDER BY a.rank,b.rank SKIP 0 {s}", .{ prefix, tail }), .{}, &diagnostic);
        defer exact.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        if (dag) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &exact, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected.rows, .{}), try std.json.Stringify.valueAlloc(a, result.rows, .{}));
    };
    try check("OPTIONAL MATCH (a:Document)-[:CITES*1..2]->(b:Document) WHERE EXISTS { MATCH (b)-[:CITES]->(d:Document) WHERE false } RETURN count(*),count(b)", "[[1,0]]");
    for ([_][]const u8{
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WHERE a.rank<0 AND 1/0=1 RETURN count(*)",
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WHERE EXISTS { MATCH (b)-[:CITES]->(d:Document) WHERE false } AND 1/0=1 RETURN count(*)",
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WHERE 1/0=1 AND a.rank<0 RETURN count(*)",
        "MATCH (a:Document)-[:CITES*1..2 {weight:1/0}]->(b:Document) WHERE a.rank<0 RETURN count(*)",
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WITH b ORDER BY 1/0 RETURN count(*)",
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WITH b,1/0 AS dead ORDER BY b.rank RETURN count(*)",
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WITH b ORDER BY b.rank LIMIT 3 WITH b WHERE EXISTS { MATCH (b)-[:CITES]->(d:Document) WHERE false } AND 1/0=1 RETURN count(*)",
        "MATCH (a:Document)-[:CITES*0..2]->(b:Document) WITH b ORDER BY b.rank LIMIT 3 MATCH (b)-[:CITES*0..2]->(d:Document) WHERE b.rank<0 AND 1/0=1 RETURN count(*)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "middle sort keeps property errors across heterogeneous outer inputs" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [{rank:1},0] AS x MATCH (a:Document)-[:CITES*0..2]->(b:Document) WITH b ORDER BY x.rank RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "connected components preserve exact clause boundaries labels and cyclic fallback" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |dag| for ([_][]const u8{
        "MATCH (a:Document)-[:CITES*0..2]->(m:Document),(m)-[:CITES*0..2]->(b:Document)",
        "MATCH (m:Document)-[:CITES*0..2]->(b:Document),(a:Document)-[:CITES*0..2]->(m)",
        "MATCH (a:Document)-[:CITES*0..2]->(m:Document) MATCH (m)-[:CITES*0..2]->(b:Document)",
        "MATCH (a:Document)-[:CITES*0..2]->(m:Document) MATCH (m:Document {rank:2})-[:CITES*0..2]->(b:Document)",
        "MATCH (a:Document)-[:CITES*0..2]->(m:Document) MATCH (m)-[:CITES*0..2]->(b:Document) WHERE b.rank>=2",
        "OPTIONAL MATCH (a:Document)-[:CITES*0..2]->(m:Document),(m)-[:CITES*0..2]->(b:Document) WHERE a.rank<0",
    }) |prefix| for ([_][]const u8{
        "RETURN count(*),sum(b.rank),min(a.rank)",
        "RETURN a.rank,b.rank,count(*) ORDER BY a.rank,b.rank",
        "WITH b ORDER BY b.rank DESC SKIP 1 LIMIT 3 RETURN count(*),sum(b.rank)",
        "RETURN count(DISTINCT b),min(b.rank),max(b.rank)",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s} {s}", .{ prefix, tail }), .{}, &diagnostic);
        defer compiled.deinit();
        // A selection barrier separates MATCH clauses and blocks component
        // projection, providing the original history-preserving interpreter.
        const separated = try std.mem.replaceOwned(u8, a, prefix, " MATCH (m", " WITH * ORDER BY a.rank SKIP 0 MATCH (m");
        var exact = try c.compile(a, try std.fmt.allocPrint(a, "{s} WITH * ORDER BY a.rank SKIP 0 {s}", .{ separated, tail }), .{}, &diagnostic);
        defer exact.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        if (dag) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &exact, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected.rows, .{}), try std.json.Stringify.valueAlloc(a, result.rows, .{}));
    };
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
        "RETURN count(length(p)),sum(length(p)),avg(length(p)),min(length(p)),max(length(p))",
        "RETURN b.rank,min(length(p)),max(length(p)),sum(length(p)),avg(length(p)) ORDER BY b.rank",
        "WITH b,length(p) AS n RETURN b.rank,min(n),max(n),sum(n),avg(n) ORDER BY b.rank",
        "WITH b,length(p) AS n RETURN b{n},min(n) ORDER BY b{n}",
        "RETURN sum(length(p))+sum(b.rank),min(DISTINCT length(p)),max(DISTINCT length(p))",
        "RETURN sum(DISTINCT length(p)),avg(DISTINCT length(p)),count(DISTINCT length(p))",
        "WHERE length(p)>1 RETURN sum(length(p)),min(length(p))",
        "WITH length(p) AS n RETURN n,count(*) ORDER BY n",
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

test "branch factorization preserves parallel edges cyclic histories and leaf constraints" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |dag| for ([_][]const u8{ "", "REPEATABLE ELEMENTS " }) |mode| for ([_][]const u8{
        "(a:Document)-[:CITES*1..2]->(m:Document),(m)-[:CITES*1..2]->(b:Document),(m)-[:CITES]->(c:Document)",
        "(a:Document)-[:CITES]->(m:Document)-[:CITES]->(b:Document),(a)-[:CITES]->(c:Document)",
        "(a:Document)-[:CITES*1..2]->(m:Document),(m)-[:CITES*1..2]->(b:Document),(a)-[:CITES {weight:2}]->(c:Document {rank:2})",
        "(a:Document)-[:CITES*1..2]->(m:Document),(m)-[:CITES*1..2]->(b:Document),(a)-[:CITES]->(c:Document),(a)-[:CITES]->(d:Document)",
        "(b:Document)<-[:CITES*1..2]-(m:Document),(m)<-[:CITES*1..2]-(a:Document),(m)<-[:CITES]-(c:Document)",
    }) |pattern| for ([_][]const u8{
        "RETURN count(*),sum(b.rank),min(a.rank)",
        "RETURN b.rank,count(*) ORDER BY b.rank",
        "RETURN count(DISTINCT b),sum(DISTINCT b.rank)",
        "RETURN count(*),sum(c.rank)",
    }) |tail| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "MATCH {s}{s} {s}", .{ mode, pattern, tail }), .{}, &diagnostic);
        defer compiled.deinit();
        var exact = try c.compile(a, try std.fmt.allocPrint(a, "MATCH {s}{s} WITH * ORDER BY a.rank SKIP 0 {s}", .{ mode, pattern, tail }), .{}, &diagnostic);
        defer exact.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        if (dag) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected = try runtime.execute(a, backend, graph, &exact, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected.rows, .{}), try std.json.Stringify.valueAlloc(a, result.rows, .{}));
    };
}

test "connected SQL cursor bags preserve original MATCH histories" {
    const Paged = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, null, 128);
            const first = if (after) |text| try std.fmt.parseUnsigned(usize, text, 10) else 0;
            const end = @min(page.items.len, first + limit);
            return .{ .items = page.items[first..end], .next = if (end < page.items.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "MATCH (a:Document)-[:CITES*1..2]->(m:Document),(m)-[:CITES*1..2]->(b:Document)",
        "MATCH (m:Document)-[:CITES*1..2]->(b:Document),(a:Document)-[:CITES*1..2]->(m)",
    }) |prefix| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s} RETURN b.rank AS rank", .{prefix}), .{}, &diagnostic);
        defer compiled.deinit();
        var exact = try c.compile(a, try std.fmt.allocPrint(a, "{s} WITH * ORDER BY a.rank SKIP 0 RETURN b.rank AS rank", .{prefix}), .{}, &diagnostic);
        defer exact.deinit();
        var fixture: Fixture = .{};
        var expected = try runtime.execute(a, fixture.backend(), graph, &exact, &.{}, .{});
        defer expected.deinit();
        var counts = [_]u64{ 0, 0, 0 };
        for (expected.rows) |row| counts[@intCast(row[0].integer - 1)] += 1;
        var callbacks = fixture.backend().vtable.*;
        callbacks.expand = Paged.expand;
        const clauses = try a.dupe(c.ast.Clause, compiled.query.clauses);
        clauses[0].operation.match.mode = .node_simple;
        var simple_query = compiled.query;
        simple_query.clauses = clauses;
        const simple = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, simple_query, &.{}, .{})).?;
        defer simple.close();
        try std.testing.expect(!try simple.enableWeights(.exact));
        const shallow = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, compiled.query, &.{}, .{ .path_depth = 2 })).?;
        defer shallow.close();
        try std.testing.expect(!try shallow.enableWeights(.exact));
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, compiled.query, &.{}, .{})).?;
        defer cursor.close();
        try std.testing.expect(try cursor.enableWeights(.exact));
        var actual = [_]u64{ 0, 0, 0 };
        while (!cursor.done) {
            var page = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer page.deinit();
            const rows = try cursor.next(page.allocator(), 1);
            for (rows, cursor.last_weights.items) |row, weight| actual[@intCast(row[0].value.integer - 1)] += weight;
        }
        try std.testing.expectEqualSlices(u64, &counts, &actual);
    }
}

test "branch factorization preserves externally bound relationship aliases" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diagnostic: c.Diagnostic = .{};
    const prefix = "MATCH (x:Document)-[r:CITES]->(y:Document) WITH r MATCH REPEATABLE ELEMENTS (a:Document)-[:CITES*1..2]->(m:Document),(m)-[:CITES*1..2]->(b:Document),(a)-[r:CITES]->(c:Document)";
    var compiled = try c.compile(a, prefix ++ " RETURN count(*),sum(b.rank)", .{}, &diagnostic);
    defer compiled.deinit();
    var exact = try c.compile(a, prefix ++ " WITH * ORDER BY a.rank SKIP 0 RETURN count(*),sum(b.rank)", .{}, &diagnostic);
    defer exact.deinit();
    var fixture: Fixture = .{};
    var actual = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
    defer actual.deinit();
    var expected = try runtime.execute(a, fixture.backend(), graph, &exact, &.{}, .{});
    defer expected.deinit();
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, expected.rows, .{}), try std.json.Stringify.valueAlloc(a, actual.rows, .{}));
}

test "weighted continuation distinguishes an empty binding from an empty input" {
    try check("MATCH (:Document) WITH * LIMIT 5 WITH DISTINCT * RETURN count(*)", "[[1]]");
    try check("MATCH (:Document) WITH * LIMIT 0 WITH DISTINCT * RETURN count(*)", "[[0]]");
    try check("MATCH (n:Document) WITH n LIMIT 5 MATCH (n)-[:CITES]->(m:Document) WITH DISTINCT * RETURN count(*)", "[[3]]");
}

test "MATCH continuations preserve rejected prefixes optional rows distinct and global windows" {
    // The first producer binding need not survive the next MATCH. A row goal
    // on that producer would return no result instead of resuming it.
    try check("MATCH (a:Document) WITH a MATCH (b:Document) WHERE a.rank=b.rank AND b.rank=3 RETURN a.rank,b.rank LIMIT 1", "[[3,3]]");
    try check("MATCH (a:Document) WITH a OPTIONAL MATCH (b:Document) WHERE a.rank=b.rank AND b.rank=3 WITH a,b WHERE b IS NULL RETURN a.rank LIMIT 2", "[[1],[2]]");
    try check("MATCH (a:Document) WITH a MATCH (b:Document) WHERE a.rank=b.rank RETURN b.rank SKIP 1 LIMIT 1", "[[2]]");
    try check("MATCH (a:Document) WITH a MATCH (b:Document) WITH DISTINCT b.rank AS k RETURN k LIMIT 2", "[[1],[2]]");
    try check("UNWIND [1,2,1] AS k CALL { WITH k RETURN k AS n } RETURN collect(n)", "[[[1,2,1]]]");
    try check("MATCH (a:Document) CALL { MATCH (a)-[:CITES]->(b) RETURN count(*) AS n } RETURN sum(n)", "[[4]]");
}

test "pattern memo captures lexical values and read fields include local entities" {
    try check("MATCH (a:Document {id:'a'}) RETURN [x IN ['missing','b','missing'] | size([(a)-[:CITES]->(b) WHERE b.id=x | b.id])]", "[[[0,2,0]]]");
    for ([_][]const u8{
        "MATCH (a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b) | b.title]",
        "MATCH (a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b) | b]",
    }, 0..) |text, index| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const fields = try @import("read_fields.zig").required(arena.allocator(), compiled.query, graph);
        if (index == 0) {
            var found = false;
            for (fields.?) |field| if (std.mem.eql(u8, field, "title")) {
                found = true;
            };
            try std.testing.expect(found);
        } else try std.testing.expect(fields == null);
    }
}

test "certified numeric consolidation releases unpublished rows on allocation failure" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "UNWIND [1,2,1] AS x MATCH (a:Document {id:'a'}) WITH x,a MATCH (a)-[:CITES]->(b) RETURN sum(x),sum(size([(a)-[:CITES]->(c)|c.id]))", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 8), result.rows[0][0].integer);
            try std.testing.expectEqual(@as(i64, 12), result.rows[0][1].integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "adaptive suffix reachability releases every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH REPEATABLE ELEMENTS (a:Document {id:'a'})-[:CITES*14..14]->(b:Document) WITH b.rank AS rank WHERE rank+0<0 RETURN rank LIMIT 1", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 0), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "integer sign certification is per aggregate group and keeps mixed-sign overflow" {
    try check("UNWIND [{k:1,x:1},{k:2,x:-1},{k:1,x:2},{k:2,x:-2}] AS r MATCH (a:Document {id:'a'}) WITH r,a MATCH (a)-[:CITES]->(b) RETURN r.k,sum(r.x)", "[[1,6],[2,-6]]");
    // The positive prefix overflows before the later negative contribution;
    // the unpublished certification attempt must retain that ordinary error.
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [9223372036854775807,1,-1] AS x MATCH (a:Document {id:'c'}) WITH x,a MATCH (a) RETURN sum(x)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "completed streaming child memo releases retained rows on every allocation failure" {
    try check("UNWIND [1,1,2] AS i MATCH (a:Document {id:'a'}) CALL { WITH a MATCH (a)-[:CITES]->(b) RETURN b.rank AS rank } RETURN i,rank LIMIT 10", "[[1,2],[1,2],[1,2],[1,2],[2,2],[2,2]]");
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "UNWIND [1,2] AS i MATCH (a:Document {id:'a'}) CALL { WITH a MATCH (a)-[:CITES]->(b) RETURN b.rank AS rank } RETURN i,rank LIMIT 10", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 4), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "continuation presence memo and intermediate DISTINCT release allocation failures" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, kind: u2) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            const prefix = "MATCH REPEATABLE ELEMENTS (a:Document {id:'a'})-[:CITES*14..14]->(b:Document) ";
            const text = switch (kind) {
                0 => prefix ++ "WITH DISTINCT b.rank AS rank RETURN rank LIMIT 3",
                1 => prefix ++ "CALL { WITH b MATCH (b)-[:CITES]->(c) WHERE c.rank+0<0 RETURN c } RETURN c.rank LIMIT 1",
                2 => prefix ++ "CALL { WITH b MATCH (b)-[:CITES]->(c) RETURN count(*) AS n } WITH n WHERE n<0 RETURN n LIMIT 1",
                3 => prefix ++ "WITH DISTINCT b.rank AS rank ORDER BY rank RETURN rank LIMIT 3",
            };
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, if (kind == 1 or kind == 2) 0 else 2), result.rows.len);
        }
    };
    inline for (0..4) |kind| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as(u2, kind)});
}

test "path summaries preserve zero hop null scope and observable identities" {
    try check("MATCH p=(a:Document {id:'a'})-[rs:CITES*0..0]->(b) RETURN length(p),size(rs),size(nodes(p)),head(nodes(p)).id,last(nodes(p)).id", "[[0,0,1,\"a\",\"a\"]]");
    try check("OPTIONAL MATCH p=(a:Document {id:'absent'})-[:CITES]->(b) RETURN count(p),count(*)", "[[0,1]]");
    try check("OPTIONAL MATCH p=(a:Document {id:'absent'})-[:CITES]->(b) RETURN length(p),head(nodes(p)),last(nodes(p)),size(nodes(p))", "[[null,null,null,null]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN count(DISTINCT p),sum(length(p))", "[[2,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) OPTIONAL MATCH p=(a)-[:CITES*2..2]->(b) RETURN count(p)", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN [p IN [null] | length(p)],length(p)", "[[[null],1],[[null],1]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN p AS q } RETURN length(q)", "[[1],[1]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) WITH p AS q RETURN length(q)", "[[1],[1]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN nodes(p)[0].id,nodes(p)[-1].id,length(p)", "[[\"a\",\"b\",1],[\"a\",\"b\",1]]");
}

test "path summary planning and product states release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH REPEATABLE ELEMENTS p=(a:Document {id:'a'})-[rs:CITES*0..3]->(b) WITH p AS q,rs AS xs RETURN length(q),size(xs),count(*) ORDER BY length(q)", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 4), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "private path summary bindings never enter wildcard result schemas" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES]->(b) WITH p AS q RETURN *,length(q) AS n", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.columns.len);
    try std.testing.expectEqualStrings("q", result.columns[0]);
    try std.testing.expectEqualStrings("n", result.columns[1]);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    for (result.rows) |row| {
        try std.testing.expectEqual(@as(usize, 1), row[0].path.relationships.len);
        try std.testing.expectEqual(@as(i64, 1), row[1].integer);
    }
}

test "nested summary scopes preserve imports shadowing optional nulls and full paths" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN EXISTS { WITH p AS q WHERE length(q)=1 RETURN q },length(p)", "[[true,1],[true,1]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN EXISTS { WITH null AS p RETURN length(p) },length(p)", "[[true,1],[true,1]]");
    try check("OPTIONAL MATCH p=(a:Document {id:'absent'})-[:CITES]->(b) RETURN EXISTS { WITH p WHERE length(p)=0 RETURN 1 },length(p)", "[[false,null]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN [p=(b)-[:CITES]->(c) | length(p)],length(p)", "[[[1,1],1],[[1,1],1]]");
    try check("RETURN size([p=(a:Document {id:'a'})-[:CITES]->(b) | length(p)])", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) WITH p MATCH p=(x)-[:CITES*2..2]->(y) RETURN count(p)", "[[0]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) WITH p OPTIONAL MATCH p=(x)-[:CITES*2..2]->(y) RETURN count(p)", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN size([(b)-[:CITES]->(c) | length(p)]),length(p)", "[[2,1],[2,1]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b,length(p) AS n RETURN b{n},count(*) ORDER BY b{n}", "[[{\"n\":0},1],[{\"n\":1},2]]");
}

test "path moments agree with independently enumerated observable paths" {
    const Dag = struct {
        fn expand(ptr: *anyopaque, a: std.mem.Allocator, mapping: catalog.Relationship, identity: v.Identity, direction: c.ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            const page = try Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
            var out: std.ArrayList(*const v.Relationship) = .empty;
            for (page.items) |edge| if (!std.mem.eql(u8, edge.identity.key, "ba")) try out.append(a, edge);
            return .{ .items = out.items };
        }
    };
    for ([_]bool{ false, true }) |acyclic| for ([_][]const u8{ "", "REPEATABLE ELEMENTS ", "DIFFERENT RELATIONSHIPS " }) |mode| for ([_][]const u8{
        "(a:Document)-[:CITES*0..3]->(b)",
        "(a:Document {id:'a'})-[:CITES*1..3]->(b)",
        "(a:Document {id:'a'})-[:CITES*0..2]->()-[:CITES*0..2]->(b)",
    }) |pattern| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        if (acyclic) callbacks.expand = Dag.expand;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        var diagnostic: c.Diagnostic = .{};
        const prefix = try std.fmt.allocPrint(a, "MATCH {s}p={s} ", .{ mode, pattern });
        var reference = try c.compile(a, try std.fmt.allocPrint(a, "{s}RETURN p", .{prefix}), .{}, &diagnostic);
        defer reference.deinit();
        var paths = try runtime.execute(a, backend, graph, &reference, &.{}, .{});
        defer paths.deinit();
        var sum: usize = 0;
        var minimum: usize = std.math.maxInt(usize);
        var maximum: usize = 0;
        for (paths.rows) |row| {
            const length = row[0].path.relationships.len;
            sum += length;
            minimum = @min(minimum, length);
            maximum = @max(maximum, length);
        }
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s}RETURN count(length(p)),sum(length(p)),min(length(p)),max(length(p)),avg(length(p))", .{prefix}), .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(a, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(paths.rows.len)), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, @intCast(sum)), result.rows[0][1].integer);
        try std.testing.expectEqual(@as(i64, @intCast(minimum)), result.rows[0][2].integer);
        try std.testing.expectEqual(@as(i64, @intCast(maximum)), result.rows[0][3].integer);
        try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(paths.rows.len)), result.rows[0][4].number, 1e-12);
    };
}

test "nested summary and moment plans release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, nested: bool) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, if (nested) "MATCH p=(a:Document {id:'a'})-[:MENTIONS*0..1]->(b) WHERE EXISTS { WITH p WHERE length(p)>=0 RETURN 1 } RETURN min(length(p)),max(length(p)),sum(length(p)),avg(length(p))" else "MATCH p=(a:Document {id:'a'})-[:MENTIONS*0..1]->(b) RETURN min(length(p)),max(length(p)),sum(length(p)),avg(length(p))", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        }
    };
    inline for (.{ false, true }) |nested| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{nested});
}

test "summary pruning preserves aggregate grouping and empty grouped inputs" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b,count(p) AS n RETURN count(*)", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b,sum(length(p)) AS n RETURN n ORDER BY n", "[[0],[2]]");
    try check("MATCH p=(a:Document {id:'absent'})-[:CITES]->(b) WITH b,count(p) AS n RETURN count(*)", "[[0]]");
    try check("MATCH p=(a:Document {id:'absent'})-[:CITES]->(b) WITH 1 AS k,count(p) AS n RETURN count(*)", "[[0]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) CALL { WITH b,p RETURN b AS k,sum(length(p)) AS n } RETURN count(*)", "[[3]]");
}

test "summary exports and representative bags preserve CALL scope and memo replay" {
    try check("MATCH p=(a:Document {id:'a'})-[rs:CITES*0..1]->(b) WITH rs AS xs RETURN sum(size(xs))", "[[2]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN p } RETURN min(length(p)),max(length(p)),sum(length(p)),avg(length(p))>0.66 AND avg(length(p))<0.67", "[[0,1,2,true]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN p AS q } RETURN count(q)", "[[3]]");
    try check("UNWIND [1,1,2] AS i CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN p } RETURN i,sum(length(p)) ORDER BY i", "[[1,4],[2,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) CALL { WITH p RETURN length(p) AS n } RETURN min(n),max(n),sum(n)", "[[0,1,2]]");
    try check("CALL { CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN p } RETURN p AS q } RETURN sum(length(q))", "[[2]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES]->(b) RETURN p } RETURN count(DISTINCT p)", "[[2]]");
    try check("CALL { MATCH p=(a:Document {id:'absent'})-[:CITES]->(b) RETURN p } RETURN count(p),sum(length(p)),min(length(p))", "[[0,0,null]]");
}

test "multiple segment statistics agree with complete observable route histories" {
    for ([_][]const u8{ "", "REPEATABLE ELEMENTS " }) |mode| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var fixture: Fixture = .{};
        var diagnostic: c.Diagnostic = .{};
        const prefix = try std.fmt.allocPrint(a, "MATCH {s}p=(a:Document {{id:'a'}})-[xs:CITES*0..2]->()-[ys:CITES*0..2]->(b) ", .{mode});
        var reference = try c.compile(a, try std.fmt.allocPrint(a, "{s}RETURN p,size(xs),size(ys)", .{prefix}), .{}, &diagnostic);
        defer reference.deinit();
        var rows = try runtime.execute(a, fixture.backend(), graph, &reference, &.{}, .{});
        defer rows.deinit();
        var sums: [3]i64 = @splat(0);
        var minima: [3]i64 = @splat(std.math.maxInt(i64));
        var maxima: [3]i64 = @splat(0);
        for (rows.rows) |row| for ([_]i64{ @intCast(row[0].path.relationships.len), row[1].integer, row[2].integer }, 0..) |n, i| {
            sums[i] += n;
            minima[i] = @min(minima[i], n);
            maxima[i] = @max(maxima[i], n);
        };
        var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s}RETURN sum(length(p)),sum(size(xs)),sum(size(ys)),min(length(p)),min(size(xs)),min(size(ys)),max(length(p)),max(size(xs)),max(size(ys)),avg(length(p)),avg(size(xs)),avg(size(ys))", .{prefix}), .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        for (0..3) |i| {
            try std.testing.expectEqual(sums[i], result.rows[0][i].integer);
            try std.testing.expectEqual(minima[i], result.rows[0][3 + i].integer);
            try std.testing.expectEqual(maxima[i], result.rows[0][6 + i].integer);
            try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(sums[i])) / @as(f64, @floatFromInt(rows.rows.len)), result.rows[0][9 + i].number, 1e-12);
        }
    }
}

test "escaping vector summary bags unwind allocation failures through CALL memoization" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "CALL { MATCH REPEATABLE ELEMENTS p=(a:Document {id:'a'})-[xs:CITES*0..2]->()-[ys:CITES*0..1]->(b) RETURN p AS q,size(xs) AS n } RETURN sum(length(q)),avg(n),min(n),max(n)", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "affine moment consumers retain checked arithmetic and alias lineage" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b,length(p) AS n WHERE b.rank<0 WITH n+9223372036854775807 AS k RETURN avg(k)", "[[null]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN min(length(p)+1),max(length(p)+1),sum(length(p)+1),avg(length(p)+1)>1.66 AND avg(length(p)+1)<1.67", "[[1,2,5,true]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH 2*length(p)+1 AS n WITH -n AS k RETURN min(k),max(k),sum(k)", "[[-3,-1,-7]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN sum(length(p)-1),avg(length(p)-length(p))", "[[-1,0]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN length(p)+1 AS n } RETURN sum(n)", "[[5]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN avg((length(p)+9223372036854775807)-9223372036854775807)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "fixed UNWIND repetitions preserve weighted scalar and path summary lineage" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n CALL { WITH n UNWIND [n,n] AS x RETURN x } RETURN count(*),sum(x)", "[[6,4]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n] AS x RETURN count(*),sum(x),min(x),max(x)", "[[6,4,0,1]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [p,p] AS q RETURN count(*),sum(length(q)),min(length(q)),max(length(q))", "[[6,4,0,1]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN length(p) AS n } UNWIND [n,n] AS x RETURN count(*),sum(x)", "[[6,4]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n+1] AS x RETURN count(*),sum(x),min(x),max(x)", "[[6,7,0,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [] AS x RETURN count(*),sum(n)", "[[0,0]]");
}

test "affine and repeated summary bags release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [p,p] AS q WITH length(q)+1 AS n RETURN sum(n),avg(n)", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 10), result.rows[0][0].integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "endpoint certificates decline before allocating summary geometry" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (a:Document)-[:CITES*0..3]->(b:Document) RETURN b.rank AS r", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const cursor = (try runtime.NodeCursor.open(std.testing.allocator, fixture.backend(), graph, compiled.query, &.{}, .{})).?;
    defer cursor.close();
    const peak = cursor.budget.peak;
    for (0..64) |_| try std.testing.expect(try cursor.planSummaryFields(&.{"r"}, .presence, "r", &.{}) == null);
    try std.testing.expectEqual(peak, cursor.budget.peak);
    var scalar = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..3]->(b:Document) RETURN length(p) AS hops", .{}, &diagnostic);
    defer scalar.deinit();
    const scalar_cursor = (try runtime.NodeCursor.open(std.testing.allocator, fixture.backend(), graph, scalar.query, &.{}, .{})).?;
    defer scalar_cursor.close();
    try std.testing.expect(try scalar_cursor.planSummaryFields(&.{"hops"}, .exact, "hops", &.{}) != null);
}

test "invariant aliases and affine list branches preserve exact reductions" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,1 AS k RETURN min(n+k),max(n+k),sum(n+k)", "[[1,2,5]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN min(length(p)+b.rank),max(length(p)+b.rank),avg(length(p)+b.rank)=7.0/3", "[[1,3,true]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n+1,0] AS x RETURN count(*),sum(x),min(x),max(x)", "[[9,7,0,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n+1] AS x UNWIND [x,2*x] AS y RETURN count(*),sum(y),min(y),max(y)", "[[12,21,0,4]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n+1] AS x RETURN x } RETURN count(*),sum(x),min(x),max(x)", "[[6,7,0,2]]");
    try check("WITH 1 AS k MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,'x' AS k RETURN min(n+k),max(n+k)", "[[\"0x\",\"1x\"]]");
    try check("WITH 1 AS k MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND ['x'] AS k RETURN min(n+k),max(n+k)", "[[\"0x\",\"1x\"]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,{rank:'x'} AS b RETURN min(n+b.rank),max(n+b.rank)", "[[\"0x\",\"1x\"]]");
    // A distribution observer must keep actual values rather than moment bins.
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n+1] AS x WITH x WHERE x=1 RETURN count(*)", "[[3]]");
    // Noninteger endpoint properties retain overloaded + and NULL semantics.
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN min(length(p)+b.id),max(length(p)+b.id)", "[[\"0a\",\"1b\"]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN count(length(p)+b.missing),avg(length(p)+b.missing)", "[[0,null]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n WHERE b.rank<0 UNWIND [n,n+9223372036854775807] AS x RETURN avg(x)", "[[null]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n UNWIND [n,n+9223372036854775807] AS x RETURN avg(x)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "bound affine parameters retain invocation types nulls and overflow" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN min(length(p)+$offset),max(length(p)+$offset),count(length(p)+$offset)", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_]v.Value{ .{ .integer = 1 }, .{ .integer = -2 }, .null, .{ .string = "x" } }, [_][]const u8{ "[[1,2,3]]", "[[-2,-1,3]]", "[[null,null,0]]", "[[\"0x\",\"1x\",3]]" }) |value, expected| {
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "offset", .value = value }}, .{});
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.rows, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings(expected, json);
    }
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "offset", .value = .{ .integer = std.math.maxInt(i64) } }}, .{}));
}

test "invariant and affine choice proofs release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            // Fail-index coverage requires deterministic arena growth. A
            // successful backing remap otherwise changes allocation counts.
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,b.rank AS k UNWIND [n,n+k] AS x RETURN min(x),max(x),avg(x)", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 3), result.rows[0][1].integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "endpoint entity proofs survive aliases wildcard and returning CALL" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b,length(p) AS n RETURN min(n+b.rank),max(n+b.rank),avg(n+b.rank)=7.0/3", "[[1,3,true]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b AS endpoint,length(p) AS n WITH endpoint AS x,n RETURN min(n+x.rank),max(n+x.rank)", "[[1,3]]");
    try check("CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN b AS endpoint,length(p) AS n } RETURN min(n+endpoint.rank),max(n+endpoint.rank)", "[[1,3]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH *,b AS endpoint RETURN min(length(p)+endpoint.rank),max(length(p)+endpoint.rank)", "[[1,3]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH b AS endpoint,length(p) AS n WITH {rank:'x'} AS endpoint,n RETURN min(n+endpoint.rank),max(n+endpoint.rank)", "[[\"0x\",\"1x\"]]");
}

test "length guard bounds preserve empty optional and eager error semantics" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=1 RETURN count(*),min(length(p)),max(length(p))", "[[2,1,1]]");
    try check("MATCH p=(a:Document {id:'a'})-[xs:CITES*0..1]->(b) WHERE 0<size(xs) AND size(xs)<=1 RETURN count(*),sum(size(xs))", "[[2,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE 0<length(p)<=1 RETURN count(*)", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)<0 RETURN count(*),min(length(p))", "[[0,null]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=2 RETURN count(*)", "[[0]]");
    try check("OPTIONAL MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=2 RETURN count(*),count(p)", "[[1,0]]");
    for ([_][]const u8{
        "MATCH p=(a:Document {id:'a'})-[:CITES*1..2]->(b:Document {rank:1/(b.rank-2)}) WHERE length(p)>=2 RETURN count(*)",
        "MATCH p=(a:Document {rank:1/0})-[:CITES*0..1]->(b) WHERE length(p)>=2 RETURN count(*)",
        "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=2 AND 1/0=0 RETURN count(*)",
        "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE 1/0=0 AND length(p)>=2 RETURN count(*)",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "length bounds specialize each invocation without changing parameter semantics" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=$minimum RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_]v.Value{ .{ .integer = 0 }, .{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = -2 }, .null, .{ .number = 0.5 }, .{ .integer = std.math.maxInt(i64) } }, [_]i64{ 3, 2, 0, 3, 0, 2, 0 }) |value, expected| {
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "minimum", .value = value }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(expected, result.rows[0][0].integer);
    }
}

test "endpoint aliases and range certificates release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var fixed: FixedAllocation = .{ .backing = backing };
            const a: std.mem.Allocator = .{ .ptr = &fixed, .vtable = &.{ .alloc = FixedAllocation.alloc, .resize = FixedAllocation.resize, .remap = FixedAllocation.remap, .free = FixedAllocation.free } };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "CALL { MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=$minimum RETURN b AS endpoint,length(p) AS n } RETURN min(n+endpoint.rank),max(n+endpoint.rank),avg(n+endpoint.rank)", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "minimum", .value = .{ .integer = 1 } }}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 3), result.rows[0][1].integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "length guards cross only total conjunctions and transparent projections" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=1 AND true AND a.id='a' RETURN count(*)", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n WITH n AS hops WHERE hops>=1 AND true RETURN count(*)", "[[2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,1 AS k WHERE n>=2 RETURN count(*)", "[[0]]");
    try check("OPTIONAL MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n WHERE n>=2 RETURN count(*)", "[[0]]");
    try check("OPTIONAL MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=2 RETURN count(*)", "[[1]]");
    for ([_][]const u8{
        "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=1 AND 1/length(p)>0 RETURN count(*)",
        "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,1/length(p) AS x WHERE n>=1 RETURN count(*)",
        "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n WHERE n>=1 AND 1/n>0 RETURN count(*)",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "UNWIND preserves endpoint lineage multiplicity and opaque shadows" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND length(p) AS n RETURN min(n),max(n),sum(n)", "[[0,1,2]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [b,b] AS x RETURN min(length(p)+x.rank),max(length(p)+x.rank),sum(length(p)+x.rank),count(*)", "[[1,3,14,6]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [b.rank,b.rank+1] AS x RETURN min(length(p)+x),max(length(p)+x),sum(length(p)+x)", "[[1,4,17]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [b] AS x UNWIND [x] AS y RETURN min(length(p)+y.rank),max(length(p)+y.rank)", "[[1,3]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [b.id] AS x RETURN min(length(p)+x),max(length(p)+x)", "[[\"0a\",\"1b\"]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) UNWIND [b,b] AS x WITH length(p) AS n,{rank:'x'} AS x RETURN min(n+x.rank),max(n+x.rank)", "[[\"0x\",\"1x\"]]");
}

test "typed length bounds retain numeric ordering aliases NULL and eager errors" {
    for ([_][]const u8{
        "WHERE length(p)>=1.0 RETURN count(*)",
        "WHERE length(p)>0.5 RETURN count(*)",
        "WITH length(p) AS n,1.0 AS low WHERE n>=low RETURN count(*)",
        "WITH length(p) AS n,1 AS low WITH n,low AS minimum WHERE n>=minimum RETURN count(*)",
    }) |tail| {
        const query = try std.fmt.allocPrint(std.testing.allocator, "MATCH p=(a:Document {{id:'a'}})-[:CITES*0..1]->(b) {s}", .{tail});
        defer std.testing.allocator.free(query);
        try check(query, "[[2]]");
    }
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)=0.5 RETURN count(*)", "[[0]]");
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,null AS low WHERE n>=low RETURN count(*)", "[[0]]");
    try check("WITH 1 AS low UNWIND [0] AS x WITH x AS low MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=low RETURN count(*)", "[[3]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=$minimum RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_]v.Value{ .{ .number = 0.5 }, .{ .number = 1.0 }, .{ .number = -0.5 }, .{ .number = 9223372036854775808.0 }, .null }, [_]i64{ 2, 2, 3, 0, 0 }) |value, expected| {
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "minimum", .value = value }}, .{});
        defer result.deinit();
        try std.testing.expectEqual(expected, result.rows[0][0].integer);
    }
    var bad = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=$minimum AND 1/length(p)>0 RETURN count(*)", .{}, &diagnostic);
    defer bad.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &bad, &.{.{ .name = "minimum", .value = .null }}, .{}));
}

test "constant bounds cross total unknown bindings without crossing errors" {
    try check("WITH 1 AS low,0 AS unrelated WITH unrelated AS x,low MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WHERE length(p)>=low RETURN count(*)", "[[2]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) WITH length(p) AS n,1/length(p) AS bad,1 AS low WHERE n>=low RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "exact floating summaries and total-axis bags retain aggregate values and allocation failures" {
    try check("MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN min(2.0*length(p)),max(length(p)/2.0),avg(length(p)/2.0)>0.33 AND avg(length(p)/2.0)<0.34", "[[0,0.5,true]]");
    try check("MATCH p=(a:Document {id:'a'})-[xs:CITES*0..1]->()-[ys:CITES*0..1]->(b) RETURN min(size(xs)+size(ys)),max(size(xs)+size(ys)),min(size(xs)-size(ys)),max(size(xs)-size(ys))", "[[0,2,-1,1]]");
    const Campaign = struct {
        fn run(allocator: std.mem.Allocator) !void {
            // Failure positions must not depend on arena remapping into
            // adjacent allocations freed by earlier group finalization.
            var vtable = allocator.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH p=(a:Document {id:'a'})-[xs:CITES*0..1]->()-[ys:CITES*0..1]->(b) WITH size(xs) AS x,size(ys) AS y RETURN min(x+y),max(0.5*(x+y))", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 0), result.rows[0][0].integer);
            try std.testing.expectEqual(@as(f64, 1), result.rows[0][1].number);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "directional, quadratic and support reductions agree with materialized paths" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base = "MATCH REPEATABLE ELEMENTS p=(a:Document {id:'a'})-[xs:CITES*0..2]->()-[ys:CITES*0..2]->(b) ";
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var reference = try c.compile(a, try std.fmt.allocPrint(a, "{s}RETURN size(xs),size(ys)", .{base}), .{}, &diagnostic);
    defer reference.deinit();
    var rows = try runtime.execute(a, fixture.backend(), graph, &reference, &.{}, .{});
    defer rows.deinit();
    var min: i64 = std.math.maxInt(i64);
    var max: i64 = std.math.minInt(i64);
    var sum: i64 = 0;
    var support: [5]bool = @splat(false);
    for (rows.rows) |row| {
        const x = row[0].integer;
        const y = row[1].integer;
        min = @min(min, 2 * x - y);
        max = @max(max, 2 * x - y);
        sum += x * x + x * y + y * y;
        support[@intCast(x + y)] = true;
    }
    var compiled = try c.compile(a, try std.fmt.allocPrint(a, "{s}RETURN min(2*size(xs)-size(ys)),max(2*size(xs)-size(ys)),avg(size(xs)*size(xs)+size(xs)*size(ys)+size(ys)*size(ys)),count(DISTINCT size(xs)+size(ys))", .{base}), .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(min, result.rows[0][0].integer);
    try std.testing.expectEqual(max, result.rows[0][1].integer);
    try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(rows.rows.len)), result.rows[0][2].number, 1e-12);
    var cardinality: i64 = 0;
    for (support) |present| {
        cardinality += @intFromBool(present);
    }
    try std.testing.expectEqual(cardinality, result.rows[0][3].integer);
}

test "quadratic path statistics release every allocation failure" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            // Arena resize/remap success depends on adjacent allocations and
            // otherwise changes the number of injected failure positions.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH REPEATABLE ELEMENTS p=(a:Document {id:'a'})-[xs:CITES*0..1]->()-[ys:CITES*0..1]->(b) RETURN avg(length(p)*length(p)),min(2*size(xs)-size(ys)),count(DISTINCT length(p))", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expect(result.rows[0][0].number > 0);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "statistics keep shortest path selectors on their frontier executor" {
    try check("MATCH p=allShortestPaths((a:Document {id:'a'})-[:CITES*0..2]->(b)) RETURN count(*),count(DISTINCT length(p)),avg(length(p)*length(p))", "[[5,3,2]]");
    try check("MATCH p=shortestPath((a:Document {id:'a'})-[:CITES*0..2]->(b)) RETURN count(*),count(DISTINCT length(p)),avg(length(p)*length(p))", "[[3,3,1.6666666666666667]]");
}

test "directional extrema retain safe offsets across wide internal costs" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH REPEATABLE ELEMENTS p=(a:Document {id:'a'})-[xs:CITES*0..3]->()-[ys:CITES*0..1]->(b) RETURN min((2*size(xs)+size(ys)-6)*1537228672809129301),max((2*size(xs)+size(ys)-6)*1537228672809129301)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .path_depth = 4 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, -9223372036854775806), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 1537228672809129301), result.rows[0][1].integer);
}

test "mixed statistic cohorts and grouped supports agree with observable histories" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const base = "MATCH REPEATABLE ELEMENTS p=(a:Document {id:'a'})-[:CITES*0..3]->(b) ";
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var reference = try c.compile(a, base ++ "RETURN b.rank,length(p)", .{}, &diagnostic);
    defer reference.deinit();
    var rows = try runtime.execute(a, fixture.backend(), graph, &reference, &.{}, .{});
    defer rows.deinit();
    var compiled = try c.compile(a, base ++ "RETURN b.rank AS rank,avg(length(p)*length(p)),avg(length(p)/2.0),count(DISTINCT length(p)) ORDER BY rank", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    for (result.rows) |row| {
        var count: usize = 0;
        var first: i64 = 0;
        var second: i64 = 0;
        var support: u8 = 0;
        for (rows.rows) |observed| if (observed[0].integer == row[0].integer) {
            const n = observed[1].integer;
            count += 1;
            first += n;
            second += n * n;
            support |= @as(u8, 1) << @as(u3, @intCast(n));
        };
        try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(second)) / @as(f64, @floatFromInt(count)), row[1].number, 1e-12);
        try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(first)) / @as(f64, @floatFromInt(count)) / 2, row[2].number, 1e-12);
        try std.testing.expectEqual(@as(i64, @popCount(support)), row[3].integer);
    }
}

test "grouped sparse supports and certificate cohorts release every allocation failure" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            // Count allocation failures independently of opportunistic arena
            // growth: successful resize/remap varies with the failing slot.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "UNWIND [-10000000000,0,10000000000] AS k MATCH p=(a:Document {id:'a'})-[:CITES*0..1]->(b) RETURN b.rank AS rank,count(DISTINCT length(p)+k),avg(length(p)*length(p)),avg(0.5*length(p)) ORDER BY rank", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = std.math.maxInt(usize) });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
            for (result.rows) |row| try std.testing.expectEqual(@as(i64, 3), row[1].integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "compressed DISTINCT supports merge with ordinary numeric contributions in either order" {
    try check("UNWIND [['c',0],['a',0]] AS pair WITH pair[0] AS id,pair[1] AS k MATCH p=(s:Document {id:id})-[:CITES*0..2]->(b) RETURN count(DISTINCT length(p)+k)", "[[3]]");
    try check("UNWIND [['a',0],['c',0]] AS pair WITH pair[0] AS id,pair[1] AS k MATCH p=(s:Document {id:id})-[:CITES*0..2]->(b) RETURN count(DISTINCT length(p)+k)", "[[3]]");
    try check("UNWIND [['c',0],['a',0.0]] AS pair WITH pair[0] AS id,pair[1] AS k MATCH p=(s:Document {id:id})-[:CITES*0..2]->(b) RETURN count(DISTINCT length(p)+k)", "[[3]]");
    try check("UNWIND [['a',0.0],['c',0]] AS pair WITH pair[0] AS id,pair[1] AS k MATCH p=(s:Document {id:id})-[:CITES*0..2]->(b) RETURN count(DISTINCT length(p)+k)", "[[3]]");
}

test "group windows preserve alias shadowing, stable ties and projection failures" {
    try check("UNWIND [2,1,3,2,1] AS x RETURN x,count(*) AS n ORDER BY n DESC,x SKIP 1 LIMIT 1", "[[2,2]]");
    try check("UNWIND [2,1,3,2,1] AS x RETURN x AS y,count(*) AS x ORDER BY x DESC,y SKIP 1 LIMIT 1", "[[2,2]]");
    for ([_][]const u8{
        "UNWIND [0,1] AS x RETURN x,count(*)/(1-x) AS n ORDER BY x LIMIT 1",
        "UNWIND [0,1] AS x RETURN x,count(*)/(1-x) AS n ORDER BY x LIMIT 0",
        "UNWIND [0,1] AS x RETURN x,count(*)/(1-x) AS n LIMIT -1",
    }) |query| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "group key slots preserve compound expressions lexical bindings aliases and entity selectors" {
    try check("UNWIND [1,2,1] AS x RETURN x+1 AS key,count(*) AS c,(x+1)+count(*) AS total ORDER BY x+1", "[[2,2,4],[3,1,4]]");
    try check("UNWIND [1,2,1] AS x RETURN x AS key,count(*) AS c,[x IN [7,8] | x] AS locals ORDER BY key", "[[1,2,[7,8]],[2,1,[7,8]]]");
    try check("UNWIND [1,2,1] AS x RETURN x+1 AS key,count(*) AS c,[x IN [7] | x+1] AS locals ORDER BY key", "[[2,2,[8]],[3,1,[8]]]");
    try check("UNWIND [1,2,1] AS x RETURN x AS key,count(*) AS x ORDER BY x DESC,key", "[[1,2],[2,1]]");
    try check("MATCH (n:Document) WITH n AS entity,count(*) AS c,n{.title} AS projected ORDER BY n.rank LIMIT 1 RETURN entity.title,c,projected", "[[\"Alpha\",1,{\"title\":\"Alpha\"}]]");
    try check("UNWIND [1,2,1] AS x RETURN x+1 AS key,[x IN [7] | x+1]+[count(*)] AS totals ORDER BY key", "[[2,[8,2]],[3,[8,1]]]");
    try check("UNWIND [1,2] AS x WITH {v:1} AS m,x RETURN m AS map,x,count(*)+m{x}.x AS total ORDER BY x", "[[{\"v\":1},1,2],[{\"v\":1},2,3]]");
    try check("UNWIND [1,1] AS x WITH x AS x,count(*) AS c WHERE c=2 RETURN x,c", "[[1,2]]");
    try check("UNWIND [1,2,1] AS x RETURN x AS key,count(*) AS c ORDER BY count(*) DESC,key", "[[1,2],[2,1]]");
}

test "shared aggregates preserve distinct flags case spelling and lexical scopes" {
    try check("UNWIND [1,1,2] AS x RETURN count(x) AS a,COUNT(x) AS b,count(DISTINCT x) AS c,size(collect(x)) AS d,size(COLLECT(x)) AS e", "[[3,3,2,3,3]]");
    try check("UNWIND [1,2,1] AS x RETURN x AS key,sum(x) AS a,SUM(x) AS b ORDER BY sum(x) DESC,key", "[[1,2,2],[2,2,2]]");
}

test "group key reuse preserves ORDER BY alias type errors" {
    const text = "UNWIND [{x:1}] AS n RETURN n.x AS n,count(*) AS c ORDER BY n.x";
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    try check("UNWIND [{x:1}] AS n RETURN DISTINCT n.x AS n,count(*) AS c ORDER BY n.x", "[[1,1]]");
}

test "DISTINCT input cohorts preserve nulls representations first-seen order and lazy errors" {
    try check("UNWIND [null,2,1,2,1.0,null] AS x RETURN count(DISTINCT x),collect(DISTINCT x),sum(DISTINCT x),avg(DISTINCT x)", "[[2,[2,1],3,1.5]]");
    try check("UNWIND [1.0,1,2,null] AS x RETURN collect(DISTINCT x),count(DISTINCT x),sum(DISTINCT x)", "[[[1,2],2,3]]");
    try check("UNWIND [[1],[1],[2],null] AS x RETURN count(DISTINCT x),collect(DISTINCT x)", "[[2,[[1],[2]]]]");
    try check("UNWIND [1,2,1] AS x RETURN x%2 AS key,count(DISTINCT x),collect(DISTINCT x) ORDER BY key", "[[0,1,[2]],[1,1,[1]]]");
    try check("UNWIND [1,2] AS x RETURN CASE WHEN true THEN count(DISTINCT x) ELSE sum(DISTINCT 1/0) END,collect(DISTINCT x)", "[[2,[1,2]]]");
}

test "bounded group keys retain exact counts stable ties nulls and multiple order columns" {
    try check("UNWIND [3,1,2,3,1,3] AS x RETURN x AS key,count(*) AS c ORDER BY key LIMIT 1", "[[1,2]]");
    try check("UNWIND [3,1,2,3,1,3] AS x RETURN x AS key,count(*) AS c ORDER BY key DESC SKIP 1 LIMIT 1", "[[2,1]]");
    try check("UNWIND [3,1,2,3,1,3] AS x RETURN x AS key,count(*) AS c ORDER BY key%2 LIMIT 2", "[[2,1],[3,3]]");
    try check("UNWIND [3,1,2,3,1,3] AS x RETURN x%2 AS rank,x AS key,count(*) AS c ORDER BY rank,key DESC LIMIT 2", "[[0,2,1],[1,3,3]]");
    try check("UNWIND [null,1,2,null,1] AS x RETURN x AS key,count(x) AS c ORDER BY key DESC LIMIT 1", "[[null,0]]");
    try check("UNWIND [1,1,2] AS x RETURN x AS key,count(*) AS c ORDER BY key LIMIT 0", "[]");
    try check("MATCH (n:Document) RETURN n.rank AS key,count(*) AS c ORDER BY key DESC LIMIT 1", "[[3,1]]");
    try check("MATCH (n:Document) RETURN n.rank%2 AS r,n.rank AS k,count(*) AS c ORDER BY r LIMIT 2", "[[0,2,1],[1,1,1]]");
    for ([_][]const u8{
        "UNWIND [0,1] AS x RETURN x AS key,count(1/(1-x)) AS c ORDER BY key LIMIT 1",
        "UNWIND [0,1] AS x RETURN x AS key,count(1/(1-x)) AS c ORDER BY key LIMIT 0",
        "UNWIND [0,1] AS x RETURN x AS key,sum(1/(1-x)) AS c ORDER BY key LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "DISTINCT cohorts and evicting group windows release every allocation failure" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, bounded: bool) !void {
            // Keep the injected failure positions independent of opportunistic
            // arena resize/remap success and adjacent allocator activity.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            const text = if (bounded)
                "MATCH (n:Document) RETURN n.rank AS key,count(n.title) AS c ORDER BY key DESC LIMIT 1"
            else
                "UNWIND [2,1,2,3] AS x RETURN count(DISTINCT x),collect(DISTINCT x),avg(DISTINCT x)";
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        }
    };
    inline for (.{ false, true }) |bounded| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{bounded});
}

test "collection cardinality preserves nulls scopes lazy branches and escaped collections" {
    try check("UNWIND [null,1,1.0,2] AS x RETURN size(collect(x)),size(collect(DISTINCT x))", "[[3,2]]");
    try check("UNWIND [] AS x RETURN size(collect(x)),size(collect(DISTINCT x))", "[[0,0]]");
    try check("UNWIND [null,[1],[1],[2]] AS x RETURN size(collect(DISTINCT x))", "[[2]]");
    try check("UNWIND [1,2,1] AS x RETURN x%2 AS k,size(collect(x)) AS c ORDER BY k", "[[0,1],[1,2]]");
    try check("MATCH(n:Document) RETURN size(collect(n.title)),size(collect(DISTINCT n.rank))", "[[3,3]]");
    try check("UNWIND [1,2] AS x RETURN CASE WHEN true THEN size(collect(x)) ELSE size(collect(1/0)) END", "[[2]]");
    try check("UNWIND [1,2] AS x RETURN size(collect(x)),collect(x)", "[[2,[1,2]]]");
    try check("UNWIND [1,2] AS x CALL { WITH x UNWIND [x,x] AS y RETURN size(collect(y)) AS n } RETURN collect(n)", "[[[2,2]]]");
    try check("UNWIND [1] AS x RETURN size(collect(x)) UNION ALL UNWIND [1,2] AS x RETURN size(collect(x))", "[[1],[2]]");
    for ([_][]const u8{
        "UNWIND [0,1] AS x RETURN size(collect(1/(1-x)))",
        "UNWIND [0,1] AS x RETURN size(collect(DISTINCT 1/(1-x))) LIMIT 0",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "borrowed group reductions own escaped nested values and order keys" {
    try check("MATCH(n:Document) RETURN n.rank%2 AS k,collect({title:n.title,rank:n.rank}) AS xs ORDER BY head(xs).rank DESC", "[[0,[{\"title\":\"Beta\",\"rank\":2}]],[1,[{\"title\":\"Alpha\",\"rank\":1},{\"title\":\"Gamma\",\"rank\":3}]]]");
    try check("MATCH(n:Document) WITH n.rank%2 AS k,collect(n.title)[0..1] AS xs ORDER BY k RETURN k,head(xs)", "[[0,\"Beta\"],[1,\"Alpha\"]]");
    try check("MATCH(n:Document) RETURN DISTINCT head(collect(n.title)) AS title ORDER BY title", "[[\"Alpha\"]]");
    try check("MATCH(n:Document) RETURN n.rank%2 AS k,min([n.title]) AS title ORDER BY title", "[[1,[\"Alpha\"]],[0,[\"Beta\"]]]");
}

test "borrowed aggregate projection and cardinality lowering release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            // In-place arena growth depends on backing allocation layout.
            // Force growth through fallible allocations so every injected
            // failure index refers to the same operation on every run.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH(n:Document) RETURN n.rank%2 AS k,head(collect(n.title)) AS title,size(collect(n.rank)) AS c ORDER BY title", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 2), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "aggregate retention keeps execution containers outside the value depth limit" {
    for ([_][]const u8{ "", " LIMIT 1" }) |tail| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "MATCH(n:Document) RETURN min(reduce(xs=0,x IN range(1,64)|[xs])) AS xs ORDER BY xs{s}", .{tail});
        defer std.testing.allocator.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        var value = result.rows[0][0];
        for (0..64) |_| value = value.list[0];
        try std.testing.expectEqual(@as(i64, 0), value.integer);
    }
}

test "alias cardinality follows bindings without changing escapes grouping or lazy errors" {
    try check("MATCH(n:Document) WITH collect(n.title) AS xs RETURN size(xs)", "[[3]]");
    try check("MATCH(n:Document) WITH collect(n.title) AS xs WITH xs AS ys WITH ys AS zs RETURN size(zs)", "[[3]]");
    try check("UNWIND [null,1,1.0,2] AS x WITH collect(DISTINCT x) AS xs RETURN size(xs)", "[[2]]");
    try check("UNWIND [] AS x WITH collect(x) AS xs RETURN size(xs)", "[[0]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,collect(x) AS ys RETURN size(xs),size(ys)", "[[2,2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,collect(x) AS ys RETURN size(xs),ys", "[[2,[1,2]]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size(xs),xs", "[[2,[1,2]]]");
    try check("UNWIND ['b','a'] AS x WITH x,collect(x) AS xs ORDER BY xs RETURN x,size(xs)", "[[\"a\",1],[\"b\",1]]");
    try check("UNWIND ['b','a'] AS x WITH x,collect(x) AS xs WITH DISTINCT xs RETURN size(xs)", "[[1],[1]]");
    try check("UNWIND ['b','a'] AS x WITH x,collect(x) AS xs WITH xs,count(*) AS c RETURN size(xs),c", "[[1,1],[1,1]]");
    try check("MATCH(n:Document) WITH collect(n.title) AS xs RETURN size(xs),[xs IN [[1,2]]|size(xs)]", "[[3,[2]]]");
    try check("MATCH(n:Document) WITH collect(n.title) AS xs WITH size(xs) RETURN `size(xs)`", "[[3]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs WITH [7] AS xs RETURN size(xs)", "[[1]]");
    try check("MATCH(n:Document) WITH collect(n.title) AS xs MATCH(m:Document {id:'a'}) RETURN size(xs)", "[[3]]");
    try check("MATCH(n:Document) WITH collect(n.title) AS xs RETURN EXISTS { WITH xs RETURN size(xs) AS n },size(xs)", "[[true,3]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN CASE WHEN false THEN size(xs) ELSE 0 END", "[[0]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs WITH * RETURN size(xs),xs", "[[2,[1,2]]]");
    try check("UNWIND [1] AS x WITH collect(x) AS xs RETURN size(xs) UNION ALL UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size(xs)", "[[1],[2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN DISTINCT size(xs) AS xs ORDER BY size(xs)", "[[2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN DISTINCT size(xs)+1 AS xs ORDER BY size(xs)+1", "[[3]]");
    for ([_][]const u8{
        "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size(xs) AS xs ORDER BY size(xs)",
        "UNWIND [1,2] AS x WITH collect(x) AS `42` RETURN size(`42`) AS c,42 ORDER BY size(`42`)",
        "MATCH(n:Document) WITH n,collect(n.title) AS `n.rank` RETURN size(`n.rank`) AS c,n.rank ORDER BY size(`n.rank`)",
    }) |text| {
        var diagnostic_: c.Diagnostic = .{};
        var compiled_ = try c.compile(std.testing.allocator, text, .{}, &diagnostic_);
        defer compiled_.deinit();
        var fixture_: Fixture = .{};
        try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture_.backend(), graph, &compiled_, &.{}, .{}));
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [0] AS x WITH collect(1/x) AS xs RETURN CASE WHEN false THEN size(xs) ELSE 0 END", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "borrowed pagination sorts preserve stable ties and nested window lifetimes" {
    try check("UNWIND [[3,'c'],[1,'a'],[2,'b'],[1,'d']] AS pair RETURN pair[1] AS s ORDER BY pair[0] SKIP 1 LIMIT 2", "[[\"d\"],[\"b\"]]");
    try check("UNWIND [1,2,3] AS x RETURN {x:x} AS m ORDER BY x SKIP 3 LIMIT 1", "[]");
    try check("UNWIND [1,2,3] AS x RETURN {x:x} AS m ORDER BY x SKIP 2 LIMIT 1", "[[{\"x\":3}]]");
    try check("UNWIND range(1,5) AS i CALL { WITH i UNWIND [i,-i] AS x RETURN x AS v ORDER BY v SKIP 1 LIMIT 1 } RETURN collect(v)", "[[[1,2,3,4,5]]]");
    try check("UNWIND [{b:'b',a:2},{a:1,b:'c'},{b:'a',a:2}] AS x RETURN x ORDER BY x SKIP 1 LIMIT 2", "[[{\"b\":\"a\",\"a\":2}],[{\"b\":\"b\",\"a\":2}]]");
}

test "alias demand and borrowed pagination release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH(n:Document) WITH collect(n.title) AS xs WITH xs AS ys UNWIND [3,1,2] AS i RETURN size(ys) AS c,{key:i} AS k ORDER BY k SKIP 1 LIMIT 1", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 3), result.rows[0][0].integer);
            try std.testing.expectEqual(@as(i64, 2), result.rows[0][1].map[0].value.integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "bounded collection consumers preserve prefixes sharing escapes and lazy evaluation" {
    try check("UNWIND [null,3,3,1,2] AS x RETURN head(collect(x)),collect(x)[0..2],collect(x)[2]", "[[3,[3,3],1]]");
    try check("UNWIND [null,3,3,1,2] AS x RETURN head(collect(DISTINCT x)),collect(DISTINCT x)[0..2],collect(DISTINCT x)[2]", "[[3,[3,1],2]]");
    try check("UNWIND [] AS x RETURN head(collect(x)),collect(x)[0..2],collect(x)[2]", "[[null,[],null]]");
    try check("UNWIND [null,null] AS x RETURN head(collect(DISTINCT x)),collect(x)[0..0]", "[[null,[]]]");
    try check("UNWIND [3,1,2] AS x RETURN head(collect(x)),collect(x),collect(x)[-1],collect(x)[1..]", "[[3,[3,1,2],2,[1,2]]]");
    try check("UNWIND [3,1,2] AS x RETURN collect(x)[1..2],collect(x)[2..1],collect(x)[8]", "[[[1],[],null]]");
    try check("UNWIND [1,2] AS x RETURN CASE WHEN true THEN head(collect(x)) ELSE head(collect(1/0)) END", "[[1]]");
    try check("UNWIND [1,2] AS x CALL { WITH x UNWIND [x,x+1] AS y RETURN head(collect(y)) AS h } RETURN collect(h)", "[[[1,2]]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN head(xs),xs[0..1],xs[1]", "[[1,[1],2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs WITH xs AS ys RETURN head(ys)", "[[1]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,collect(x) AS ys RETURN head(xs),ys[0..2]", "[[1,[1,2]]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN head(xs),size(xs),xs", "[[1,2,[1,2]]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN head(xs),[xs IN [[7,8]]|xs[1]]", "[[1,[8]]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs WITH [7,8] AS xs RETURN xs[1]", "[[8]]");
    try check("MATCH(n:Document) RETURN n.rank%2 AS k,head(collect(n.title)) AS h ORDER BY k", "[[0,\"Beta\"],[1,\"Alpha\"]]");
    try check("MATCH(n:Document) RETURN count(DISTINCT n),head(collect(DISTINCT n)).title", "[[3,\"Alpha\"]]");
    try check("MATCH(n:Document) RETURN head(collect(DISTINCT n)).title,count(DISTINCT n)", "[[\"Alpha\",3]]");
    try check("MATCH(n:Document) RETURN count(DISTINCT [n]),head(collect(DISTINCT [n]))[0].title", "[[3,\"Alpha\"]]");
    for ([_][]const u8{
        "UNWIND [0,1] AS x RETURN head(collect(1/(1-x)))",
        "UNWIND [0,1] AS x RETURN collect(DISTINCT 1/(1-x))[0..0]",
        "UNWIND [0,1] AS x WITH collect(1/(1-x)) AS xs RETURN head(xs)",
        "UNWIND [0,1] AS x RETURN CASE WHEN true THEN head(collect(1/(1-x))) ELSE 0 END",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "bounded collect equality memberships and shared sort ownership release allocation failures" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH(n:Document) RETURN n.rank%2 AS k,count(DISTINCT n),head(collect(DISTINCT n)).title AS h ORDER BY h LIMIT 1", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "lazy bounded collect preserves intermediate value depth limits" {
    for ([_]usize{ 63, 64 }) |depth| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "MATCH(n:Document) RETURN CASE WHEN true THEN head(collect(reduce(xs=0,x IN range(1,{d})|[xs]))) ELSE null END", .{depth});
        defer std.testing.allocator.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        // COLLECT itself contributes one level before HEAD consumes it.
        if (depth == 64) {
            try std.testing.expectError(error.CypherValueDepthExceeded, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
        } else {
            var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            var value = result.rows[0][0];
            for (0..depth) |_| value = value.list[0];
            try std.testing.expectEqual(@as(i64, 0), value.integer);
        }
    }
}

test "collection suffix demands preserve order overlap DISTINCT aliases and lazy errors" {
    try check("UNWIND [null,3,1,2] AS x RETURN last(collect(x)),collect(x)[-1],collect(x)[-2..],collect(x)[-3..-1]", "[[2,2,[1,2],[3,1]]]");
    try check("UNWIND [3,1,2,3] AS x RETURN head(collect(x)),last(collect(x)),collect(x)[0..2],collect(x)[-2..],collect(x)[-8]", "[[3,3,[3,1],[2,3],null]]");
    try check("UNWIND [3,1] AS x RETURN collect(x)[0..4],collect(x)[-4..],collect(x)[-3..-1]", "[[[3,1],[3,1],[3]]]");
    try check("UNWIND [] AS x RETURN last(collect(x)),collect(x)[-1],collect(x)[-2..]", "[[null,null,[]]]");
    try check("UNWIND [1,2,1,null] AS x RETURN last(collect(DISTINCT x)),collect(DISTINCT x)[-2..],count(DISTINCT x)", "[[2,[1,2],2]]");
    try check("UNWIND [1,2,1] AS x RETURN count(DISTINCT x),last(collect(DISTINCT x)),collect(DISTINCT x)[0..1]", "[[2,2,[1]]]");
    try check("UNWIND [1,2,1] AS x RETURN last(collect(x)),collect(x),size(collect(x))", "[[1,[1,2,1],3]]");
    try check("UNWIND [1,2,3] AS x WITH collect(x) AS xs WITH xs AS ys RETURN head(ys),last(ys),ys[-2..]", "[[1,3,[2,3]]]");
    try check("UNWIND [1,2,3] AS x WITH collect(x) AS xs RETURN last(xs),size(xs),xs", "[[3,3,[1,2,3]]]");
    try check("UNWIND [1,2] AS x RETURN CASE WHEN true THEN last(collect(x)) ELSE last(collect(1/0)) END", "[[2]]");
    try check("MATCH(n:Document) RETURN count(DISTINCT n),last(collect(DISTINCT n)).title", "[[3,\"Gamma\"]]");
    try check("MATCH(n:Document) RETURN last(collect(DISTINCT [n]))[0].title,count(DISTINCT [n])", "[[\"Gamma\",3]]");
    try check("UNWIND [1,2] AS x RETURN x%2 AS k,last(collect(x)) AS last ORDER BY k", "[[0,2],[1,1]]");
    try check("UNWIND [1,2] AS g UNWIND [g,g,g+1] AS x RETURN g,CASE WHEN true THEN last(collect(DISTINCT [EXISTS { WITH x UNWIND CASE WHEN x%2=0 THEN [1] ELSE [] END AS y RETURN y }])) ELSE null END AS v ORDER BY g", "[[1,[true]],[2,[false]]]");
    for ([_][]const u8{
        "UNWIND [0,1] AS x RETURN last(collect(1/(1-x)))",
        "UNWIND [0,1] AS x RETURN collect(1/(1-x))[-1]",
        "UNWIND [0,1] AS x WITH collect(1/(1-x)) AS xs RETURN xs[-2..]",
        "UNWIND [0,1] AS x RETURN CASE WHEN true THEN last(collect(1/(1-x))) ELSE 0 END",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "grouped entity retention follows property demand without changing identities or escapes" {
    try check("MATCH(n:Document) WITH n AS a,n AS b,min(n.title) AS p RETURN a.rank,b.title,size(p) ORDER BY a.rank", "[[1,\"Alpha\",5],[2,\"Beta\",4],[3,\"Gamma\",5]]");
    try check("MATCH(n:Document) WITH n,min(n.title) AS p RETURN n.rank,size(p) ORDER BY n.rank", "[[1,5],[2,4],[3,5]]");
    try check("MATCH(n:Document) WITH n,min(n.rank) AS r WITH n AS m,r RETURN m.title,r ORDER BY r", "[[\"Alpha\",1],[\"Beta\",2],[\"Gamma\",3]]");
    try check("MATCH(n:Document) WITH n,count(*) AS c WHERE n.rank=2 RETURN n.title,c", "[[\"Beta\",1]]");
    try check("MATCH(n) WITH n,count(*) AS c RETURN n.rank,c ORDER BY n.rank", "[[1,1],[2,1],[3,1],[null,1]]");
    try check("MATCH(n) WITH n,count(*) AS c RETURN count(*)", "[[4]]");
    try check("MATCH(n:Document) WITH n,count(*) AS c RETURN properties(n).title,c ORDER BY n.rank", "[[\"Alpha\",1],[\"Beta\",1],[\"Gamma\",1]]");
    try check("MATCH(n:Document) WITH n,count(*) AS c MATCH(n:Document {rank:2}) RETURN n.title,c", "[[\"Beta\",1]]");
    try check("MATCH(n:Document) WITH n,count(*) AS c RETURN [n IN [1,2]|n+1],n.title ORDER BY n.title", "[[[2,3],\"Alpha\"],[[2,3],\"Beta\"],[[2,3],\"Gamma\"]]");
    const Authorize = struct {
        fn project(_: *anyopaque, _: std.mem.Allocator, node: *const v.Node) !?*const v.Node {
            return if ((v.Value{ .node = node }).property("title") == .null) null else node;
        }
    };
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var callbacks = backend.vtable.*;
    callbacks.project_node = Authorize.project;
    backend.vtable = &callbacks;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH(n:Document) WITH n,count(*) AS c RETURN n.rank,c", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
}

test "group finalization and rolling suffix ownership release every allocation failure" {
    const Campaign = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var vtable = allocator.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "MATCH(n:Document) UNWIND range(1,6) AS i WITH n,collect(DISTINCT n.title+toString(i))[-5..] AS p RETURN n.rank,p ORDER BY p", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 3), result.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "collection summaries combine cardinality windows and filtered counts" {
    try check("UNWIND [null,1,2,2,3] AS x WITH collect(x) AS xs RETURN size(xs),head(xs),last(xs),xs[-2..],size([y IN xs WHERE y>1])", "[[4,1,3,[2,3],3]]");
    try check("UNWIND [null,1,2,2,3] AS x RETURN size(collect(x)),last(collect(x)),size([y IN collect(x) WHERE y>1])", "[[4,3,3]]");
    try check("UNWIND [null,1,1.0,2,3] AS x WITH collect(DISTINCT x) AS xs WITH xs AS ys RETURN size(ys),head(ys),last(ys),size([y IN ys WHERE y>1 | null])", "[[3,1,3,2]]");
    try check("UNWIND [] AS x WITH collect(x) AS xs RETURN size(xs),head(xs),last(xs),size([y IN xs WHERE y>1])", "[[0,null,null,0]]");
    try check("MATCH(n:Document) WITH n.rank%2 AS k,collect(n.title) AS xs RETURN k,size(xs),last(xs),size([y IN xs WHERE y STARTS WITH 'A']) ORDER BY k", "[[0,1,\"Beta\",0],[1,2,\"Gamma\",1]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN xs,size([y IN xs WHERE y>1])", "[[[1,2],1]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN CASE WHEN true THEN 7 ELSE size([y IN xs WHERE y/0>1]) END", "[[7]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size([y IN xs WHERE y>1]),size([y IN xs WHERE y>0 | null])", "[[1,2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size([xs IN xs WHERE xs>1])", "[[1]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,1 AS k RETURN head(xs),size([y IN xs WHERE y>k])", "[[1,1]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs ORDER BY size(xs) RETURN size(xs),last(xs)", "[[2,2]]");
    try check("UNWIND [1,2] AS x RETURN CASE WHEN true THEN size([y IN collect(DISTINCT x) WHERE y>1]) ELSE 0 END,size([y IN collect(DISTINCT x) WHERE y>0])", "[[1,2]]");
}

test "collection summary consumers preserve deferred errors and argument precedence" {
    const cases = .{
        .{ "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size([y IN xs WHERE y/0>1])", error.CypherDivisionByZero },
        .{ "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size([y IN xs WHERE true | 1/0])", error.CypherDivisionByZero },
        .{ "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size([y IN xs WHERE y+true>0])", error.CypherTypeMismatch },
        .{ "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN size([y IN xs WHERE y>$missing])", error.MissingCypherParameter },
        .{ "UNWIND [1,0] AS x WITH collect(1/x) AS xs RETURN size([y IN xs WHERE y+true>0])", error.CypherDivisionByZero },
        .{ "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN 1/0,size([y IN xs WHERE y+true>0])", error.CypherDivisionByZero },
    };
    inline for (cases) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case[0], .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(case[1], runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "bounded ordered DISTINCT keeps stable complete keys and evaluates rejected inputs" {
    try check("UNWIND [3,2,3,1,2,0,3] AS x RETURN DISTINCT x ORDER BY x SKIP 1 LIMIT 2", "[[1],[2]]");
    try check("UNWIND [3,2,3,1,2,0,3] AS x RETURN DISTINCT x ORDER BY x DESC SKIP 1 LIMIT 2", "[[2],[1]]");
    try check("UNWIND [null,2,null,1] AS x RETURN DISTINCT x ORDER BY x LIMIT 3", "[[1],[2],[null]]");
    try check("UNWIND [3,2,3,1] AS x RETURN DISTINCT x,x%2 AS k ORDER BY k LIMIT 2", "[[2,0],[3,1]]");
    try check("UNWIND [3,2,1] AS x WITH DISTINCT x ORDER BY x LIMIT 2 RETURN collect(x)", "[[[1,2]]]");
    try check("UNWIND [3,2,1] AS x RETURN DISTINCT x%2 AS k,count(*) AS c ORDER BY k LIMIT 1", "[[0,1]]");
    try check("UNWIND [] AS x RETURN DISTINCT x ORDER BY x LIMIT 1", "[]");
    try check("UNWIND [3,2.0,2,1] AS x RETURN DISTINCT x ORDER BY x LIMIT 2", "[[1],[2]]");
    try check("MATCH(a:Document {id:'a'})-[:CITES*1..5]->(b:Document) RETURN DISTINCT b.rank ORDER BY b.rank LIMIT 2", "[[1],[2]]");
    var diagnostic: c.Diagnostic = .{};
    var numeric = try c.compile(std.testing.allocator, "UNWIND [3,2.0,2,1] AS x RETURN DISTINCT x ORDER BY x LIMIT 2", .{}, &diagnostic);
    defer numeric.deinit();
    var numeric_fixture: Fixture = .{};
    var numeric_result = try runtime.execute(std.testing.allocator, numeric_fixture.backend(), graph, &numeric, &.{}, .{});
    defer numeric_result.deinit();
    try std.testing.expect(numeric_result.rows[1][0] == .number);
    var compiled = try c.compile(std.testing.allocator, "UNWIND [3,2,0] AS x RETURN DISTINCT 1/x AS k ORDER BY k LIMIT 0", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "collection summaries and ordered distinct windows release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    inline for (.{
        "UNWIND [3,2,3,1,0] AS x RETURN DISTINCT x ORDER BY x LIMIT 2",
        "UNWIND [1,2,3] AS x WITH collect(x) AS xs RETURN size(xs),head(xs),last(xs),size([y IN xs WHERE y>1 | null])",
        "UNWIND [1,2] AS x WITH collect(DISTINCT x) AS xs RETURN size([y IN xs WHERE y>1]),CASE WHEN true THEN 7 ELSE size([y IN xs WHERE y/0>1]) END",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as([]const u8, text)});
}

test "invariant IN membership preserves nullable structures numeric identity and recycled rows" {
    try check("UNWIND [0,1,2,null] AS x RETURN x IN [1,null]", "[[null],[true],[null],[null]]");
    try check("UNWIND [[1],[2],[null],[1,null],[]] AS x RETURN x IN [[1],[null]]", "[[true],[null],[null],[false],[false]]");
    try check("UNWIND [{x:1},{x:2},{x:null},{y:1}] AS x RETURN x IN [{x:1},{x:null}]", "[[true],[null],[null],[false]]");
    try check("UNWIND [9007199254740992,9007199254740993] AS x RETURN x IN [9007199254740992.0]", "[[true],[false]]");
    try check("UNWIND [[0],[1],[2]] AS xs RETURN 1 IN xs", "[[false],[true],[false]]");
    try check("RETURN null IN [],null IN [1],1 IN null", "[[false,null,null]]");
    try check("MATCH(n:Document) RETURN n IN [n]", "[[true],[true],[true]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [$a,$b] AS xs UNWIND [0,2] AS x RETURN x IN xs", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{ .{ .name = "a", .value = .{ .list = &.{.{ .integer = 0 }} } }, .{ .name = "b", .value = .{ .list = &.{.{ .integer = 2 }} } } }, .{});
    defer result.deinit();
    for (result.rows, [_]bool{ true, false, false, true }) |row, expected| try std.testing.expectEqual(expected, row[0].boolean);
}

test "invariant IN membership has bounded work through parameter aliases" {
    var values: [512]v.Value = undefined;
    for (&values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
    for ([_][]const u8{
        "UNWIND range(0,2999) AS x RETURN sum(CASE WHEN x IN $xs THEN 1 ELSE 0 END)",
        "WITH $xs AS ys UNWIND range(0,2999) AS x RETURN sum(CASE WHEN x IN ys THEN 1 ELSE 0 END)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &values } }}, .{ .intermediate_states = 200_000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 512), result.rows[0][0].integer);
    }
}

test "parameter collection bounds specialize each execution without mutating prepared AST" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [1,2,3] AS x WITH collect(x) AS xs RETURN xs[$i],xs[..$k],size(xs)", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_]i64{ 0, -1, 20 }) |index| {
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{ .{ .name = "i", .value = .{ .integer = index } }, .{ .name = "k", .value = .{ .integer = 1 } } }, .{});
        defer result.deinit();
        if (index == 20) try std.testing.expect(result.rows[0][0] == .null) else try std.testing.expectEqual(@as(i64, if (index == 0) 1 else 3), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, 1), result.rows[0][1].list[0].integer);
        try std.testing.expectEqual(@as(i64, 3), result.rows[0][2].integer);
    }
    const bound = compiled.query.clauses[2].operation.project.value.items[0].expression.value.index.index;
    try std.testing.expect(bound.execution_integer == null);
    var fixture: Fixture = .{};
    var null_result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{ .{ .name = "i", .value = .null }, .{ .name = "k", .value = .{ .integer = 1 } } }, .{});
    defer null_result.deinit();
    try std.testing.expect(null_result.rows[0][0] == .null);
    try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{ .{ .name = "i", .value = .{ .string = "bad" } }, .{ .name = "k", .value = .{ .integer = 1 } } }, .{}));
}

test "boolean collection summaries preserve three valued results short circuiting and lazy errors" {
    try check("UNWIND [] AS x WITH collect(x) AS xs RETURN all(y IN xs WHERE true),any(y IN xs WHERE true),none(y IN xs WHERE true),single(y IN xs WHERE true)", "[[true,false,true,false]]");
    try check("UNWIND [0,1] AS x WITH collect(x) AS xs RETURN all(y IN xs WHERE CASE WHEN y=0 THEN null ELSE true END),any(y IN xs WHERE CASE WHEN y=0 THEN null ELSE true END),none(y IN xs WHERE CASE WHEN y=0 THEN null ELSE true END),single(y IN xs WHERE CASE WHEN y=0 THEN null ELSE true END)", "[[null,true,false,null]]");
    try check("UNWIND [1,0] AS x WITH collect(x) AS xs RETURN any(y IN xs WHERE 1/y>0),none(y IN xs WHERE 1/y>0)", "[[true,false]]");
    try check("UNWIND [-1,0] AS x WITH collect(x) AS xs RETURN all(y IN xs WHERE 1/y>0)", "[[false]]");
    try check("UNWIND [1,1,0] AS x RETURN single(y IN collect(x) WHERE 1/y>0)", "[[false]]");
    try check("UNWIND [0,1] AS x WITH collect(x) AS xs RETURN CASE WHEN true THEN 7 ELSE any(y IN xs WHERE 1/y>0) END", "[[7]]");
    try check("UNWIND [1,1,2,null] AS x WITH collect(DISTINCT x) AS xs WITH xs AS ys RETURN size(ys),head(ys),last(ys),single(y IN ys WHERE y=1),size([y IN ys WHERE y>1])", "[[2,1,2,true,1]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,1 AS k RETURN any(y IN xs WHERE y>k),xs", "[[true,[1,2]]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [0,1] AS x WITH collect(x) AS xs RETURN any(y IN xs WHERE 1/y>0)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "invariant membership preserves shared value DAGs and cancellation" {
    try check("RETURN 0 IN [reduce(a=[1],x IN range(1,24)|[a,a])]", "[[false]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,1000) AS x RETURN x IN range(1,1000)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{ .cancel_after = 10 };
    try std.testing.expectError(error.Canceled, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "invariant membership bound specialization and boolean summaries release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, kind: u1) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            const wide: [512]u8 = @splat('x');
            const parameters = [_]v.Entry{
                .{ .name = "xs", .value = .{ .list = &.{ .{ .string = &wide }, .null, .{ .list = &.{.null} } } } },
                .{ .name = "word", .value = .{ .string = &wide } },
                .{ .name = "i", .value = .{ .integer = -1 } },
            };
            var diagnostic: c.Diagnostic = .{};
            const text = if (kind == 0)
                "WITH $xs AS ys UNWIND [$word,'other'] AS x RETURN x IN $xs,x IN ys"
            else
                "UNWIND [1,1,2,null] AS x WITH collect(DISTINCT x) AS xs RETURN xs[$i],size(xs),any(y IN xs WHERE y>1),all(y IN xs WHERE y>0),single(y IN xs WHERE y=2)";
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &parameters, .{});
            defer result.deinit();
        }
    };
    inline for (.{ @as(u1, 0), @as(u1, 1) }) |kind| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{kind});
}

test "streaming comprehension windows preserve mapping filtering nulls and consumer bounds" {
    try check("UNWIND [1,2,3,4,null] AS x WITH collect(x) AS xs RETURN head([y IN xs | y*2]),last([y IN xs WHERE y%2=0 | y+10]),[y IN xs WHERE y>1 | y*2][1..2],[y IN xs | null][-1],size(xs)", "[[2,14,[6],null,4]]");
    try check("UNWIND [1,2,3,4] AS x RETURN [y IN collect(x) WHERE y>1 | y*2][-2..-1]", "[[[6]]]");
    try check("UNWIND [] AS x WITH collect(x) AS xs RETURN head([y IN xs | y]),last([y IN xs | y]),[y IN xs | y][..2]", "[[null,null,[]]]");
    try check("UNWIND [1,1,2] AS x WITH collect(DISTINCT x) AS xs RETURN head([y IN xs | y]),last([y IN xs | y]),size(xs)", "[[1,2,2]]");
    // A full escape and correlated projection keep their materialized behavior.
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs RETURN xs,head([y IN xs | y+1])", "[[[1,2],2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,10 AS k RETURN head([y IN xs | y+k])", "[[11]]");
    try check("UNWIND [1,0] AS x WITH collect(x) AS xs RETURN CASE WHEN true THEN 7 ELSE head([y IN xs | 1/y]) END", "[[7]]");
    try check("UNWIND [1,0] AS x WITH collect(x) AS xs RETURN CASE WHEN true THEN 7 ELSE [y IN xs | 1/y][..0] END", "[[7]]");
}

test "streaming folds preserve sequential transitions ownership empty inputs and null exclusion" {
    try check("UNWIND [1,2,3,null] AS x WITH collect(x) AS xs RETURN reduce(s=10,y IN xs | s-y),reduce(s=[],y IN xs | s+[y]),reduce(s={v:0},y IN xs | {v:s.v+y}),size(xs)", "[[4,[1,2,3],{\"v\":6},3]]");
    try check("UNWIND [1,2] AS x RETURN reduce(s=0,y IN collect(x) | s*10+y)", "[[12]]");
    try check("UNWIND [] AS x WITH collect(x) AS xs RETURN reduce(s=[9],y IN xs | s+[y])", "[[[9]]]");
    try check("UNWIND [null,null] AS x RETURN reduce(s=5,y IN collect(x) | s+y)", "[[5]]");
    try check("UNWIND [1,1,2] AS x WITH collect(DISTINCT x) AS xs RETURN reduce(s=0,y IN xs | s*10+y),head(xs),last(xs)", "[[12,1,2]]");
    try check("UNWIND [1,2] AS x WITH collect(x) AS xs,10 AS k RETURN reduce(s=k,y IN xs | s+y)", "[[13]]");
    try check("UNWIND [1,0] AS x WITH collect(x) AS xs RETURN CASE WHEN true THEN 7 ELSE reduce(s=1,y IN xs | s/y) END", "[[7]]");
    try check("UNWIND [] AS x WITH collect(x) AS xs RETURN CASE WHEN true THEN 7 ELSE reduce(s=1/0,y IN xs | s+y) END", "[[7]]");
    // Source argument errors precede any deferred consumer error.
    const Case = struct { text: []const u8, err: anyerror };
    for ([_]Case{
        .{ .text = "UNWIND [1,0] AS x WITH collect(x) AS xs RETURN head([y IN xs | 1/y])", .err = error.CypherDivisionByZero },
        .{ .text = "UNWIND [1,0] AS x WITH collect(x) AS xs RETURN [y IN xs | 1/y][..0]", .err = error.CypherDivisionByZero },
        .{ .text = "UNWIND [] AS x WITH collect(x) AS xs RETURN reduce(s=1/0,y IN xs | s+y)", .err = error.CypherDivisionByZero },
        .{ .text = "UNWIND [1,0] AS x WITH collect(1/x) AS xs RETURN reduce(s=1+'bad',y IN xs | s+y)", .err = error.CypherDivisionByZero },
        .{ .text = "UNWIND [1,-1] AS x WITH collect(x) AS xs RETURN reduce(s=9223372036854775807,y IN xs | s+y)", .err = error.CypherNumericOverflow },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "streaming windows and fold state release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, "UNWIND ['ab','cd'] AS x WITH collect(x) AS xs RETURN head([y IN xs | y+'!']),last([y IN xs | y+'?']),reduce(s={v:''},y IN xs | {v:s.v+y})", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "streaming consumers keep physical summaries outside public value depth" {
    var nested: [65][1]v.Value = undefined;
    nested[0][0] = .{ .integer = 1 };
    for (1..nested.len) |i| nested[i][0] = .{ .list = &nested[i - 1] };
    const Case = struct { text: []const u8, value: v.Value, failure: ?anyerror = null };
    for ([_]Case{
        .{ .text = "UNWIND [1] AS x WITH collect(x) AS xs RETURN size(reduce(s=$deep,y IN xs | s))", .value = nested[64][0] },
        .{ .text = "UNWIND [1] AS x WITH collect(x) AS xs WITH xs AS ys RETURN size(reduce(s=$deep,y IN ys | s).nested)", .value = .{ .map = &.{.{ .name = "nested", .value = nested[63][0] }} } },
        .{ .text = "UNWIND [1] AS x WITH collect(x) AS xs RETURN size(head([y IN xs | $deep]))", .value = nested[63][0] },
        .{ .text = "UNWIND [1,2] AS x WITH collect(x) AS xs RETURN head([y IN xs | CASE WHEN y=1 THEN 7 ELSE $deep END])", .value = nested[64][0], .failure = error.CypherValueDepthExceeded },
        .{ .text = "UNWIND [1,2,0] AS x WITH collect(x) AS xs RETURN head([y IN xs | CASE WHEN y=2 THEN $deep ELSE 1/y END])", .value = nested[64][0], .failure = error.CypherDivisionByZero },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        const parameters = &.{v.Entry{ .name = "deep", .value = case.value }};
        if (case.failure) |failure| {
            try std.testing.expectError(failure, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, parameters, .{}));
        } else {
            var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, parameters, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
        }
    }
}

test "streaming consumer plans specialize parameters independently for repeated prepared execution" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [1,2,3] AS x WITH collect(x) AS xs RETURN reduce(s=$initial,y IN xs | s+y),head([y IN xs WHERE y>=$k | y*10]),[y IN xs | y][$i]", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_]struct { initial: i64, k: i64, i: i64 }{
        .{ .initial = 10, .k = 2, .i = -1 },
        .{ .initial = -10, .k = 4, .i = 20 },
        .{ .initial = 0, .k = 0, .i = 0 },
    }) |case| {
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{
            .{ .name = "initial", .value = .{ .integer = case.initial } },
            .{ .name = "k", .value = .{ .integer = case.k } },
            .{ .name = "i", .value = .{ .integer = case.i } },
        }, .{});
        defer result.deinit();
        try std.testing.expectEqual(case.initial + 6, result.rows[0][0].integer);
        if (case.k == 4) {
            try std.testing.expect(result.rows[0][1] == .null and result.rows[0][2] == .null);
        } else {
            try std.testing.expectEqual(@as(i64, if (case.k == 2) 20 else 10), result.rows[0][1].integer);
            try std.testing.expectEqual(@as(i64, if (case.i == -1) 3 else 1), result.rows[0][2].integer);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), compiled.query.clauses[1].operation.project.value.items[0].expression.value.call.collect_filters.len);
}

test "bounded pattern consumers agree with materialization and keep projection error precedence" {
    try check("MATCH(a:Document {id:'a'}) RETURN head([(a)-[r:CITES]->(b)|r.weight]),last([(a)-[r:CITES]->(b)|r.weight]),[(a)-[r:CITES]->(b)|r.weight][-2..-1]", "[[1,2,[1]]]");
    try check("MATCH(a:Document {id:'a'}) RETURN head([(a)-[:CITES]->(b)|b.title]),last([(a)-[:CITES]->(b)|b.title]),size([(a)-[:CITES]->(b)|null])", "[[\"Beta\",\"Beta\",2]]");
    try check("MATCH(a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b)|b.rank][-2..-1],[(a)-[:CITES]->(b)|null][0],size([(a)<-[:CITES]-(b)|b])", "[[[2],null,1]]");
    try check("MATCH(a:Document {id:'a'}) RETURN size([(a)-[:CITES*1..3]->(b)|null])", "[[8]]");
    try check("MATCH(a:Document {id:'a'}) RETURN CASE WHEN true THEN 7 ELSE head([(a)-[:CITES]->(b)|1/0]) END", "[[7]]");
    var diagnostic: c.Diagnostic = .{};
    for ([_][]const u8{
        "MATCH(a:Document {id:'a'}) RETURN head([(a)-[:CITES]->(b)|1/0])",
        "MATCH(a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b)|1/0][..0]",
        "MATCH(a:Document {id:'a'}) RETURN size([(a)-[:CITES]->(b)|1/0])",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    for ([_][]const u8{
        "MATCH(a:Document {id:'a'}) RETURN head([(a)-[r:CITES]->(b) WHERE CASE WHEN r.weight=1 THEN true ELSE 1/0>0 END | true+1])",
        "MATCH(a:Document {id:'a'}) RETURN size([(a)-[r:CITES]->(b) WHERE CASE WHEN r.weight=1 THEN true ELSE 1/0>0 END | true+1])",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "range consumers are bounded and preserve dynamic null errors and inclusive arithmetic" {
    try check("RETURN head(range(1,300000)),last(range(1,300000)),size(range(1,300000)),range(1,300000)[100..102]", "[[1,300000,300000,[101,102]]]");
    try check("RETURN range(5,-5,-3)[-1],range(5,-5,-3)[-2..],range(5,-5,-3)[4],range(5,-5,-3)[8..1]", "[[-4,[-1,-4],null,[]]]");
    try check("RETURN head(range(2,1)),last(range(null,1)),size(range(1,null)),range(1,3)[null],range(null,3)[1..2]", "[[null,null,null,null,null]]");
    try check("RETURN head(range(-9223372036854775808,9223372036854775807)),last(range(-9223372036854775808,9223372036854775807)),range(-9223372036854775808,9223372036854775807)[-1..]", "[[-9223372036854775808,9223372036854775807,[9223372036854775807]]]");
    const Case = struct { query: []const u8, err: anyerror };
    for ([_]Case{
        .{ .query = "RETURN head(range(1,2,0))", .err = error.CypherInvalidRange },
        .{ .query = "RETURN range(null,2)[1/0]", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN range(null,2)[1/0..2]", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN range(1,2)[true]", .err = error.CypherTypeMismatch },
        .{ .query = "RETURN size(range(-9223372036854775808,9223372036854775807))", .err = error.CypherNumericOverflow },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "scalar invariants and general folds use independent ownership and preserve lazy errors" {
    try check("RETURN size(reduce(s='',x IN range(1,5000)|s+'x'))", "[[5000]]");
    try check("WITH 10 AS k RETURN reduce(s=0,x IN [1,2,3] | s+x+k)", "[[36]]");
    try check("RETURN reduce(s=[1],x IN [2,3] | [s,s])=reduce(s=[1],x IN [2,3] | [s,s])", "[[true]]");
}

test "invariant scalar results are lazy and owned independently for each prepared invocation" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,1000) AS x WITH collect(x) AS xs RETURN reduce(s=0,y IN xs|s+size(toLower($word))),CASE WHEN $safe THEN 7 ELSE size(toLower(1/0)) END", .{}, &diagnostic);
    defer compiled.deinit();
    const wide: [16384]u8 = @splat('A');
    for ([_][]const u8{ &wide, "BcD", &wide }) |word| {
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{
            .{ .name = "word", .value = .{ .string = word } },
            .{ .name = "safe", .value = .{ .boolean = true } },
        }, .{ .intermediate_states = 50000, .memory_bytes = 8 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(word.len * 1000)), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, 7), result.rows[0][1].integer);
    }
    try std.testing.expect(compiled.query.clauses[2].operation.project.value.items[0].expression.execution_invariant == null);
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{
        .{ .name = "word", .value = .{ .string = "x" } },
        .{ .name = "safe", .value = .{ .boolean = false } },
    }, .{}));
}

test "range descriptors generic folds and pattern sinks release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    inline for (.{
        "RETURN reduce(s={v:'start'},x IN range(1,3)|{v:s.v+'!'}),range(1,300000)[-3..-1]",
        "MATCH(a:Document {id:'a'}) RETURN head([(a)-[:CITES]->(b)|b.title]),size([(a)-[:CITES*1..3]->(b)|null])",
        "RETURN size(toLower('AbC')),CASE WHEN true THEN 7 ELSE size(toLower(1/0)) END",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as([]const u8, text)});
}

test "immutable string aliases retain analysis and borrowed fold results escape safely" {
    const word: [400000]u8 = @splat('X');
    for ([_][]const u8{
        "WITH $word AS text RETURN reduce(s=0,i IN range(1,1000)|s+size(ltrim(text)))",
        "WITH $word AS text RETURN reduce(s=0,i IN range(1,1000)|s+size(toLower(text)))",
        "RETURN size(reduce(s=$word,i IN range(1,1000)|ltrim(s)))*1000",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "word", .value = .{ .string = &word } }}, .{ .intermediate_states = 50000, .memory_bytes = 8 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 400000000), result.rows[0][0].integer);
    }
    try check("RETURN reduce(s=0,i IN range(1,3)|'done'),reduce(s=[],i IN [1,2]|[3,4])", "[[\"done\",[3,4]]]");
}

test "ordinary comprehension demands preserve order nulls shadowing and eager errors" {
    try check("WITH reduce(s=[0],i IN range(1,30)|[s,s]) AS deep RETURN size(head([x IN [1]|deep]))", "[[2]]");
    try check("RETURN [x IN range(5,-5,-3)|x+1],size([x IN range(5,-5,-3) WHERE x<3|null])", "[[[6,3,0,-3],3]]");
    try check("WITH 9 AS x RETURN [x IN [1,2]|[x IN [x+1]|x]],x", "[[[[2],[3]],9]]");
    try check("RETURN head([x IN [1,2,3]|null]),last([x IN [1,2,3]|x]),[x IN range(1,9) WHERE x%2=1|x][-3..-1],[x IN range(1,9)|x][2..4]", "[[null,3,[5,7],[3,4]]]");
    try check("RETURN head([x IN null|x]),[x IN null|x][null],[x IN null|x][null..2],size([x IN null|x])", "[[null,null,null,null]]");
    try check("RETURN any(x IN [null,false] WHERE x),all(x IN [null,true] WHERE x),none(x IN [] WHERE x),single(x IN [null,true] WHERE x)", "[[null,null,true,null]]");
    try check("RETURN any(x IN range(-9223372036854775808,9223372036854775807) WHERE x=-9223372036854775808),all(x IN range(3,1) WHERE false),reduce(s=7,x IN range(3,1)|s+x)", "[[true,true,7]]");
    const Case = struct { query: []const u8, err: anyerror };
    for ([_]Case{
        .{ .query = "RETURN head([x IN [1,0]|1/x])", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN size([x IN [1,0]|1/x])", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN [x IN [1,0]|1/x][0..0]", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN [x IN [1,0]|1/x][true]", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN [x IN null|x][1/0]", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN [x IN null|x][1/0..2]", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN any(x IN [y IN [0]|1/y] WHERE true)", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN any(x IN range(1,2,0) WHERE true)", .err = error.CypherInvalidRange },
        .{ .query = "RETURN reduce(s=1/0,x IN range(1,2,0)|s+x)", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN size([x IN 1|x])", .err = error.CypherTypeMismatch },
        .{ .query = "WITH reduce(s=0,i IN range(1,64)|[s]) AS deep RETURN head([x IN [1]|deep])", .err = error.CypherValueDepthExceeded },
        .{ .query = "WITH reduce(s=0,i IN range(1,64)|[s]) AS deep RETURN size([x IN [1]|deep])", .err = error.CypherValueDepthExceeded },
        .{ .query = "WITH reduce(s=0,i IN range(1,64)|[s]) AS deep RETURN head([x IN [1,0]|CASE WHEN x=1 THEN deep ELSE 1/x END])", .err = error.CypherDivisionByZero },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "ordinary comprehension scopes and list sources release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            // In-place arena growth depends on backing allocation layout.
            // Force growth through fallible allocations so every injected
            // failure index refers to the same operation on every run.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    inline for (.{
        "RETURN [x IN range(1,3)|{a:toString(x)}],head([x IN range(1,3)|{a:toString(x)}]),[x IN range(1,4)|toString(x)][-2..]",
        "RETURN any(x IN range(1,3) WHERE toString(x)='3'),reduce(s='',x IN range(1,3)|s+toString(x)),size([x IN range(1,3) WHERE x>1|null])",
        "WITH 3 AS n WITH [x IN range(1,n)|toString(x)] AS xs,n AS n WITH xs AS ys,n+1 AS n UNWIND reverse(ys) AS x RETURN n,collect(x)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as([]const u8, text)});
}

test "projected list producers compose summaries aliases nulls and lexical scopes" {
    try check("WITH [x IN [1,null,2]|x] AS xs WITH xs AS ys RETURN size(ys),head(ys),last(ys),ys[-2..],any(x IN ys WHERE x=2),reduce(s=0,x IN ys|s+coalesce(x,0))", "[[3,1,2,[null,2],true,3]]");
    try check("WITH [x IN null|x] AS xs RETURN size(xs),head(xs),last(xs),any(x IN xs WHERE true),reduce(s=9,x IN xs|s+x)", "[[null,null,null,null,null]]");
    try check("WITH [x IN []|x] AS xs RETURN size(xs),head(xs),any(x IN xs WHERE true),reduce(s=9,x IN xs|s+x)", "[[0,null,false,9]]");
    try check("WITH range(-9223372036854775808,9223372036854775807) AS xs RETURN any(x IN xs WHERE x=-9223372036854775808),all(x IN xs WHERE x>0),last(xs)", "[[true,false,9223372036854775807]]");
    try check("WITH range(-9223372036854775808,9223372036854775807) AS xs RETURN head(xs),last(xs),xs[-2..]", "[[-9223372036854775808,9223372036854775807,[9223372036854775806,9223372036854775807]]]");
    try check("WITH [x IN range(1,3)|x*2] AS xs RETURN xs,head(xs),size(xs)", "[[[2,4,6],2,3]]");
    try check("WITH [x IN range(1,3)|x*2] AS xs WITH xs,xs AS ys RETURN head(xs),last(ys)", "[[2,6]]");
    try check("WITH 7 AS k RETURN reduce(s=0,x IN [y IN [1,2,3]|y*2]|s+x+k),any(x IN [y IN [1,2]|y] WHERE x=k)", "[[33,false]]");
    try check("WITH 9 AS y RETURN head([x IN [y IN [1,2]|y+1]|x+y]),y", "[[11,9]]");
    try check("RETURN any(x IN [y IN [null,false]|y] WHERE x),single(x IN [y IN [true,true]|y] WHERE x)", "[[null,false]]");
    try check("MATCH(a:Document {id:'a'}) WITH [(a)-[:CITES]->(b)|b.rank] AS xs RETURN size(xs),head(xs),last(xs),reduce(s=0,x IN xs|s+x)", "[[2,2,2,4]]");
    try check("MATCH(a:Document {id:'a'}) RETURN any(x IN [(a)-[:CITES]->(b)|b.rank] WHERE x=2),reduce(s=0,x IN [(a)-[:CITES]->(b)|b.rank]|s+x)", "[[true,4]]");
    try check("MATCH(a:Document {id:'a'}) RETURN head([x IN [(a)-[:CITES]->(b)|b.rank]|x+1])", "[[3]]");
    try check("WITH [x IN [1,2]|x] AS xs RETURN CASE WHEN false THEN reduce(s=1/0,x IN xs|s+x) ELSE 7 END", "[[7]]");
}

test "projected list phases preserve source consumer initial and late depth failures" {
    const Case = struct { query: []const u8, err: anyerror };
    for ([_]Case{
        .{ .query = "RETURN reduce(s=1/0,x IN [y IN [1]|true+1]|s+x)", .err = error.CypherDivisionByZero },
        .{ .query = "WITH [y IN [1]|true+1] AS xs RETURN reduce(s=1/0,x IN xs|s+x)", .err = error.CypherTypeMismatch },
        .{ .query = "RETURN any(x IN [y IN [1,0]|1/y] WHERE true)", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN any(x IN [y IN [1,0]|1/y] WHERE true+1)", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN head([x IN [y IN [1,0]|1/y]|true+1])", .err = error.CypherDivisionByZero },
        .{ .query = "RETURN reduce(s=0,x IN [y IN [1,0]|1/y]|true+1)", .err = error.CypherDivisionByZero },
        .{ .query = "WITH [x IN null|x] AS xs RETURN reduce(s=1/0,x IN xs|s+x)", .err = error.CypherDivisionByZero },
        .{ .query = "WITH reduce(s=0,i IN range(1,64)|[s]) AS deep RETURN any(x IN [y IN [1]|deep] WHERE true)", .err = error.CypherValueDepthExceeded },
        .{ .query = "WITH reduce(s=0,i IN range(1,64)|[s]) AS deep,[x IN [1]|x] AS xs RETURN size([x IN xs|deep])", .err = error.CypherValueDepthExceeded },
        .{ .query = "MATCH(a:Document {id:'a'}) RETURN any(x IN [(a)-[r:CITES]->(b) WHERE CASE WHEN r.weight=1 THEN true ELSE 1/0>0 END|true+1] WHERE true)", .err = error.CypherDivisionByZero },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(case.err, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "scalar row seeds stream UNWIND aggregates windows and intermediate barriers" {
    try check("UNWIND null AS x RETURN count(*)", "[[0]]");
    try check("UNWIND 7 AS x RETURN x", "[[7]]");
    try check("UNWIND range(1,3) AS x UNWIND [x,x] AS y RETURN y ORDER BY y DESC LIMIT 3", "[[3],[3],[2]]");
    try check("UNWIND range(1,4) AS x WITH x ORDER BY x DESC LIMIT 2 UNWIND [x,x] AS y RETURN sum(y),count(*)", "[[14,4]]");
    try check("UNWIND [2,2,1] AS x WITH DISTINCT x RETURN sum(x),count(*)", "[[3,2]]");
    try check("UNWIND range(1,3) AS x RETURN x,count(*) ORDER BY x", "[[1,1],[2,1],[3,1]]");
    try check("UNWIND range(1,3) AS x UNWIND [1,2] AS y RETURN collect(y)", "[[[1,2,1,2,1,2]]]");
    try check("UNWIND [v IN range(1,3)|v] AS x UNWIND [1,2] AS y RETURN collect(y)", "[[[1,2,1,2,1,2]]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,300000) AS x RETURN x LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
}

test "producer summaries and scalar seeds release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    inline for (.{
        "WITH [x IN range(1,3)|toString(x)] AS xs RETURN head(xs),last(xs),size(xs),reduce(s='',x IN xs|s+x)",
        "RETURN reduce(s='',x IN [y IN range(1,3)|toString(y)]|s+x),head([x IN [y IN range(1,3)|toString(y)]|x])",
        "MATCH(a:Document {id:'a'}) RETURN any(x IN [(a)-[:CITES]->(b)|b.title] WHERE size(x)>1),reduce(s='',x IN [(a)-[:CITES]->(b)|b.title]|s+x)",
        "UNWIND range(1,3) AS x UNWIND [x,x] AS y RETURN y,count(*) ORDER BY y DESC LIMIT 2",
        "UNWIND [1,0] AS x RETURN CASE WHEN true THEN count(*) ELSE sum(1/x) END",
        "UNWIND [y IN range(1,3)|toString(y)] AS x RETURN collect(x)",
        "WITH reverse([x IN range(1,3)|toString(x)]) AS xs RETURN head(xs),last(xs),size(xs)",
        "UNWIND range(1,3) AS x RETURN EXISTS { MATCH(n:Document) WHERE n.rank=x },head([(a:Document {rank:x})-[:CITES]->(b)|b.title]) ORDER BY x DESC LIMIT 2",
        "UNWIND range(1,1000000000) AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs WITH xs AS ys UNWIND ys AS x RETURN count(x),count(DISTINCT x)",
        "WITH range(1,1000000000) AS xs WITH xs AS ys UNWIND ys AS x RETURN count(*),count(1)",
        "WITH [x IN range(1,3)|toString(x)]+[4] AS xs RETURN head(xs),last(xs),size(xs)",
        "UNWIND [1,2,3] AS n RETURN n IN [x IN [1,null]|x]",
        "WITH range(1,1000000000) AS xs WITH xs AS ys RETURN 1 IN ys,head(ys)",
        "UNWIND [[1],[1.0],[1]] AS ys UNWIND [1,2,3] AS n RETURN 0 IN [x IN ys|x/2]",
        "UNWIND reverse([x IN [1,2,3]|toString(x)]) AS x RETURN collect(x)",
        "UNWIND [1,2] AS n RETURN n IN ([x IN [1,2,3]|x]+[4])",
        "WITH range(1,3) AS xs,toString(4) AS seed UNWIND xs AS x RETURN collect(x),min(seed)",
        "MATCH(a:Document {id:'a'}) UNWIND [1,2,3] AS n RETURN 'Beta' IN [(a)-[:CITES]->(b)|b.title]",
        "MATCH(a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b)|[(b)-[:CITES]->(d)|d.title]]",
        "WITH 3 AS n WITH range(1,n) AS xs,9 AS n WITH (xs+[n]) AS ys,n+1 AS n UNWIND (reverse(ys)+[n]) AS x RETURN n,collect(x)",
        "UNWIND [[1,null,2],[]] AS xs RETURN head(reverse(xs)),reverse(xs)[1..2],size(reverse(xs))",
        "WITH [x IN [1,2,3]|toString(x)] AS xs,1 AS seed WITH xs AS ys,seed AS seed UNWIND (ys+ys) AS x RETURN collect(x)",
        "WITH range(1,3) AS xs UNWIND (xs+range(4,6)) AS x RETURN count(*)",
        "UNWIND [1,1.0,1] AS x RETURN {k:[x]},[x/2]",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{@as([]const u8, text)});
}

test "direct range consumers retain sparse work alongside generic producer plans" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "RETURN head(range(1,300000)),last(range(1,300000)),size(range(1,300000)),range(1,300000)[100..102],range(1,300000)[-1]", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 1000, .memory_bytes = 1 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 300000), result.rows[0][2].integer);
    try std.testing.expectEqual(@as(usize, 2), result.rows[0][3].list.len);
}

test "streamed UNWIND producers preserve eager phases nulls lexical scope and bags" {
    try check("UNWIND [y IN range(1,3)|y*2] AS x RETURN x ORDER BY x DESC", "[[6],[4],[2]]");
    try check("UNWIND [y IN null|y] AS x RETURN count(*)", "[[0]]");
    try check("UNWIND [y IN [1,null,2]|y] AS x RETURN count(*),count(x)", "[[3,2]]");
    try check("WITH 9 AS y UNWIND [y IN range(1,3)|y] AS x RETURN y,sum(x)", "[[9,6]]");
    try check("MATCH(a:Document {id:'a'}) UNWIND [(a)-[:CITES]->(b)|b.title] AS title RETURN title,count(*) ORDER BY title", "[[\"Beta\",2]]");
    try check("UNWIND [y IN range(1,3)|toString(y)] AS x UNWIND [x,x] AS y RETURN reduce(s='',v IN collect(y)|s+v)", "[[\"112233\"]]");
    for ([_][]const u8{
        "UNWIND [y IN [1,0]|1/y] AS x RETURN x LIMIT 1",
        "UNWIND [y IN [1,0]|1/y] AS x RETURN x+'bad' LIMIT 1",
        "UNWIND [y IN [1,0]|1/y] AS x RETURN count(*)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "list reversal propagates windows cardinality aliases and preserves string overload" {
    try check("RETURN head(reverse([x IN range(1,5)|x])),last(reverse([x IN range(1,5)|x])),reverse([x IN range(1,5)|x])[1..3],reverse([x IN range(1,5)|x])[-2..]", "[[5,1,[4,3],[2,1]]]");
    try check("WITH reverse([x IN range(1,5)|x]) AS xs WITH xs AS ys RETURN head(ys),last(ys),size(ys),ys[1..3],ys[-2..]", "[[5,1,5,[4,3],[2,1]]]");
    try check("RETURN head(reverse(reverse([x IN range(1,5)|x]))),size(reverse([x IN [null,1]|x])),size(reverse([x IN []|x]))", "[[1,2,0]]");
    try check("RETURN head(reverse([x IN null|x])),size(reverse([x IN null|x])),reverse([x IN null|x])[0],reverse([x IN null|x])[0..1]", "[[null,null,null,null]]");
    try check("RETURN reverse('abc'),head(reverse([null,2])),reduce(s='',x IN reverse([y IN range(1,3)|toString(y)])|s+x)", "[[\"cba\",2,\"321\"]]");
    try check("WITH reverse([x IN range(1,3)|x]) AS xs RETURN head(xs),reduce(s='',x IN xs|s+toString(x))", "[[3,\"321\"]]");
    try check("MATCH(a:Document {id:'a'}) RETURN head(reverse([(a)-[:CITES]->(b)|b.id])),size(reverse([(a)-[:CITES]->(b)|b.id]))", "[[\"b\",2]]");
}

test "graph expression consumers preserve outer streaming scopes and owned results" {
    try check("MATCH(a:Document {id:'a'}) RETURN [(a)-[:CITES]->(b)|[(b)-[:CITES]->(d)|d.title]]", "[[[[\"Alpha\",\"Gamma\"],[\"Alpha\",\"Gamma\"]]]]");
    try check("UNWIND range(1,3) AS x RETURN EXISTS { MATCH(n:Document) WHERE n.rank=x },x ORDER BY x DESC LIMIT 2", "[[true,3],[true,2]]");
    try check("UNWIND range(1,3) AS x WITH x,EXISTS { MATCH(n:Document) WHERE n.rank=x } AS ok WHERE ok RETURN sum(x)", "[[6]]");
    try check("UNWIND range(1,3) AS x RETURN head([(n:Document {rank:x})-[:CITES]->(b)|b.id]),x ORDER BY x DESC LIMIT 2", "[[null,3],[\"a\",2]]");
    try check("UNWIND range(1,3) AS x RETURN size([(n:Document {rank:x})-[:CITES]->(b)|b]),count(*) ORDER BY size([(n:Document {rank:x})-[:CITES]->(b)|b])", "[[0,1],[2,2]]");
    try check("MATCH(a:Document) RETURN EXISTS { MATCH(b:Document) WHERE b.id=a.id },a.id ORDER BY a.id LIMIT 2", "[[true,\"a\"],[true,\"b\"]]");
    try check("UNWIND range(1,3) AS x UNWIND [(n:Document {rank:x})-[:CITES]->(b)|b.id] AS id RETURN collect(id)", "[[[\"b\",\"b\",\"a\",\"c\"]]]");
}

test "unobserved range axes reduce arithmetic multiplicity with checked lazy results" {
    try check("UNWIND range(1,0) AS x RETURN count(*)", "[[0]]");
    try check("UNWIND range(3,1,-1) AS x RETURN count(*),count(1),count(null),count(DISTINCT 1),sum(2)", "[[3,3,0,1,6]]");
    try check("UNWIND range(1,3) AS x WITH DISTINCT 7 AS y RETURN count(*)", "[[1]]");
    try check("UNWIND range(1,3) AS x RETURN x,count(*) ORDER BY x", "[[1,1],[2,1],[3,1]]");
    try check("UNWIND range(1,3) AS x UNWIND [1,2] AS y RETURN collect(y)", "[[[1,2,1,2,1,2]]]");
    try check("UNWIND [v IN range(1,3)|v] AS x UNWIND [1,2] AS y RETURN collect(y)", "[[[1,2,1,2,1,2]]]");
    for ([_][]const u8{
        "UNWIND range(1,1000000000) AS x RETURN count(*)",
        "UNWIND range(1,1000) AS x UNWIND range(1,1000000) AS y RETURN count(*)",
        "RETURN head(reverse(range(-9223372036854775808,9223372036854775807))),last(reverse(range(-9223372036854775808,9223372036854775807)))",
        "UNWIND range(1,100000) AS x RETURN EXISTS { MATCH(n:Document) } LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 1000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        if (std.mem.indexOf(u8, text, "count(*)") != null) try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][0].integer);
    }
}

test "range domains reduce value counts without leaking uniqueness across axes" {
    try check("UNWIND range(1,3) AS x RETURN count(x),count(DISTINCT x)", "[[3,3]]");
    try check("UNWIND range(-2,-5,-1) AS x RETURN count(x),count(DISTINCT x)", "[[4,4]]");
    try check("UNWIND range(1,0) AS x RETURN count(x),count(DISTINCT x)", "[[0,0]]");
    try check("UNWIND [1,2] AS seed UNWIND range(1,3) AS x RETURN count(x),count(DISTINCT x)", "[[6,3]]");
    try check("UNWIND [1,1] AS seed UNWIND range(1,3) AS x RETURN seed,count(x),count(DISTINCT x)", "[[1,6,3]]");
    try check("UNWIND range(1,3) AS x WITH null AS x RETURN count(x),count(DISTINCT x)", "[[0,0]]");
    try check("WITH {} AS n UNWIND range(1,3) AS x RETURN n{x},count(x) ORDER BY n{x}", "[[{\"x\":1},1],[{\"x\":2},1],[{\"x\":3},1]]");
    for ([_][]const u8{
        "UNWIND range(1,1000000000) AS x RETURN count(x),count(DISTINCT x)",
        "WITH range(1,1000000000) AS xs WITH xs AS ys UNWIND ys AS x RETURN count(x),count(DISTINCT x)",
        "WITH range(1,1000000000) AS xs WITH xs AS ys UNWIND ys AS x RETURN count(*),count(1)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][1].integer);
    }
}

test "producer forwarding captures one binding and preserves alias escapes and shadows" {
    try check("WITH range(1,count(*)) AS xs UNWIND xs AS x RETURN x", "[[1]]");
    try check("WITH 3 AS n WITH range(1,n) AS xs UNWIND xs AS x RETURN collect(x)", "[[[1,2,3]]]");
    try check("WITH [n IN range(1,3)|toString(n)] AS xs WITH xs AS ys UNWIND ys AS x RETURN collect(x)", "[[[\"1\",\"2\",\"3\"]]]");
    try check("WITH range(1,2) AS xs UNWIND xs AS x RETURN xs,x", "[[[1,2],1],[[1,2],2]]");
    try check("WITH range(1,3) AS xs UNWIND xs AS x WITH null AS xs,x AS x RETURN count(xs),sum(x)", "[[0,6]]");
    try check("WITH [x IN null|x] AS xs UNWIND xs AS x RETURN count(*)", "[[0]]");
    try check("MATCH(a:Document {id:'a'}) WITH [(a)-[:CITES]->(b)|b.title] AS xs UNWIND xs AS title RETURN collect(title)", "[[[\"Beta\",\"Beta\"]]]");
}

test "concatenation composes list windows scalar overloads NULLs and aliases" {
    try check("RETURN head([x IN range(1,3)|x]+[]),last([]+[x IN range(1,3)|x]),size([x IN range(1,3)|x]+[4,5])", "[[1,3,5]]");
    try check("WITH [x IN range(1,3)|x]+[y IN range(4,6)|y] AS xs RETURN head(xs),last(xs),size(xs),xs[1..4],xs[-3..]", "[[1,6,6,[2,3,4],[4,5,6]]]");
    try check("RETURN head(0+[x IN range(1,3)|x]),last([x IN range(1,3)|x]+4),head('a'+[1]),size([1]+{})", "[[0,4,\"a\",2]]");
    try check("RETURN head([x IN range(1,3)|x]+null),size(null+[1]),head(reverse([1,2]+[3,4])),last([1]+reverse([2,3]))", "[[null,null,4,2]]");
    try check("RETURN ([1]+[2])[0..10],([1]+[2])[-10..],head([]+[])", "[[[1,2],[1,2],null]]");
}

test "streaming membership preserves NULL eager errors and repeated invariant probes" {
    try check("RETURN null IN [x IN []|x],null IN [x IN [1]|x],1 IN [x IN [null,2]|x],1 IN [x IN [null,1]|x]", "[[false,null,null,true]]");
    try check("UNWIND range(1,3) AS n RETURN n IN [x IN [1,null]|x]", "[[true],[null],[null]]");
    try check("MATCH(a:Document {id:'a'}) UNWIND [1,2,3] AS n RETURN 'Beta' IN [(a)-[:CITES]->(b)|b.title]", "[[true],[true],[true]]");
    try check("MATCH(a:Document {id:'a'}) UNWIND [1,2,3] AS n RETURN n IN [(a)-[:CITES]->(b)|b.rank]", "[[false],[true],[false]]");
    try check("RETURN 1 IN [x IN null|x],{x:null} IN [x IN [{x:null}]|x]", "[[null,null]]");
    for ([_][]const u8{
        "RETURN 1 IN [x IN [1,0]|1/x]",
        "RETURN head([x IN [1,0]|1/x]+[])",
        "RETURN head([1]+[x IN [1,0]|1/x])",
        "WITH [x IN [1,0]|1/x] AS xs UNWIND xs AS x RETURN x LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "range membership uses exact arithmetic and preserves empty NULL and numeric equality" {
    try check("RETURN 1 IN range(1,3),1.0 IN range(1,3),1.5 IN range(1,3),null IN range(1,0),null IN range(1,3),'1' IN range(1,3)", "[[true,true,false,false,null,false]]");
    try check("RETURN -4 IN range(5,-5,-3),-5 IN range(5,-5,-3),1 IN range(null,3),9223372036854775808.0 IN range(9223372036854775806,9223372036854775807)", "[[true,false,null,false]]");
    for ([_][]const u8{
        "RETURN 1000000000 IN range(1,1000000000)",
        "RETURN -9223372036854775808 IN range(-9223372036854775808,9223372036854775807)",
        "UNWIND (range(1,500000000)+range(1,500000000)) AS x RETURN count(*)=1000000000",
        "WITH range(1,1000000000) AS xs,1 AS seed UNWIND xs AS x RETURN count(*)=1000000000",
        "WITH 2 AS n WITH range(1,1000000000) AS xs,n AS seed UNWIND xs AS x RETURN seed,count(x)=1000000000",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expect(result.rows[0][result.rows[0].len - 1].boolean);
    }
}

test "concatenated producers stream ordered cells after complete eager validation" {
    try check("UNWIND ([x IN [1,2]|x]+[x IN [3,4]|x]) AS x RETURN collect(x)", "[[[1,2,3,4]]]");
    try check("UNWIND (0+[x IN [1,2]|x]+3) AS x RETURN collect(x)", "[[[0,1,2,3]]]");
    try check("UNWIND ([1]+null) AS x RETURN count(*)", "[[0]]");
    try check("RETURN 1 IN ([x IN [1,2]|x]+[]),null IN ([]+[]),9 IN ([null]+[1])", "[[true,false,null]]");
    try check("WITH 3 AS n WITH range(1,n) AS xs,n+1 AS n UNWIND xs AS x RETURN collect(x),min(n)", "[[[1,2,3],4]]");
    try check("WITH 3 AS n WITH n+1 AS n,range(1,n) AS xs UNWIND xs AS x RETURN collect(x),min(n)", "[[[1,2,3],4]]");
    try check("WITH range(1,2) AS xs,4 AS seed UNWIND xs AS x RETURN xs,x,seed", "[[[1,2],1,4],[[1,2],2,4]]");
    try check("WITH range(1,2) AS xs,4 AS seed WITH xs AS ys UNWIND ys AS x RETURN collect(x)", "[[[1,2]]]");
    for ([_][]const u8{
        "RETURN 1 IN ([x IN [1,0]|1/x]+[])",
        "UNWIND ([1]+[x IN [1,0]|1/x]) AS x RETURN x LIMIT 1",
        "UNWIND (null+[x IN [1,0]|1/x]) AS x RETURN count(*)",
        "WITH [x IN [1,0]|1/x] AS xs,1%0 AS seed UNWIND xs AS x RETURN count(*)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "bounded membership admission retains narrow reusable indexes" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,500) AS n RETURN count(CASE WHEN toString(n) IN [x IN range(1,1000)|toString(x)] THEN 1 END)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 4 << 20, .intermediate_states = 100000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 500), result.rows[0][0].integer);
}

test "arithmetic membership survives aliases reversal and concatenation without exposing carriers" {
    try check("WITH range(1,3) AS xs MATCH(n:Document) WHERE n.rank=size(xs) RETURN 1 IN xs,count(*)", "[[true,1]]");
    try check("WITH range(1,3) AS xs RETURN 1 IN xs,CASE WHEN size(xs)=3 THEN 1 ELSE 0 END", "[[true,1]]");
    try check("WITH range(1,3) AS xs WITH xs AS ys RETURN 2 IN ys,size(ys),head(ys),last(ys),ys[-2..]", "[[true,3,1,3,[2,3]]]");
    try check("WITH range(1,3) AS xs RETURN 2 IN reverse(xs),9 IN (xs+[]),null IN (xs+[])", "[[true,false,null]]");
    try check("WITH range(1,3) AS xs RETURN 1 IN (0+xs),3 IN (xs+'tail')", "[[true,true]]");
    try check("WITH range(1,3) AS xs RETURN 2 IN xs,xs", "[[true,[1,2,3]]]");
    try check("WITH range(1,3) AS xs RETURN 2 IN xs,any(x IN xs WHERE x=3),reduce(s=0,x IN xs|s+x)", "[[true,true,6]]");
    try check("WITH range(null,3) AS xs RETURN 1 IN xs,size(xs)", "[[null,null]]");
    for ([_][]const u8{
        "WITH range(1,1000000000) AS xs RETURN 1 IN xs",
        "WITH range(1,1000000000) AS xs WITH xs AS ys RETURN 1 IN reverse(ys)",
        "RETURN 1 IN (range(1,1000000000)+[])",
        "WITH range(-9223372036854775808,9223372036854775807) AS xs RETURN 9223372036854775807 IN xs",
        "UNWIND reverse(range(1,1000000000)) AS x RETURN count(*)=1000000000",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expect(result.rows[0][0].boolean);
    }
}

test "reversal producers preserve encounter order and validate before replay" {
    try check("UNWIND reverse([x IN range(1,5) WHERE x%2=1|x*2]) AS x RETURN collect(x)", "[[[10,6,2]]]");
    try check("UNWIND reverse([x IN [1,2]|x]+[3,4]) AS x RETURN collect(x)", "[[[4,3,2,1]]]");
    try check("UNWIND reverse(reverse([x IN [1,2]|x]+[3,4])) AS x RETURN collect(x)", "[[[1,2,3,4]]]");
    try check("UNWIND reverse([x IN null|x]) AS x RETURN count(*)", "[[0]]");
    try check("RETURN null IN reverse([x IN []|x]),1 IN reverse([x IN [null,1]|x])", "[[false,true]]");
    for ([_][]const u8{
        "RETURN 1 IN reverse([x IN [1,0]|1/x])",
        "UNWIND reverse([x IN [0,1]|1/x]) AS x RETURN x LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "captured memberships cache actual bindings and keep numeric representation" {
    try check("UNWIND [[1],[1.0],[1]] AS ys UNWIND [1,2,3] AS n RETURN 0 IN [x IN ys|x/2]", "[[true],[true],[true],[false],[false],[false],[true],[true],[true]]");
    try check("MATCH(a:Document {id:'a'}) UNWIND [1,2,3,4] AS n RETURN 'Beta' IN [(a)-[:CITES]->(b)|b.title]", "[[true],[true],[true],[true]]");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,100) AS n WITH n,$xs AS ys RETURN count(CASE WHEN toString(n) IN [x IN ys|toString(x)] THEN 1 END)", .{}, &diagnostic);
    defer compiled.deinit();
    var items: [1000]v.Value = undefined;
    for (&items, 1..) |*item, i| item.* = .{ .integer = @intCast(i) };
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &items } }}, .{ .memory_bytes = 1 << 20, .intermediate_states = 100000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 100), result.rows[0][0].integer);
}

test "wide captured membership keeps probe metadata across producer epochs" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [2000,1999] AS n WITH n,$xs AS ys RETURN ($word+toString(n)) IN [x IN ys|$word+toString(x)]", .{}, &diagnostic);
    defer compiled.deinit();
    var items: [2000]v.Value = undefined;
    for (&items, 1..) |*item, i| item.* = .{ .integer = @intCast(i) };
    const word: [4096]u8 = @splat('A');
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{ .{ .name = "xs", .value = .{ .list = &items } }, .{ .name = "word", .value = .{ .string = &word } } }, .{ .memory_bytes = 1 << 20, .intermediate_states = 700000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    for (result.rows) |row| try std.testing.expect(row[0].boolean);
}

test "validated producer replay stops at consumer completion" {
    for ([_][]const u8{
        "UNWIND reverse(range(1,1000000000)) AS x RETURN x LIMIT 1",
        "UNWIND (range(1000000000,1,-1)+[]) AS x RETURN x LIMIT 1",
        "UNWIND reverse(reverse(range(1000000000,1,-1))+[]) AS x RETURN x LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][0].integer);
    }
    // Initial source validation must still evaluate the entire eager list,
    // including operands which replay will never visit after LIMIT.
    for ([_][]const u8{
        "UNWIND [x IN [1,0]|1/x] AS x RETURN x LIMIT 1",
        "UNWIND [x IN reverse(range(0,1))|1/x] AS x RETURN x LIMIT 1",
        "UNWIND [x IN (range(1,1)+[0])|1/x] AS x RETURN x LIMIT 1",
        "UNWIND reverse([x IN [0,1]|1/x]) AS x RETURN x LIMIT 1",
        "UNWIND (range(1,1000000000)+[x IN [0]|1/x]) AS x RETURN x LIMIT 1",
        "UNWIND reverse(range(1,1000000000)) AS x RETURN 1/0 LIMIT 1",
        "UNWIND (range(1,1000000000)+[]) AS x RETURN 1/0 LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [x IN (range(1,1)+[0])|size(range(1,2,x))] AS x RETURN 1/0 LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherInvalidRange, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 1000 }));
}

test "deferred source closures cross sibling WITH chains and reversed consumers" {
    for ([_][]const u8{
        "WITH range(1,1000000000) AS xs,1 AS seed WITH xs AS ys,seed+1 AS seed UNWIND ys AS x RETURN count(*),min(seed)",
        "WITH range(1,1000000000) AS xs UNWIND reverse(xs) AS x RETURN count(*),min(2)",
        "WITH range(1,1000000000) AS xs,1 AS seed WITH seed+1 AS seed,xs AS ys WITH ys AS zs,seed AS seed UNWIND reverse(zs) AS x RETURN count(*),min(seed)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, 2), result.rows[0][1].integer);
    }
    try check("WITH 3 AS n,7 AS _producer_capture_0 WITH range(1,n) AS xs,9 AS n,_producer_capture_0 AS z WITH xs AS ys,n+1 AS n,z AS z UNWIND reverse(ys) AS x RETURN n,z,collect(x)", "[[10,7,[3,2,1]]]");
    try check("WITH 3 AS seed WITH range(1,seed) AS xs,seed AS seed WITH xs AS ys,seed+1 AS seed UNWIND reverse(ys) AS x RETURN seed,collect(x)", "[[4,[3,2,1]]]");
    try check("WITH range(1,3) AS xs,1 AS seed WITH xs AS ys,size(xs) AS seed UNWIND reverse(ys) AS x RETURN seed,collect(x)", "[[3,[3,2,1]]]");
    try check("WITH range(1,2) AS xs,1 AS seed WITH xs AS ys,seed AS seed UNWIND ys AS x RETURN ys,x", "[[[1,2],1],[[1,2],2]]");
}

test "runtime reverse cursors retain list string NULL and invalid overload behavior" {
    try check("UNWIND [[1,2,3],[],null] AS xs UNWIND reverse(xs) AS x RETURN collect(x)", "[[[3,2,1]]]");
    try check("UNWIND [[1,2,3],[]] AS xs UNWIND reverse(reverse(xs)) AS x RETURN collect(x)", "[[[1,2,3]]]");
    try check("UNWIND ['aé🌱',null] AS xs UNWIND reverse(xs) AS x RETURN collect(x)", "[[[\"🌱éa\"]]]");
    try check("UNWIND ['aé🌱'] AS xs UNWIND reverse(reverse(xs)) AS x RETURN x", "[[\"aé🌱\"]]");
    try check("UNWIND [[null,1],[],null] AS xs RETURN 1 IN reverse(xs)", "[[true],[false],[null]]");
    for ([_][]const u8{ "UNWIND [1] AS xs UNWIND reverse(xs) AS x RETURN x", "UNWIND ['abc'] AS xs RETURN 'a' IN reverse(xs)" }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND reverse($xs) AS x RETURN x LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    const items = try std.testing.allocator.alloc(v.Value, 100000);
    defer std.testing.allocator.free(items);
    for (items, 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = items } }}, .{ .memory_bytes = 1 << 20, .intermediate_states = 200000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 99999), result.rows[0][0].integer);
    var repeated = try c.compile(std.testing.allocator, "UNWIND range(1,100) AS n RETURN count(CASE WHEN n IN reverse($xs) THEN 1 END)", .{}, &diagnostic);
    defer repeated.deinit();
    var cached = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &repeated, &.{.{ .name = "xs", .value = .{ .list = items[0..10000] } }}, .{ .memory_bytes = 8 << 20, .intermediate_states = 200000 });
    defer cached.deinit();
    try std.testing.expectEqual(@as(i64, 100), cached.rows[0][0].integer);
}

test "pattern collection summaries memoize captured bindings after lowering" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,100) AS i MATCH(a:Document {id:'a'}) RETURN i,size([(a)-[:CITES]->(b)|b.rank]) AS n", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .explored_edges = 4 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 100), result.rows.len);
    try std.testing.expectEqual(@as(i64, 100), result.rows[99][0].integer);
    try std.testing.expectEqual(@as(i64, 2), result.rows[99][1].integer);
    try std.testing.expect(result.stats.explored_edges <= 4);
    try check("UNWIND [1,2,1,3,2] AS i MATCH(a:Document {id:'a'}) RETURN i,size([(a)-[:CITES]->(b) WHERE b.rank<=i|b.rank])", "[[1,0],[2,2],[1,0],[3,2],[2,2]]");
    try check("UNWIND [1,1.0,1] AS i MATCH(a:Document {id:'a'}) RETURN head([(a)-[:CITES]->(b)|i/2])", "[[0],[0.5],[0]]");
    var windows = try c.compile(std.testing.allocator, "UNWIND range(1,100) AS i MATCH(a:Document {id:'a'}) RETURN i,head([(a)-[:CITES]->(b)|b.rank]),size([(a)-[:CITES]->(b)|b.rank])", .{}, &diagnostic);
    defer windows.deinit();
    var bounded = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &windows, &.{}, .{ .explored_edges = 8 });
    defer bounded.deinit();
    try std.testing.expectEqual(@as(usize, 100), bounded.rows.len);
    try std.testing.expectEqual(@as(i64, 2), bounded.rows[99][1].integer);
    try std.testing.expectEqual(@as(i64, 2), bounded.rows[99][2].integer);
}

test "deferred transformations preserve arithmetic sources and operand scopes" {
    for ([_][]const u8{
        "WITH range(1,1000000000) AS xs UNWIND (xs+[]) AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs,1 AS seed WITH reverse(xs) AS ys,seed AS seed UNWIND ys AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs WITH ([]+reverse(xs)) AS ys UNWIND (ys+[]) AS x RETURN count(*)",
        "UNWIND reverse(range(1,1000000000)) AS x RETURN count(x)",
        "UNWIND reverse(range(1,1000000000)) AS x RETURN count(DISTINCT x)",
        "WITH range(1,1000000000) AS xs WITH reverse(xs) AS ys UNWIND ys AS x RETURN count(x),count(DISTINCT x)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][0].integer);
    }
    try check("WITH 3 AS n WITH range(1,n) AS xs,9 AS n WITH (xs+[n]) AS ys,n+1 AS n UNWIND (ys+[n]) AS x RETURN n,collect(x)", "[[10,[1,2,3,9,10]]]");
    try check("WITH range(1,3) AS xs WITH (9+reverse(xs)) AS ys UNWIND (ys+7) AS x RETURN collect(x)", "[[[9,3,2,1,7]]]");
    try check("WITH range(1,3) AS xs WITH (xs+null) AS ys UNWIND reverse(ys) AS x RETURN count(*)", "[[0]]");
    try check("WITH range(1,3) AS xs WITH reverse(xs) AS ys UNWIND ys AS x RETURN ys,x", "[[[3,2,1],3],[[3,2,1],2],[[3,2,1],1]]");
    // Individually shallow clauses must not build an unbounded source AST.
    var chain: std.ArrayList(u8) = .empty;
    defer chain.deinit(std.testing.allocator);
    try chain.appendSlice(std.testing.allocator, "WITH range(1,3) AS xs ");
    for (0..80) |_| try chain.appendSlice(std.testing.allocator, "WITH reverse(xs) AS xs ");
    try chain.appendSlice(std.testing.allocator, "UNWIND xs AS x RETURN collect(x)");
    try check(chain.items, "[[[1,2,3]]]");
    for ([_][]const u8{
        "WITH range(1,3,0) AS xs UNWIND (xs+[1/0]) AS x RETURN x LIMIT 1",
        "WITH range(1,3) AS xs WITH (xs+[size(range(1,2,0))]) AS ys,1/0 AS n UNWIND ys AS x RETURN x",
        "WITH range(1,3) AS xs UNWIND ((xs+[size(range(1,2,0))])+[1/0]) AS x RETURN x LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherInvalidRange, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "evaluated list axes carry multiplicity and reversed consumers retain only demand" {
    const items = try std.testing.allocator.alloc(v.Value, 100000);
    defer std.testing.allocator.free(items);
    for (items, 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
    for ([_][]const u8{
        "RETURN size(reverse($xs)),head(reverse($xs)),last(reverse($xs)),reverse($xs)[-1],reverse($xs)[1..3]",
        "UNWIND (reverse($xs)+[]) AS x RETURN x LIMIT 1",
        "UNWIND ([]+reverse($xs)) AS x RETURN x LIMIT 1",
        "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,count(*)",
    }, 0..) |text, i| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = items } }}, .{ .memory_bytes = 1 << 20, .intermediate_states = 150000 });
        defer result.deinit();
        if (i == 0) {
            try std.testing.expectEqual(@as(i64, 100000), result.rows[0][0].integer);
            try std.testing.expectEqual(@as(i64, 99999), result.rows[0][1].integer);
            try std.testing.expectEqual(@as(i64, 0), result.rows[0][2].integer);
            try std.testing.expectEqual(@as(i64, 0), result.rows[0][3].integer);
            try std.testing.expectEqual(@as(i64, 99998), result.rows[0][4].list[0].integer);
            try std.testing.expectEqual(@as(i64, 99997), result.rows[0][4].list[1].integer);
        } else if (i == 3) {
            try std.testing.expectEqual(@as(usize, 30), result.rows.len);
            try std.testing.expectEqual(@as(i64, 100000), result.rows[29][1].integer);
        } else try std.testing.expectEqual(@as(i64, 99999), result.rows[0][0].integer);
    }
    try check("UNWIND [[1,null,2],[],null] AS xs UNWIND range(1,2) AS i UNWIND xs AS x RETURN i,count(*)", "[[1,3],[2,3]]");
    try check("UNWIND [[1,null,2]] AS xs UNWIND xs AS x RETURN count(x),count(*)", "[[2,3]]");
    try check("UNWIND ['aé🌱',null] AS xs RETURN size(reverse(xs))", "[[3],[null]]");
    try check("UNWIND [[1,2,3],[],null] AS xs RETURN head(reverse(xs)),reverse(xs)[-1],reverse(xs)[1..2]", "[[3,1,[2]],[null,null,[]],[null,null,null]]");
    try check("UNWIND ['abc'] AS xs RETURN reverse(xs)[null],reverse(xs)[null..1]", "[[null,null]]");
    for ([_][]const u8{
        "UNWIND ['abc'] AS xs RETURN head(reverse(xs))",
        "UNWIND ['abc'] AS xs RETURN reverse(xs)[0]",
        "UNWIND ['abc'] AS xs RETURN reverse(xs)[0..1]",
        "UNWIND [1] AS xs RETURN size(reverse(xs))",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "lexical source DAGs share validation and keep added producer frames" {
    for ([_][]const u8{
        "WITH range(1,1000000000) AS xs UNWIND (xs+range(1000000001,2000000000)) AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs UNWIND (xs+xs) AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs WITH (xs+xs) AS ys UNWIND reverse(ys) AS x RETURN count(*)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2000000000), result.rows[0][0].integer);
    }
    var chain: std.ArrayList(u8) = .empty;
    defer chain.deinit(std.testing.allocator);
    try chain.appendSlice(std.testing.allocator, "WITH [x IN range(1,10000)|toString(x)] AS xs,1 AS seed ");
    for (0..16) |_| try chain.appendSlice(std.testing.allocator, "WITH xs AS xs,seed AS seed ");
    try chain.appendSlice(std.testing.allocator, "UNWIND xs AS x RETURN count(*)");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, chain.items, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 2 << 20, .intermediate_states = 100000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 10000), result.rows[0][0].integer);
    // A source without a replay proof still streams once when no intervening
    // sibling/transform needs its eager result. It must not acquire a closure.
    var once_query = try c.compile(std.testing.allocator, "WITH [x IN range(1,10000)|EXISTS { RETURN 1 }] AS xs UNWIND xs AS x RETURN count(x)", .{}, &diagnostic);
    defer once_query.deinit();
    var once = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &once_query, &.{}, .{ .memory_bytes = 256 << 10 });
    defer once.deinit();
    try std.testing.expectEqual(@as(i64, 10000), once.rows[0][0].integer);
    var direct_query = try c.compile(std.testing.allocator, "UNWIND [x IN range(1,10000)|EXISTS { RETURN 1 }] AS x RETURN count(x)", .{}, &diagnostic);
    defer direct_query.deinit();
    var direct = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &direct_query, &.{}, .{ .memory_bytes = 256 << 10 });
    defer direct.deinit();
    try std.testing.expect(once.stats.intermediate_states <= direct.stats.intermediate_states + 20100);
    try check("WITH 2 AS n WITH range(1,n) AS xs,5 AS n WITH (xs+range(n,n+1)) AS ys,n+1 AS n UNWIND (ys+range(n,n+1)) AS x RETURN collect(x)", "[[[1,2,5,6,6,7]]]");
    try check("WITH [x IN [1,null,2]|x] AS xs UNWIND (xs+xs) AS x RETURN count(x),count(*),collect(x)", "[[4,6,[1,2,1,2]]]");
    try check("WITH [x IN [1,2,3]|toString(x)] AS xs WITH xs AS ys,1 AS seed UNWIND reverse(ys) AS x RETURN collect(x)", "[[[\"3\",\"2\",\"1\"]]]");
    try check("WITH [x IN [1,2]|x] AS xs UNWIND [y IN (xs+xs)|y/2] AS x RETURN collect(x)", "[[[0,1,0,1]]]");
    for ([_][]const u8{
        "WITH [x IN [1,0]|1/x] AS xs UNWIND (xs+range(1,3,0)) AS x RETURN x LIMIT 1",
        "WITH range(1,3) AS xs UNWIND (xs+[x IN [0]|1/x]) AS x RETURN x LIMIT 1",
        "WITH range(1,3) AS xs WITH (xs+[x IN [0]|1/x]) AS ys,1 AS seed UNWIND ys AS x RETURN x LIMIT 1",
    }) |text| {
        var bad = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer bad.deinit();
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &bad, &.{}, .{}));
    }
}

test "returned values share immutable allocations across the result ownership boundary" {
    const payload = try std.testing.allocator.alloc(u8, 200000);
    defer std.testing.allocator.free(payload);
    @memset(payload, 'x');
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.testing.allocator);
    try text.appendSlice(std.testing.allocator, "RETURN ");
    for (0..16) |i| {
        if (i != 0) try text.appendSlice(std.testing.allocator, ",");
        const field = try std.fmt.allocPrint(std.testing.allocator, "$s AS c{d}", .{i});
        defer std.testing.allocator.free(field);
        try text.appendSlice(std.testing.allocator, field);
    }
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, text.items, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "s", .value = .{ .string = payload } }}, .{ .memory_bytes = 1 << 20 });
    defer result.deinit();
    for (result.rows[0]) |cell| {
        try std.testing.expectEqual(@as(usize, 200000), cell.string.len);
        try std.testing.expectEqual(result.rows[0][0].string.ptr, cell.string.ptr);
    }
    @memset(payload, 'z');
    try std.testing.expectEqual(@as(u8, 'x'), result.rows[0][0].string[0]);
    var typed_query = try c.compile(std.testing.allocator, "UNWIND [1,1.0,1] AS x RETURN {k:[x]}", .{}, &diagnostic);
    defer typed_query.deinit();
    var typed = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &typed_query, &.{}, .{});
    defer typed.deinit();
    try std.testing.expect(typed.rows[0][0].map[0].value.list[0] == .integer);
    try std.testing.expect(typed.rows[1][0].map[0].value.list[0] == .number);
    try std.testing.expect(typed.rows[2][0].map[0].value.list[0] == .integer);
    var dag_query = try c.compile(std.testing.allocator, "RETURN reduce(xs=[1],i IN range(1,16)|[xs,xs])", .{}, &diagnostic);
    defer dag_query.deinit();
    var dag = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &dag_query, &.{}, .{ .memory_bytes = 1 << 20 });
    defer dag.deinit();
    const children = dag.rows[0][0].list;
    try std.testing.expectEqual(@as(usize, 2), children.len);
    try std.testing.expectEqual(children[0].list.ptr, children[1].list.ptr);

    try check("UNWIND [1,1.0,1] AS x RETURN {k:[x]},[x/2]", "[[{\"k\":[1]},[0]],[{\"k\":[1]},[0.5]],[{\"k\":[1]},[0]]]");
}

test "read-only graph summaries retain identity keys without copying wide entity properties" {
    const Wide = struct {
        const payload: [600000]u8 = @splat('x');
        fn widen(a: std.mem.Allocator, node: *const v.Node) !*const v.Node {
            if (!node.identity.eql(Fixture.nodes[0].identity)) return node;
            const fields = try a.alloc(v.Entry, node.properties.len + 1);
            @memcpy(fields[0..node.properties.len], node.properties);
            fields[node.properties.len] = .{ .name = "payload", .value = .{ .string = &payload } };
            const copy = try a.create(v.Node);
            copy.* = node.*;
            copy.properties = fields;
            return copy;
        }
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, name: []const u8, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            var page = try Fixture.scan(ptr, a, name, cursor, limit);
            const nodes = try a.alloc(*const v.Node, page.items.len);
            for (page.items, nodes) |node, *out| out.* = try widen(a, node);
            page.items = nodes;
            return page;
        }
        fn get(ptr: *anyopaque, a: std.mem.Allocator, identity: v.Identity) !?*const v.Node {
            return if (try Fixture.get(ptr, a, identity)) |node| try widen(a, node) else null;
        }
    };
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.scan = Wide.scan;
    vtable.get = Wide.get;
    backend.vtable = &vtable;
    var diagnostic: c.Diagnostic = .{};
    for ([_][]const u8{
        "MATCH(a:Document {id:'a'}) UNWIND range(1,30) AS i RETURN i,size([(a)-[:CITES]->(b) WHERE size(a.payload)>=0|b.rank])",
        "MATCH(a:Document {id:'a'}) UNWIND range(1,30) AS i CALL { WITH a MATCH(a)-[:CITES]->(b) WHERE size(a.payload)>=0 RETURN count(*) AS c } RETURN i,c",
        "MATCH(a:Document {id:'a'}) UNWIND range(1,30) AS i CALL { WITH a MATCH(a)-[:CITES]->(b) WITH a,count(*) AS c RETURN c,a.payload AS p } RETURN i,c,size(p)",
        "MATCH(a:Document {id:'a'}) UNWIND range(1,100) AS i CALL { WITH a MATCH(a)-[:CITES]->(b) WITH a,count(*) AS c RETURN c,a.payload AS p } RETURN i,c,size(p)",
        "MATCH(a:Document {id:'a'}) UNWIND range(1,100) AS i CALL { WITH a MATCH(a)-[:CITES]->(b) WITH a,count(*) AS c RETURN c,a.payload AS p } RETURN i,c,head([z IN [1,2]|size(p)])",
        "MATCH(a:Document {id:'a'}) UNWIND range(1,30) AS i CALL { WITH a MATCH(a)-[:CITES]->(b) WITH a,count(*) AS c RETURN c,a.payload AS p } RETURN i,c,p ORDER BY i",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .memory_bytes = 8 << 20, .explored_edges = 6, .intermediate_states = if (std.mem.indexOf(u8, text, "RETURN i,c,p ORDER") != null) 1000000 else 100000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (std.mem.indexOf(u8, text, "range(1,100)") != null) 100 else 30), result.rows.len);
        for (result.rows) |row| try std.testing.expectEqual(@as(i64, 2), row[1].integer);
    }
}

test "read-policy grouping reuses statement-owned payloads through WITH projections" {
    const Wide = struct {
        fn scan(_: *anyopaque, a: std.mem.Allocator, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            if (!std.mem.eql(u8, table, "docs")) return .{ .items = &.{}, .unfiltered = true };
            const start = if (after) |cursor| (try std.fmt.parseInt(usize, cursor[1..], 10)) + 1 else 0;
            const end = @min(@as(usize, 3000), start + @min(limit, 3000));
            const nodes = try a.alloc(*const v.Node, end - start);
            for (nodes, start..) |*out, i| {
                const key = try std.fmt.allocPrint(a, "n{d:0>8}", .{i});
                const payload = try a.alloc(u8, 4092);
                @memset(payload, 'x');
                _ = try std.fmt.bufPrint(payload[0..12], "{d:0>12}", .{i});
                const properties = try a.alloc(v.Entry, 3);
                properties[0] = .{ .name = "id", .value = .{ .string = key } };
                properties[1] = .{ .name = "rank", .value = .{ .integer = @intCast(i) } };
                properties[2] = .{ .name = "payload", .value = .{ .string = payload } };
                const node = try a.create(v.Node);
                node.* = .{ .identity = .{ .table = "docs", .key = key }, .labels = &.{"Document"}, .properties = properties };
                out.* = node;
            }
            return .{ .items = nodes, .next = if (end < 3000) nodes[nodes.len - 1].identity.key else null, .unfiltered = true };
        }
        fn versions(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            var page = try scan(ptr, a, table, null, 3);
            for (page.items, "zam") |node, first| {
                @constCast(node).identity.key = "same";
                @constCast(node.properties[2].value.string)[0] = first;
            }
            page.next = null;
            return page;
        }
        // The callback deliberately prevents entity-field pruning. Ownership
        // reuse must preserve the complete policy-visible node, not drop fields.
        fn project(_: *anyopaque, _: std.mem.Allocator, node: *const v.Node) !?*const v.Node {
            return node;
        }
    };
    var fixture: Fixture = .{};
    var backend = fixture.backend();
    var vtable = backend.vtable.*;
    vtable.scan = Wide.scan;
    vtable.project_node = Wide.project;
    backend.vtable = &vtable;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH(n:Document) WITH n,min(n.payload) AS p RETURN n.rank,size(p) ORDER BY n.rank", .{}, &diagnostic);
    defer compiled.deinit();
    // Forbid arena extension so the budget also covers the worst allocation
    // layout, independently of neighboring allocations in a larger test run.
    var allocation_vtable = std.testing.allocator.vtable.*;
    allocation_vtable.resize = std.mem.Allocator.noResize;
    allocation_vtable.remap = std.mem.Allocator.noRemap;
    const backing: std.mem.Allocator = .{ .ptr = std.testing.allocator.ptr, .vtable = &allocation_vtable };
    var result = try runtime.execute(backing, backend, graph, &compiled, &.{}, .{ .memory_bytes = 40 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3000), result.rows.len);
    for (result.rows, 0..) |row, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
        try std.testing.expectEqual(@as(i64, 4092), row[1].integer);
    }
    // Equal entity identities may arrive with different policy-visible fields.
    // Only the first input's exact allocations may be borrowed from its key;
    // subsequent MIN/MAX/COLLECT inputs must retain their own actual payloads.
    vtable.scan = Wide.versions;
    var versions = try c.compile(std.testing.allocator, "MATCH(n:Document) WITH n,min(CASE WHEN EXISTS { RETURN 1 } THEN n.payload END) AS lo,max(n.payload) AS hi,collect(n.payload) AS xs RETURN substring(n.payload,0,1),substring(lo,0,1),substring(hi,0,1),[x IN xs|substring(x,0,1)]", .{}, &diagnostic);
    defer versions.deinit();
    var distinct_views = try runtime.execute(std.testing.allocator, backend, graph, &versions, &.{}, .{});
    defer distinct_views.deinit();
    try std.testing.expectEqual(@as(usize, 1), distinct_views.rows.len);
    const cells = distinct_views.rows[0];
    try std.testing.expectEqualStrings("z", cells[0].string);
    try std.testing.expectEqualStrings("a", cells[1].string);
    try std.testing.expectEqualStrings("z", cells[2].string);
    for (cells[3].list, "zam") |cell, expected| try std.testing.expectEqual(expected, cell.string[0]);
}

test "statement payload slabs transfer result ownership and release every allocation failure" {
    const Campaign = struct {
        const large: [70000]u8 = @splat('L');
        const small: [4092]u8 = @splat('s');
        fn run(allocator: std.mem.Allocator) !void {
            var vtable = allocator.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const backing: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(backing, "RETURN $large,[$large,$small],{p:$small}", .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(backing, fixture.backend(), graph, &compiled, &.{ .{ .name = "large", .value = .{ .string = &large } }, .{ .name = "small", .value = .{ .string = &small } } }, .{});
            defer result.deinit();
            try std.testing.expect(result.statement_byte_pool != null);
            try std.testing.expectEqual(result.rows[0][0].string.ptr, result.rows[0][1].list[0].string.ptr);
            try std.testing.expectEqualStrings(&large, result.rows[0][0].string);
            try std.testing.expectEqualStrings(&small, result.rows[0][2].map[0].value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}

test "multiple deferred producer bindings retain lexical frames and eager validation" {
    for ([_][]const u8{
        "WITH range(1,1000000000) AS xs,range(1000000001,2000000000) AS ys UNWIND (xs+ys) AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs WITH xs,range(1000000001,2000000000) AS ys UNWIND (xs+ys) AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs WITH xs AS ys,xs AS zs UNWIND (reverse(ys)+zs) AS x RETURN count(*)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2000000000), result.rows[0][0].integer);
    }
    try check("WITH 2 AS n WITH range(1,n) AS xs,range(n+1,n+2) AS ys,n+10 AS n WITH reverse(xs) AS a,(ys+xs) AS b,n+1 AS n UNWIND (a+b+range(n,n)) AS x RETURN collect(x),min(n)", "[[[2,1,3,4,1,2,13],13]]");
    try check("WITH range(1,2) AS xs,range(3,4) AS ys UNWIND (xs+ys) AS z RETURN collect(z)", "[[[1,2,3,4]]]");
    try check("WITH range(1,3) AS xs,range(4,5) AS ys UNWIND xs AS x RETURN collect(x),head(collect(ys))", "[[[1,2,3],[4,5]]]");
    try check("WITH [x IN null|x] AS xs,range(1,3) AS ys UNWIND (xs+ys) AS x RETURN count(*)", "[[0]]");
    try check("WITH range(1,2) AS xs,range(3,4) AS ys UNWIND [x IN (xs+ys)|x/2] AS z RETURN collect(z)", "[[[0,1,1,2]]]");
    var diagnostic: c.Diagnostic = .{};
    var fixture: Fixture = .{};
    for ([_][]const u8{
        "WITH range(1,1000000000) AS xs,[x IN [0]|1/x] AS ys UNWIND xs AS x RETURN count(*)",
        "WITH range(1,1000000000) AS xs,1/0 AS n,range(1,2,0) AS ys UNWIND (xs+ys) AS x RETURN count(*)",
        "WITH [x IN null|x] AS xs,[x IN [0]|1/x] AS ys UNWIND (xs+ys) AS x RETURN x LIMIT 1",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 1000 }));
    }
    var bad = try c.compile(std.testing.allocator, "WITH range(1,2,0) AS xs,1/0 AS n,range(1,3) AS ys UNWIND (xs+ys) AS x RETURN count(*)", .{}, &diagnostic);
    defer bad.deinit();
    try std.testing.expectError(error.CypherInvalidRange, runtime.execute(std.testing.allocator, fixture.backend(), graph, &bad, &.{}, .{}));
}

test "immutable list count facts share DISTINCT domains across groups and merge overlaps" {
    const items = try std.testing.allocator.alloc(v.Value, 10002);
    defer std.testing.allocator.free(items);
    for (items[0..10000], 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
    items[10000] = .{ .number = 1.0 };
    items[10001] = .null;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,count(x),count(DISTINCT x),count(*),count(null),count(DISTINCT i) ORDER BY i", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = items } }}, .{ .memory_bytes = 4 << 20, .intermediate_states = 100000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 30), result.rows.len);
    for (result.rows, 1..) |row, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
        try std.testing.expectEqual(@as(i64, 10001), row[1].integer);
        try std.testing.expectEqual(@as(i64, 10000), row[2].integer);
        try std.testing.expectEqual(@as(i64, 10002), row[3].integer);
        try std.testing.expectEqual(@as(i64, 0), row[4].integer);
        try std.testing.expectEqual(@as(i64, 1), row[5].integer);
    }
    const lists: v.Value = .{ .list = &.{
        .{ .list = &.{ .{ .integer = 1 }, .{ .integer = 2 }, .null } },
        .{ .list = &.{ .{ .number = 2.0 }, .{ .integer = 3 } } },
        .{ .list = &.{.null} },
        .{ .list = &.{} },
        .{ .list = &.{ .{ .integer = 1 }, .{ .integer = 2 } } },
    } };
    var union_query = try c.compile(std.testing.allocator, "UNWIND $lists AS xs UNWIND xs AS x RETURN count(x),count(DISTINCT x),count(*)", .{}, &diagnostic);
    defer union_query.deinit();
    var merged = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &union_query, &.{.{ .name = "lists", .value = lists }}, .{});
    defer merged.deinit();
    try std.testing.expectEqual(@as(i64, 6), merged.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 3), merged.rows[0][1].integer);
    try std.testing.expectEqual(@as(i64, 8), merged.rows[0][2].integer);
    var fallback_query = try c.compile(std.testing.allocator, "UNWIND [1,2] AS i UNWIND CASE WHEN i=1 THEN $xs ELSE [i,3] END AS x RETURN count(x),count(DISTINCT x),count(*)", .{}, &diagnostic);
    defer fallback_query.deinit();
    var fallback = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &fallback_query, &.{.{ .name = "xs", .value = .{ .list = &.{ .{ .integer = 1 }, .{ .integer = 2 } } } }}, .{});
    defer fallback.deinit();
    try std.testing.expectEqual(@as(i64, 4), fallback.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 3), fallback.rows[0][1].integer);
    try std.testing.expectEqual(@as(i64, 4), fallback.rows[0][2].integer);
    const Policy = struct {
        fn project(_: *anyopaque, _: std.mem.Allocator, node: *const v.Node) !?*const v.Node {
            return if (node.identity.eql(Fixture.nodes[1].identity)) null else node;
        }
    };
    var backend = fixture.backend();
    var callbacks = backend.vtable.*;
    callbacks.project_node = Policy.project;
    backend.vtable = &callbacks;
    var entities_query = try c.compile(std.testing.allocator, "UNWIND $xs AS x RETURN count(x),count(DISTINCT x),count(*)", .{}, &diagnostic);
    defer entities_query.deinit();
    var entities = try runtime.execute(std.testing.allocator, backend, graph, &entities_query, &.{.{ .name = "xs", .value = .{ .list = &.{ .{ .node = &Fixture.nodes[0] }, .{ .node = &Fixture.nodes[1] } } } }}, .{});
    defer entities.deinit();
    try std.testing.expectEqual(@as(i64, 1), entities.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 1), entities.rows[0][1].integer);
    try std.testing.expectEqual(@as(i64, 2), entities.rows[0][2].integer);
}

test "multiple source frames and shared list count domains release failed allocations" {
    const Campaign = struct {
        fn run(allocator: std.mem.Allocator, text: []const u8) !void {
            var vtable = allocator.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const backing: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(backing, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            const lists: v.Value = .{ .list = &.{
                .{ .list = &.{ .{ .integer = 1 }, .{ .integer = 2 }, .null } },
                .{ .list = &.{ .{ .number = 2.0 }, .{ .integer = 3 } } },
            } };
            var result = try runtime.execute(backing, fixture.backend(), graph, &compiled, &.{.{ .name = "lists", .value = lists }}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "WITH range(1,3) AS xs,range(4,6) AS ys WITH reverse(xs) AS a,(ys+xs) AS b UNWIND (a+b) AS x RETURN collect(x)",
        "UNWIND $lists AS xs UNWIND xs AS x RETURN count(DISTINCT x),count(x),count(*)",
        "UNWIND [1,2] AS i UNWIND CASE WHEN i=1 THEN $lists[0] ELSE [i,3] END AS x RETURN count(DISTINCT x),count(*)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "composed arithmetic count domains preserve overlaps aliases and eager errors" {
    for ([_]struct { text: []const u8, count: i64, distinct: i64 }{
        .{ .text = "WITH range(1,1000000000) AS xs,range(500000001,1500000000) AS ys UNWIND (reverse(xs)+ys) AS x RETURN count(x),count(DISTINCT x),count(*)", .count = 2000000000, .distinct = 1500000000 },
        .{ .text = "WITH range(1,1000000000,2) AS xs,range(1000000000,1,-3) AS ys UNWIND (xs+ys) AS x WITH x AS y WITH y AS z RETURN count(z),count(DISTINCT z),count(*)", .count = 833333334, .distinct = 666666667 },
        .{ .text = "UNWIND (range(1,1000000000)+range(1,1000000000)) AS x RETURN count(x),count(DISTINCT x),count(*)", .count = 2000000000, .distinct = 1000000000 },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 2000 });
        defer result.deinit();
        try std.testing.expectEqual(case.count, result.rows[0][0].integer);
        try std.testing.expectEqual(case.distinct, result.rows[0][1].integer);
        try std.testing.expectEqual(case.count, result.rows[0][2].integer);
    }
    try check("UNWIND (range(null,3)+range(1,3)) AS x RETURN count(x),count(DISTINCT x)", "[[0,0]]");
    try check("UNWIND (range(3,1)+range(5,7)) AS x RETURN count(x),count(DISTINCT x)", "[[3,3]]");
    try check("UNWIND [1,2] AS i UNWIND (range(1,3)+range(2,4)) AS x RETURN count(x),count(DISTINCT x),count(*)", "[[12,4,12]]");
    try check("UNWIND (range(1,3)+range(2,4)) AS x WITH x AS y,x AS z RETURN count(y),count(DISTINCT z)", "[[6,4]]");
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var bad = try c.compile(std.testing.allocator, "UNWIND (range(null,3)+range(1,3,0)) AS x RETURN count(x)", .{}, &diagnostic);
    defer bad.deinit();
    try std.testing.expectError(error.CypherInvalidRange, runtime.execute(std.testing.allocator, fixture.backend(), graph, &bad, &.{}, .{}));
}

test "immutable list aggregate facts survive renames and preserve extrema representations" {
    const items = try std.testing.allocator.alloc(v.Value, 10002);
    defer std.testing.allocator.free(items);
    for (items[0..10000], 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
    items[10000] = .{ .number = 1.0 };
    items[10001] = .null;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x AS y WITH i AS j,y AS z RETURN j,count(z),count(DISTINCT z),min(z),max(DISTINCT z),count(*) ORDER BY j", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = items } }}, .{ .memory_bytes = 4 << 20, .intermediate_states = 200000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 30), result.rows.len);
    for (result.rows, 1..) |row, i| {
        try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
        for ([_]i64{ 10001, 10000, 0, 9999, 10002 }, row[1..]) |expected, value| try std.testing.expectEqual(expected, value.integer);
    }
    var ties = try c.compile(std.testing.allocator, "UNWIND $xs AS x RETURN min(x),max(x),count(x)", .{}, &diagnostic);
    defer ties.deinit();
    var tied = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &ties, &.{.{ .name = "xs", .value = .{ .list = &.{ .null, .{ .number = 1.0 }, .{ .integer = 1 } } } }}, .{});
    defer tied.deinit();
    try std.testing.expect(tied.rows[0][0] == .number and tied.rows[0][1] == .number);
    try std.testing.expectEqual(@as(i64, 2), tied.rows[0][2].integer);
}

test "shared source summaries and CALL escape release failed allocations" {
    const Campaign = struct {
        fn run(allocator: std.mem.Allocator, text: []const u8) !void {
            var vtable = allocator.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &.{ .{ .integer = 3 }, .null, .{ .integer = 1 }, .{ .number = 1.0 } } } }}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "WITH range(1,9,2) AS xs,range(10,1,-3) AS ys UNWIND (xs+ys) AS x WITH x AS y RETURN count(y),count(DISTINCT y)",
        "UNWIND [1,2] AS i UNWIND $xs AS x WITH i,x AS y RETURN i,count(y),count(DISTINCT y),min(y),max(y)",
        "MATCH(a:Document {id:'a'}) UNWIND [1,2,3] AS i CALL {WITH a MATCH(a)-[:CITES]->(b) RETURN collect(b.title) AS titles} RETURN i,titles",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "arithmetic domain certificates preserve source DAG sharing" {
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.testing.allocator);
    try text.appendSlice(std.testing.allocator, "WITH range(1,1000000000) AS xs ");
    for (0..20) |_| try text.appendSlice(std.testing.allocator, "WITH (xs+xs) AS xs ");
    try text.appendSlice(std.testing.allocator, "UNWIND xs AS x RETURN count(x),count(DISTINCT x)");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, text.items, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 2 << 20, .intermediate_states = 10000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1000000000) << 20, result.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 1000000000), result.rows[0][1].integer);
}

test "CALL memo result sharing preserves maximum user value depth" {
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [1,2,3] AS i CALL {RETURN reduce(xs=0,x IN range(1,64)|[xs]) AS xs} RETURN i,xs ORDER BY i", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    for (result.rows) |row| {
        var value = row[1];
        for (0..64) |_| value = value.list[0];
        try std.testing.expectEqual(@as(i64, 0), value.integer);
    }
}

test "source summaries cross independent projections and certify integer sums" {
    const items = try std.testing.allocator.alloc(v.Value, 10000);
    defer std.testing.allocator.free(items);
    for (items, 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i+1 AS j,x AS y RETURN j,count(y),sum(y),min(y),max(y) ORDER BY j", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = items } }}, .{ .memory_bytes = 4 << 20, .intermediate_states = 150000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 30), result.rows.len);
    for (result.rows, 2..) |row, j| {
        try std.testing.expectEqual(@as(i64, @intCast(j)), row[0].integer);
        for ([_]i64{ 10000, 49995000, 0, 9999 }, row[1..]) |expected, value| try std.testing.expectEqual(expected, value.integer);
    }
}

test "integer list sum certificates preserve prefix overflow mixed domains and deferred errors" {
    const inputs = [_]v.Value{
        .{ .list = &.{ .{ .integer = 9223372036854775807 }, .{ .integer = 1 }, .{ .integer = -1 } } },
        .{ .list = &.{ .{ .integer = -9223372036854775808 }, .{ .integer = -1 }, .{ .integer = 1 } } },
    };
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var overflow = try c.compile(std.testing.allocator, "UNWIND $xs AS x RETURN sum(x)", .{}, &diagnostic);
    defer overflow.deinit();
    for (inputs) |value| try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &overflow, &.{.{ .name = "xs", .value = value }}, .{}));
    var scalar_error = try c.compile(std.testing.allocator, "UNWIND $xs AS x WITH x AS y,1/0 AS n RETURN sum(y),min(n)", .{}, &diagnostic);
    defer scalar_error.deinit();
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &scalar_error, &.{.{ .name = "xs", .value = inputs[0] }}, .{}));
    try check("UNWIND [[1,2,null],[-3,4],[],[null]] AS xs UNWIND xs AS x RETURN sum(x),count(x)", "[[4,4]]");
    // Floating and non-integral domains keep their original evaluation order.
    const mixed: v.Value = .{ .list = &.{ .{ .list = &.{.{ .number = 1e16 }} }, .{ .list = &.{ .{ .integer = -1 }, .{ .integer = -1 } } } } };
    var grouped = try c.compile(std.testing.allocator, "UNWIND $lists AS xs UNWIND xs AS x RETURN sum(x)", .{}, &diagnostic);
    defer grouped.deinit();
    var actual = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &grouped, &.{.{ .name = "lists", .value = mixed }}, .{});
    defer actual.deinit();
    try std.testing.expectEqual(@as(f64, 1e16), actual.rows[0][0].number);
    var deferred = try c.compile(std.testing.allocator, "UNWIND $xs AS x RETURN CASE WHEN false THEN sum(x) ELSE 7 END", .{}, &diagnostic);
    defer deferred.deinit();
    var unused = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &deferred, &.{.{ .name = "xs", .value = inputs[0] }}, .{});
    defer unused.deinit();
    try std.testing.expectEqual(@as(i64, 7), unused.rows[0][0].integer);
}

test "arithmetic extrema compose without enumerating cells" {
    for ([_]struct { text: []const u8, low: i64, high: i64 }{
        .{ .text = "UNWIND range(1,1000000000) AS x RETURN min(x),max(x)", .low = 1, .high = 1000000000 },
        .{ .text = "WITH range(1000000000,1,-3) AS xs,range(-1000000000,-1,2) AS ys UNWIND (reverse(xs)+ys) AS x WITH x AS y,2+3 AS n RETURN min(DISTINCT y),max(y),min(n)", .low = -1000000000, .high = 1000000000 },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(case.low, result.rows[0][0].integer);
        try std.testing.expectEqual(case.high, result.rows[0][1].integer);
    }
    try check("UNWIND range(3,1) AS x RETURN min(x),max(x)", "[[null,null]]");
    try check("UNWIND (range(null,1)+range(1,2)) AS x RETURN min(x),max(x)", "[[null,null]]");
}

test "new source aggregate and metadata owners release every failed allocation" {
    const Campaign = struct {
        const payload: [300]u8 = @splat('x');
        fn run(allocator: std.mem.Allocator, text: []const u8) !void {
            var vtable = allocator.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = allocator.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{ .{ .name = "xs", .value = .{ .list = &.{ .{ .integer = 3 }, .null, .{ .integer = -1 }, .{ .integer = 2 } } } }, .{ .name = "s", .value = .{ .string = &payload } } }, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND $xs AS x WITH i+1 AS j,x AS y RETURN j,sum(y),count(y),min(y),max(y)",
        "WITH range(1,3) AS xs,range(5,2,-1) AS ys UNWIND (xs+ys) AS x RETURN min(x),max(x),count(DISTINCT x)",
        "UNWIND [1,2,3] AS i CALL {RETURN $s AS p} RETURN i,size(p),toUpper(p)",
        "UNWIND [1,2] AS i UNWIND $xs AS x WITH i,x+1 AS y RETURN i,sum(DISTINCT y),sum(y)",
        "UNWIND reverse(range(-2,3)+range(2,5)) AS x RETURN sum(x)",
        "UNWIND [1,2,3] AS i RETURN substring($s,i,1)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "fragmented arithmetic certificates fit a budget cheaper than pairwise proof" {
    const Builder = struct {
        fn append(a: std.mem.Allocator, text: *std.ArrayList(u8), first: usize, end: usize) !void {
            if (end - first == 1) {
                const leaf = try std.fmt.allocPrint(a, "range({d},{d})", .{ first, first });
                defer a.free(leaf);
                return text.appendSlice(a, leaf);
            }
            try text.append(a, '(');
            const middle = first + (end - first) / 2;
            try append(a, text, first, middle);
            try text.append(a, '+');
            try append(a, text, middle, end);
            try text.append(a, ')');
        }
    };
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.testing.allocator);
    try text.appendSlice(std.testing.allocator, "UNWIND ");
    try Builder.append(std.testing.allocator, &text, 0, 256);
    try text.appendSlice(std.testing.allocator, " AS x RETURN count(DISTINCT x)");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, text.items, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 3000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 256), result.rows[0][0].integer);
}

test "mapped immutable axes share transformed domains across groups" {
    const items = try std.testing.allocator.alloc(v.Value, 1000);
    defer std.testing.allocator.free(items);
    for (items, 0..) |*item, i| item.* = .{ .integer = @intCast(i) };
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    for ([_][]const u8{
        "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x+1 AS y RETURN i,count(y),sum(y),sum(DISTINCT y) ORDER BY i",
        "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x+1 AS y WITH i,y*2 AS z RETURN i,count(z),sum(z)/2,sum(DISTINCT z)/2 ORDER BY i",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = items } }}, .{ .memory_bytes = 4 << 20, .intermediate_states = 100000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 30), result.rows.len);
        for (result.rows, 1..) |row, i| {
            try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
            for ([_]i64{ 1000, 500500, 500500 }, row[1..]) |expected, value| try std.testing.expectEqual(expected, value.integer);
        }
    }
    try check("UNWIND [1,2,null] AS x WITH x+1 AS y RETURN count(y),sum(y)", "[[2,5]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x+i AS y RETURN i,sum(y) ORDER BY i", "[[1,5],[2,7]]");
    // A speculative mapping failure must not displace an earlier sibling error.
    var failed = try c.compile(std.testing.allocator, "UNWIND $xs AS x WITH 1/0 AS n,x+1 AS y RETURN min(n),sum(y)", .{}, &diagnostic);
    defer failed.deinit();
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &failed, &.{.{ .name = "xs", .value = .{ .list = &.{ .{ .integer = 1 }, .{ .integer = std.math.maxInt(i64) } } } }}, .{}));
}

test "distinct integer summaries preserve ordered support and overlapping domains" {
    try check("UNWIND [[1,2,2,null],[2,3,1],[3,4]] AS xs UNWIND xs AS x RETURN sum(DISTINCT x),count(DISTINCT x),collect(DISTINCT x)", "[[10,4,[1,2,3,4]]]");
    try check("UNWIND [[1,2],[2.0,3.0],[4]] AS xs UNWIND xs AS x RETURN sum(DISTINCT x),count(DISTINCT x)", "[[10,4]]");
    try check("UNWIND [[null],[],[null]] AS xs UNWIND xs AS x RETURN sum(DISTINCT x)", "[[0]]");
    try check("UNWIND [[1,2],[2,3],[1,2]] AS xs UNWIND xs AS x RETURN CASE WHEN false THEN sum(DISTINCT x) ELSE 7 END", "[[7]]");
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND $xs AS x RETURN sum(DISTINCT x)", .{}, &diagnostic);
    defer compiled.deinit();
    try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &.{ .{ .integer = std.math.maxInt(i64) }, .{ .integer = 1 }, .{ .integer = -1 } } } }}, .{}));
}

test "ordered arithmetic sums preserve prefix extrema and source bag multiplicity" {
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    for ([_]struct { text: []const u8, expected: i64 }{
        .{ .text = "UNWIND range(1,1000000000) AS x RETURN sum(x)", .expected = 500000000500000000 },
        .{ .text = "UNWIND reverse(range(-1000000000,1000000000)) AS x RETURN sum(x)", .expected = 0 },
        .{ .text = "WITH range(1,1000000) AS xs WITH xs,xs+xs AS ys UNWIND reverse(ys)+xs AS x RETURN sum(x)", .expected = 1500001500000 },
    }) |case| {
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 1 << 20, .intermediate_states = 1000 });
        defer result.deinit();
        try std.testing.expectEqual(case.expected, result.rows[0][0].integer);
    }
    for ([_][]const u8{
        "UNWIND range(-9223372036854775808,9223372036854775807) AS x RETURN sum(x)",
        "UNWIND range(9223372036854775807,9223372036854775807)+range(1,1)+range(-1,-1) AS x RETURN sum(x)",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    try check("UNWIND range(3,1) AS x RETURN sum(x)", "[[0]]");
    try check("UNWIND range(1,3) AS x RETURN sum(x),sum(0.1)", "[[6,0.30000000000000004]]");
    // Compare independent enumerating reducers over generated source geometries.
    var random = std.Random.DefaultPrng.init(0x8a31e7);
    for (0..150) |_| {
        const start = random.random().intRangeAtMost(i64, -100, 100);
        const end = random.random().intRangeAtMost(i64, -100, 100);
        const step: i64 = if (random.random().boolean()) 3 else -2;
        const text = try std.fmt.allocPrint(std.testing.allocator, "UNWIND reverse(range({d},{d},{d})+range(-5,7)) AS x RETURN sum(x)", .{ start, end, step });
        defer std.testing.allocator.free(text);
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        var expected: i64 = 13;
        var value = start;
        while (if (step > 0) value <= end else value >= end) : (value += step) expected += value;
        try std.testing.expectEqual(expected, result.rows[0][0].integer);
    }
}

test "dynamic string windows reuse root validation and bounded Unicode offsets" {
    const ascii = try std.testing.allocator.alloc(u8, 1000000);
    defer std.testing.allocator.free(ascii);
    @memset(ascii, 'x');
    const unicode = try std.testing.allocator.alloc(u8, 1200000);
    defer std.testing.allocator.free(unicode);
    for (0..unicode.len / 6) |i| @memcpy(unicode[i * 6 ..][0..6], "é🙂");
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND range(100000,100099) AS i RETURN substring($s,i,1)", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_][]const u8{ ascii, unicode }, 0..) |text, kind| {
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "s", .value = .{ .string = text } }}, .{ .memory_bytes = 4 << 20, .intermediate_states = 150000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 100), result.rows.len);
        for (result.rows, 0..) |row, i| try std.testing.expectEqualStrings(if (kind == 0) "x" else if (i % 2 == 0) "é" else "🙂", row[0].string);
    }
}

test "shared mapped and distinct summaries match sequential reducers over generated domains" {
    const Compare = struct {
        fn run(text: []const u8, parameters: []const v.Entry) !void {
            var diagnostic: c.Diagnostic = .{};
            var fast = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
            defer fast.deinit();
            const control = try std.fmt.allocPrint(std.testing.allocator, "{s},sum(0)+size(coalesce(collect(null),[]))", .{text});
            defer std.testing.allocator.free(control);
            var slow = try c.compile(std.testing.allocator, control, .{}, &diagnostic);
            defer slow.deinit();
            var fixture: Fixture = .{};
            var actual = runtime.execute(std.testing.allocator, fixture.backend(), graph, &fast, parameters, .{}) catch |err| {
                try std.testing.expectError(err, runtime.execute(std.testing.allocator, fixture.backend(), graph, &slow, parameters, .{}));
                return;
            };
            defer actual.deinit();
            var expected = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &slow, parameters, .{});
            defer expected.deinit();
            try std.testing.expectEqual(expected.rows.len, actual.rows.len);
            for (actual.rows, expected.rows) |a, e| for (a, e[0..a.len]) |left, right| {
                try std.testing.expectEqual(std.meta.activeTag(right), std.meta.activeTag(left));
                try std.testing.expect(v.Value.eql(left, right));
            };
        }
    };
    var random = std.Random.DefaultPrng.init(0x7e91a2);
    const choices = [_]v.Value{ .null, .{ .integer = 0 }, .{ .integer = -2 }, .{ .integer = 3 }, .{ .integer = std.math.maxInt(i64) }, .{ .integer = std.math.minInt(i64) }, .{ .number = 1e16 }, .{ .number = 2.5 }, .{ .number = 3 }, .{ .boolean = true }, .{ .string = "a" } };
    for (0..180) |iteration| {
        var cells: [24]v.Value = undefined;
        for (&cells) |*cell| cell.* = choices[random.random().uintLessThan(usize, if (iteration < 80) 4 else choices.len)];
        const lists = [_]v.Value{ .{ .list = cells[0..8] }, .{ .list = cells[8..16] }, .{ .list = cells[16..24] }, .{ .list = cells[0..8] } };
        const parameters: []const v.Entry = &.{ .{ .name = "lists", .value = .{ .list = &lists } }, .{ .name = "groups", .value = .{ .list = cells[0..3] } }, .{ .name = "unused", .value = .{ .string = "primitive" } } };
        for ([_][]const u8{
            "UNWIND $lists AS xs UNWIND xs AS x RETURN sum(DISTINCT x),count(DISTINCT x)",
            "UNWIND $lists AS xs UNWIND xs AS x WITH x+1 AS y RETURN sum(y),count(y)",
            "UNWIND $lists AS xs UNWIND xs AS x WITH x*2 AS y RETURN sum(DISTINCT y),count(DISTINCT y)",
            "UNWIND $lists AS xs UNWIND xs AS x RETURN CASE WHEN false THEN sum(DISTINCT x) ELSE 7 END",
        }) |text| try Compare.run(text, parameters);
    }
}

test "synthetic mapped domains do not consume the user value depth allowance" {
    var levels: [65]v.Value = undefined;
    levels[0] = .{ .integer = 1 };
    for (1..levels.len) |i| levels[i] = .{ .list = levels[i - 1 .. i] };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND $xs AS x WITH [x] AS y RETURN count(y)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = levels[64] }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
}

test "range sum fallback cursors retain reversal when shape proof is depth bounded" {
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(std.testing.allocator);
    try text.appendSlice(std.testing.allocator, "UNWIND ");
    for (0..65) |_| try text.appendSlice(std.testing.allocator, "reverse(");
    try text.appendSlice(std.testing.allocator, "range(-9223372036854775808,9223372036854775806,9223372036854775807)");
    for (0..65) |_| try text.append(std.testing.allocator, ')');
    try text.appendSlice(std.testing.allocator, " AS x RETURN sum(x)");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, text.items, .{ .max_depth = 256 }, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, -3), result.rows[0][0].integer);
}

test "ordered reduction memos cover multiple outputs floats reversals and overlapping domains" {
    const a = std.testing.allocator;
    const xs = try a.alloc(v.Value, 1000);
    defer a.free(xs);
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    const cases = [_]struct { text: []const u8, expected: []const v.Value, floating: bool = false }{
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x+1 AS y,x*2 AS z RETURN i,sum(y),sum(z) ORDER BY i", .expected = &.{ .{ .integer = 500500 }, .{ .integer = 999000 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,avg(x) ORDER BY i", .expected = &.{.{ .number = 499.5 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {RETURN avg(x) AS y} RETURN i,avg(y) ORDER BY i", .expected = &.{.{ .number = 499.5 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x ORDER BY x DESC RETURN i,avg(x),head(collect(x))", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 999 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x ORDER BY x DESC RETURN i,avg(x) AS a,head(collect(x)) ORDER BY a", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 999 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x ORDER BY x DESC WITH i,x RETURN i,avg(x),head(collect(x)) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 999 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x ORDER BY x WITH i,x ORDER BY x DESC RETURN i,avg(x),head(collect(x)) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 999 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {UNWIND [x,x+1] AS y RETURN y} RETURN i,sum(y) ORDER BY i", .expected = &.{.{ .integer = 1000000 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {RETURN x*2 AS y LIMIT 1} RETURN i,avg(y) ORDER BY i", .expected = &.{.{ .number = 999 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {WITH DISTINCT x RETURN x*2 AS y} RETURN i,avg(y) ORDER BY i", .expected = &.{.{ .number = 999 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH * WHERE x>500 RETURN i,avg(x) ORDER BY i", .expected = &.{.{ .number = 750 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,avg(x),any(v IN collect(x) WHERE v>500) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .boolean = true } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,avg(x),head([v IN collect(x) WHERE v>500]) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 501 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,avg(x),reduce(s=0,v IN collect(x)|s+v) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 499500 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x ORDER BY x DESC RETURN i,avg(x),head(collect(x)) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 999 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {RETURN x*2 AS y} RETURN i,avg(y) ORDER BY i", .expected = &.{.{ .number = 999 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {WITH * RETURN x*2 AS y} RETURN i,avg(y) ORDER BY i", .expected = &.{.{ .number = 999 }} },

        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x CALL {WITH x AS t RETURN t*2 AS y,7 AS z} RETURN i,avg(y),min(z) ORDER BY i", .expected = &.{ .{ .number = 999 }, .{ .integer = 7 } } },

        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x WHERE x>500 RETURN i,avg(x) ORDER BY i", .expected = &.{.{ .number = 750 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x WITH DISTINCT i,x RETURN i,avg(x) ORDER BY i", .expected = &.{.{ .number = 499.5 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,avg(x),head(collect(x)),last(collect(x)) ORDER BY i", .expected = &.{ .{ .number = 499.5 }, .{ .integer = 0 }, .{ .integer = 999 } } },

        .{ .text = "UNWIND range(1,30) AS i UNWIND reverse($xs) AS x RETURN i,sum(x) ORDER BY i", .expected = &.{.{ .integer = 499500 }} },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $lists AS xs UNWIND xs AS x RETURN i,sum(DISTINCT x),count(DISTINCT x) ORDER BY i", .expected = &.{ .{ .integer = 280875 }, .{ .integer = 750 } } },
        .{ .text = "UNWIND range(1,30) AS i UNWIND $xs AS x RETURN i,sum(x) ORDER BY i", .expected = &.{.{ .number = 49950 }}, .floating = true },
    };
    for (cases) |case| {
        for (xs, 0..) |*value, i| value.* = if (case.floating) .{ .number = @as(f64, @floatFromInt(i)) / 10 } else .{ .integer = @intCast(i) };
        const lists = [_]v.Value{ .{ .list = xs[0..500] }, .{ .list = xs[250..750] } };
        var compiled = try c.compile(a, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{ .{ .name = "xs", .value = .{ .list = xs } }, .{ .name = "lists", .value = .{ .list = &lists } } }, .{ .memory_bytes = 4 << 20, .intermediate_states = 100000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 30), result.rows.len);
        for (result.rows, 1..) |row, i| {
            try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
            for (row[1..], case.expected) |actual, expected| {
                try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(actual));
                try std.testing.expect(v.Value.eql(expected, actual));
            }
        }
    }
}

test "ordered memo traces preserve reached scalar errors and empty tails" {
    var fixture: Fixture = .{};
    var diagnostic: c.Diagnostic = .{};
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1] AS x WITH i,1/0 AS bad,x AS y UNWIND [] AS z RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,1/0 AS bad,1/(x-1) AS y RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x+1 AS y,1/(i-2) AS bad RETURN i,avg(y)",
    }) |text| {
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    try check("UNWIND [1,2] AS i UNWIND [] AS x WITH i,1/0 AS bad,x AS y RETURN i,avg(y)", "[]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x RETURN i,CASE WHEN false THEN avg('bad') ELSE 7 END", "[[1,7],[2,7]]");
    try check("UNWIND [1,1,2] AS i UNWIND [1.0,2.0,3.0] AS x RETURN i,avg(x),sum(x) ORDER BY i", "[[1,2,12],[2,2,6]]");
    try check("UNWIND [1,1,2] AS i UNWIND [[1,2],[2,3],[1.0,4]] AS xs UNWIND xs AS x RETURN i,avg(DISTINCT x),sum(DISTINCT x),count(DISTINCT x) ORDER BY i", "[[1,2.5,10,4],[2,2.5,10,4]]");
}

test "ordered reduction snapshots match independent sequential execution across partitions" {
    const Compare = struct {
        fn run(text: []const u8, parameters: []const v.Entry) !void {
            const a = std.testing.allocator;
            var diagnostic: c.Diagnostic = .{};
            var fast = try c.compile(a, text, .{}, &diagnostic);
            defer fast.deinit();
            const returned = std.mem.lastIndexOf(u8, text, " RETURN ") orelse 0;
            var split = text.len;
            for ([_][]const u8{ " ORDER BY ", " SKIP ", " LIMIT " }) |keyword| if (std.mem.lastIndexOf(u8, text, keyword)) |position| {
                if (position > returned) split = @min(split, position);
            };
            const control = try std.fmt.allocPrint(a, "{s},sum(0)+size(coalesce(collect(null),[])){s}", .{ text[0..split], text[split..] });
            defer a.free(control);
            var slow = try c.compile(a, control, .{}, &diagnostic);
            defer slow.deinit();
            var fixture: Fixture = .{};
            var actual = runtime.execute(a, fixture.backend(), graph, &fast, parameters, .{}) catch |err| {
                try std.testing.expectError(err, runtime.execute(a, fixture.backend(), graph, &slow, parameters, .{}));
                return;
            };
            defer actual.deinit();
            var expected = try runtime.execute(a, fixture.backend(), graph, &slow, parameters, .{});
            defer expected.deinit();
            try std.testing.expectEqual(expected.rows.len, actual.rows.len);
            for (actual.rows, expected.rows) |left, right| for (left, right[0..left.len]) |x, y| {
                try std.testing.expectEqual(std.meta.activeTag(y), std.meta.activeTag(x));
                if (x == .number) try std.testing.expectEqual(@as(u64, @bitCast(y.number)), @as(u64, @bitCast(x.number))) else try std.testing.expect(v.Value.eql(x, y));
            };
        }
    };
    var random = std.Random.DefaultPrng.init(0xa328d4);
    const choices = [_]v.Value{ .null, .{ .integer = 0 }, .{ .integer = -2 }, .{ .integer = 3 }, .{ .integer = std.math.maxInt(i64) }, .{ .integer = std.math.minInt(i64) }, .{ .number = 1e16 }, .{ .number = 2.5 }, .{ .number = 3 }, .{ .number = -0.0 }, .{ .boolean = true }, .{ .string = "bad" } };
    for (0..160) |iteration| {
        var cells: [18]v.Value = undefined;
        for (&cells) |*value| value.* = choices[random.random().uintLessThan(usize, if (iteration < 60) 4 else choices.len)];
        const lists = [_]v.Value{ .{ .list = cells[0..6] }, .{ .list = cells[6..12] }, .{ .list = cells[12..] }, .{ .list = cells[0..6] } };
        const parameters: []const v.Entry = &.{ .{ .name = "lists", .value = .{ .list = &lists } }, .{ .name = "groups", .value = .{ .list = cells[0..3] } }, .{ .name = "unused", .value = .{ .string = "primitive" } } };
        for ([_][]const u8{
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x RETURN i,sum(x),avg(x),sum(DISTINCT x),avg(DISTINCT x),count(DISTINCT x)",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {RETURN avg(x) AS y} RETURN i,avg(y),head(collect(y)) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH i,avg(x) AS y RETURN y} RETURN i,avg(y),head(collect(y)) ORDER BY i",
            "UNWIND $groups AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH i,avg(x) AS y RETURN y} RETURN i,avg(y),head(collect(y)) ORDER BY i",
            "WITH $groups AS gs UNWIND gs AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH i AS j,x CALL {WITH j,avg(x) AS y RETURN y} RETURN y} RETURN i,avg(y) ORDER BY i",
            "WITH $unused AS dead UNWIND $groups AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH * WITH i,count(x) AS y RETURN y} RETURN i,count(y) ORDER BY i LIMIT 1",

            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {UNWIND [x,x+1] AS k WITH i,sum(k) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {UNWIND CASE WHEN x IS NULL THEN [] ELSE [x] END AS k WITH i,count(k) AS n RETURN n} RETURN i,sum(n) ORDER BY i",
            "UNWIND [3,1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {RETURN x AS y} RETURN i,count(y) ORDER BY i LIMIT 1",
            "UNWIND [3,1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {CALL {RETURN x AS z} RETURN z AS y} RETURN i,count(y) ORDER BY i%2 SKIP 1 LIMIT 1",

            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH x WHERE x>0 RETURN count(*) AS n} RETURN i,n,count(*) ORDER BY i,n",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {CALL {WITH x WHERE x>0 RETURN 7 AS y} RETURN count(*) AS n} RETURN i,n,count(*) ORDER BY i,n",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {UNWIND [1,2] AS k WITH k,count(*) AS n RETURN n} RETURN i,sum(n),head(collect(n)) ORDER BY i",
            "UNWIND [3,1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC RETURN i,avg(x),head(collect(x))",
            "UNWIND [3,1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC RETURN i,avg(x) AS a,head(collect(x)) ORDER BY a",
            "UNWIND [3,1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC RETURN i,count(x) ORDER BY i LIMIT 1",
            "UNWIND [3,1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,i*2 AS j,x ORDER BY x DESC RETURN i,j,head(collect(x)) LIMIT 2",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC WITH i,x RETURN i,avg(x),head(collect(x)),last(collect(x)) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x WITH i,x ORDER BY x DESC RETURN i,avg(x),head(collect(x)) ORDER BY i",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {UNWIND [x,x+1] AS y RETURN y} RETURN i,sum(y),avg(y),head(collect(y)),last(collect(y)) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH * CALL {UNWIND [x,7] AS t RETURN t} RETURN t AS y} RETURN i,avg(y),head(collect(y)) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {RETURN x*2 AS y LIMIT 1} RETURN i,avg(y) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {WITH DISTINCT x RETURN x*2 AS y} RETURN i,avg(y) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {UNWIND [x,7] AS t WITH t ORDER BY t DESC RETURN t AS y SKIP 1 LIMIT 1} RETURN i,head(collect(y)),last(collect(y)) ORDER BY i",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x+1 AS y,x*2 AS z RETURN i,sum(y),avg(z)",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND reverse(xs) AS x RETURN i,sum(x),avg(x),sum(DISTINCT x)",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x RETURN i,CASE WHEN i=2 THEN avg(x) ELSE 7 END",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH * WHERE x IS NOT NULL RETURN i,avg(x),count(*)",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x RETURN i,avg(x),any(v IN collect(x) WHERE v>1),reduce(s=0,v IN collect(x)|s+v),head([v IN collect(x) WHERE v>1])",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x RETURN i,avg(x),any(v IN collect(DISTINCT x) WHERE v>1),reduce(s=0,v IN collect(DISTINCT x)|s+v),last([v IN collect(DISTINCT x) WHERE v>1])",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {RETURN x*2 AS y,7 AS z} RETURN i,avg(y),min(z)",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x CALL {CALL {RETURN x AS t} WITH t WHERE t IS NOT NULL RETURN CASE WHEN t>0 THEN t ELSE 7 END AS y} RETURN i,avg(y),head(collect(y))",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC RETURN i,sum(x),avg(x),head(collect(x)),last(collect(x)) ORDER BY i",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC RETURN i,sum(x),avg(x),head(collect(x)),last(collect(x)) ORDER BY i",
            "UNWIND [1,2] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x ORDER BY x DESC RETURN i,avg(x+i) ORDER BY i",

            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH DISTINCT i,x RETURN i,avg(x),count(*)",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x WHERE x IS NOT NULL AND x>0 RETURN i,sum(x),avg(x),head(collect(x)),last(collect(x))",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH DISTINCT i,x,x*2 AS y WITH i,x,y WHERE x>0 RETURN i,avg(x),head(collect(y)),last(collect(y))",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x RETURN i,avg(x),collect(DISTINCT x)[0..2],collect(DISTINCT x)[-2..]",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH DISTINCT i,x WITH DISTINCT i,x RETURN i,count(*),collect(x)[0..2],collect(x)[-2..]",
            "UNWIND [1,2,1] AS i UNWIND $lists AS xs UNWIND xs AS x WITH i,x AS y WHERE x IS NOT NULL RETURN i,avg(y),head(collect(y))",
        }) |text| try Compare.run(text, parameters);
    }
}

test "ordered reduction memo owners release every failed allocation" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1,2,3] AS x WITH i,x+1 AS y,x*2 AS z RETURN i,sum(y),avg(z)",
        "UNWIND [1,2] AS i UNWIND [[1,2],[2,3]] AS xs UNWIND xs AS x RETURN i,sum(DISTINCT x),avg(DISTINCT x),count(DISTINCT x)",

        "UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x WITH DISTINCT i,x RETURN i,avg(x),head(collect(x)),last(collect(x))",
        "UNWIND [1,2] AS i UNWIND [1,2,3] AS x WITH i,x WHERE x>1 RETURN i,avg(x),collect(DISTINCT x)[0..1],collect(DISTINCT x)[-1..]",
        "UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x WITH * RETURN i,avg(x),any(v IN collect(DISTINCT x) WHERE v>1),reduce(s=0,v IN collect(DISTINCT x)|s+v),head([v IN collect(x) WHERE v>1])",
        "UNWIND [1,2] AS i UNWIND [1,2,3] AS x CALL {WITH x AS t RETURN t*2 AS y,7 AS z} RETURN i,avg(y),min(z)",
        "UNWIND [1,2] AS i UNWIND [1,2,3] AS x CALL {WITH * CALL {RETURN x AS t} RETURN t*2 AS y} RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [3,1,2] AS x WITH i,x ORDER BY x DESC RETURN i,avg(x),head(collect(x)) ORDER BY i",
        "UNWIND [1,2] AS i UNWIND [3,1,2] AS x WITH i,x ORDER BY x WITH i,x ORDER BY x DESC WITH i,x RETURN i,avg(x),head(collect(x)) ORDER BY i",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [x,x+1] AS y RETURN y} RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH DISTINCT x RETURN x AS y LIMIT 1} RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x AS y WHERE y>1 RETURN i,count(y) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x,x*2 AS unused RETURN i,count(x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {RETURN avg(x) AS y} RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [1,2] AS k WITH k,count(*) AS n RETURN n} RETURN i,sum(n)",
        "UNWIND [3,1,2] AS i UNWIND [1,2] AS x WITH i,[i,7] AS j,x ORDER BY x DESC RETURN i,j,head(collect(x))",
        "UNWIND [3,1,2] AS i UNWIND [1,2] AS x WITH i,x ORDER BY x RETURN i,count(1/x) ORDER BY i LIMIT 1",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "partitioned distinct and collection snapshots retain filters and contribution order" {
    try check("UNWIND [[1,2],[2,3],[3,4]] AS xs UNWIND [1,2] AS i UNWIND xs AS x WITH DISTINCT i,x RETURN i,avg(x),count(*) ORDER BY i", "[[1,2.5,4],[2,2.5,4]]");
    try check("UNWIND [[1,2],[3,4],[1,2]] AS xs UNWIND [1,2] AS i UNWIND xs AS x WITH i,x WHERE x<3 WITH DISTINCT i,x RETURN i,count(*) ORDER BY i", "[[1,2],[2,2]]");
    try check("UNWIND [1,2,1] AS i UNWIND [[1,2],[2,3]] AS xs UNWIND xs AS x WITH DISTINCT i,x RETURN i,count(*),collect(x) ORDER BY i", "[[1,3,[1,2,3]],[2,3,[1,2,3]]]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2,2,3] AS x WITH DISTINCT i,x WITH DISTINCT i,x RETURN i,avg(x),head(collect(x)),last(collect(x)) ORDER BY i", "[[1,2,1,3],[2,2,1,3]]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x RETURN i,collect(x)[0..2],collect(x)[-2..] ORDER BY i", "[[1,[1,2],[2,3]],[2,[1,2],[2,3]]]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x RETURN i,collect(DISTINCT x)[0..2],collect(DISTINCT x)[-2..] ORDER BY i", "[[1,[1,2],[2,3]],[2,[1,2],[2,3]]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x WITH DISTINCT i UNWIND [] AS y RETURN i,count(*)", "[]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2] AS x WITH i,x WHERE i=1 RETURN i,avg(x)", "[[1,1.5]]");
    try check("UNWIND [1,2] AS k UNWIND [10,20] AS i UNWIND [1,2,3] AS x WITH i,x WHERE x>k RETURN i,avg(x),head(collect(x)) ORDER BY i", "[[10,2.6666666666666665,2],[20,2.6666666666666665,2]]");
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1] AS x WITH i,x,1/0 AS bad WHERE false RETURN i,avg(x)",
        "UNWIND [1,2] AS i UNWIND [1] AS x WITH DISTINCT i,x,1/0 AS bad UNWIND [] AS y RETURN i,avg(x)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "normalized scalar scopes and ordered certificates preserve barriers and lazy failures" {
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH * CALL {RETURN x AS z} RETURN z*2 AS y} RETURN i,avg(y) ORDER BY i", "[[1,3],[2,3]]");
    try check("UNWIND [2,1] AS i UNWIND [3,1,2] AS x WITH *,x*2 AS y WHERE y>2 RETURN i,avg(y) ORDER BY i", "[[1,5],[2,5]]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x CALL {WITH x AS t WHERE t>1 RETURN t*2 AS y,7 AS z} RETURN i,avg(y),z ORDER BY i", "[[1,5,7],[2,5,7]]");
    try check("UNWIND [1,2] AS i UNWIND [1] AS x CALL {WITH x WHERE false RETURN 1/0 AS y} RETURN i,avg(y)", "[]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x RETURN i,reduce(s=0,v IN collect(x)|s+v),any(v IN collect(x) WHERE v>2),head([v IN collect(x) WHERE v>1]) ORDER BY i", "[[1,12,true,2],[2,6,true,2]]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2,3] AS x RETURN i,reduce(s=0,v IN collect(DISTINCT x)|s+v),any(v IN collect(DISTINCT x) WHERE v>2) ORDER BY i", "[[1,6,true],[2,6,true]]");
    try check("UNWIND [1,2] AS i UNWIND [null] AS x RETURN i,reduce(s=7,v IN collect(x)|s+v),any(v IN collect(x) WHERE v>2),head([v IN collect(x) WHERE v>1]) ORDER BY i", "[[1,7,false,null],[2,7,false,null]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x RETURN i,CASE WHEN false THEN reduce(s=1/0,v IN collect(x)|s+v) ELSE 7 END ORDER BY i", "[[1,7],[2,7]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x ORDER BY x DESC RETURN i,head(collect(x)),last(collect(x)) ORDER BY i", "[[1,2,1],[2,2,1]]");
    try check("UNWIND [1,2,1] AS i UNWIND [1,2] AS x WITH i,x ORDER BY x DESC RETURN i,collect(x) ORDER BY i", "[[1,[2,2,1,1]],[2,[2,1]]]");
    try check("UNWIND [1,1.0,2] AS i UNWIND [1,2] AS x WITH i,x ORDER BY x DESC RETURN i,count(*) ORDER BY i", "[[1,4],[2,2]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x ORDER BY x DESC LIMIT 1 RETURN i,count(*) ORDER BY i", "[[1,1]]");
    try check("UNWIND [1,2] AS i UNWIND [] AS x WITH i,x,1/0 AS bad ORDER BY x RETURN i,avg(x) ORDER BY i", "[]");
    {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH * CALL {RETURN 7 AS x} RETURN 1 AS y} RETURN i,avg(y)", .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherVariableConflict, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,1/0 AS bad,x ORDER BY x RETURN i,avg(x) ORDER BY i",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {RETURN 1/0 AS y} RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH *,1/0 AS bad RETURN x AS y} RETURN i,avg(y)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "ordered domain cache admission does not add depth to captured values" {
    var cells: [65]v.Value = undefined;
    cells[0] = .{ .integer = 7 };
    for (1..cells.len) |index| cells[index] = .{ .list = cells[index - 1 .. index] };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND [1,2] AS i UNWIND $xs AS x WITH i,x ORDER BY x RETURN i,count(*) ORDER BY i", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = cells[64] }}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    for (result.rows) |row| try std.testing.expectEqual(@as(i64, 1), row[1].integer);
}

test "group windows share certified successful reduction domains" {
    const a = std.testing.allocator;
    const xs = try a.alloc(v.Value, 1000);
    defer a.free(xs);
    for (xs, 0..) |*cell, index| cell.* = .{ .integer = @intCast(index) };
    var diagnostic: c.Diagnostic = .{};
    for ([_][]const u8{
        "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x+1 AS y WHERE y>100 RETURN i,count(y) ORDER BY i LIMIT 5",
        "UNWIND range(1,30) AS i UNWIND $xs AS x WITH i,x+1 AS y WHERE y>100 WITH i,y ORDER BY y DESC RETURN i,count(y) ORDER BY i LIMIT 5",
    }) |text| {
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = xs } }}, .{ .memory_bytes = 4 << 20, .intermediate_states = 100000 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5), result.rows.len);
        for (result.rows, 1..) |row, index| {
            try std.testing.expectEqual(@as(i64, @intCast(index)), row[0].integer);
            try std.testing.expectEqual(@as(i64, 900), row[1].integer);
        }
    }
}

test "scoped expanding domains and group windows retain boundaries and errors" {
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [1,2] AS y RETURN y} RETURN i,y,count(*) ORDER BY i,y", "[[1,1,2],[1,2,2],[2,1,2],[2,2,2]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {RETURN x AS y LIMIT 0} RETURN i,count(y)", "[]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH x WHERE false RETURN 1/0 AS y LIMIT 1} RETURN i,count(y)", "[]");
    try check("UNWIND [3,1,2,1] AS i UNWIND [1,2] AS x WITH i,x+1 AS y WHERE y>1 RETURN i,count(y) ORDER BY i LIMIT 1", "[[1,4]]");
    try check("UNWIND [3,1,2] AS i UNWIND [1,2] AS x WITH i,x+1 AS y WHERE y>1 RETURN i,count(y) ORDER BY i LIMIT 0", "[]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x ORDER BY x DESC WITH i,x ORDER BY x LIMIT 1 RETURN i,count(*) ORDER BY i", "[[1,1]]");
    for ([_][]const u8{
        "UNWIND [3,1,2] AS i UNWIND [1,0] AS x RETURN i,count(1/x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x,1/(x-1) AS bad RETURN i,count(x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x,1/(x-1) AS bad RETURN i,avg(x) ORDER BY i",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x,1/(x-1) AS bad WHERE false RETURN i,avg(x)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x,1/(x-1) AS bad UNWIND [] AS z RETURN i,avg(x)",
        "UNWIND [3,1,2] AS i UNWIND [1,0] AS x RETURN i,count(1/x) ORDER BY i LIMIT 0",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {RETURN 1/0 AS y LIMIT 0} RETURN i,avg(y)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,1/0 AS bad,x ORDER BY x WITH i,x RETURN i,avg(x) ORDER BY i",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "local reducer cardinality and ordered discovery retain scoped semantics" {
    try check("UNWIND [3,1,2] AS i UNWIND [1,2] AS x WITH i,[i,7] AS j,x ORDER BY x DESC RETURN i,j,head(collect(x))", "[[3,[3,7],2],[1,[1,7],2],[2,[2,7],2]]");
    try check("UNWIND [3,1,2] AS i UNWIND [1,2] AS x WITH i,{k:i} AS j,x ORDER BY x DESC RETURN i,j,head(collect(x))", "[[3,{\"k\":3},2],[1,{\"k\":1},2],[2,{\"k\":2},2]]");
    try check("UNWIND [1,2] AS i UNWIND [0,1,2] AS x CALL {WITH x WHERE x>0 RETURN count(*) AS n} RETURN i,n,count(*) ORDER BY i,n", "[[1,0,1],[1,1,2],[2,0,1],[2,1,2]]");
    try check("UNWIND [1,2] AS i UNWIND [0,1,2] AS x CALL {CALL {WITH x WHERE x>0 RETURN 7 AS y} RETURN count(*) AS n} RETURN i,n,count(*) ORDER BY i,n", "[[1,0,1],[1,1,2],[2,0,1],[2,1,2]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [1,2] AS k WITH k,count(*) AS n RETURN n} RETURN i,count(*) ORDER BY i", "[[1,4],[2,4]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [] AS k RETURN count(*) AS n} RETURN i,sum(n) ORDER BY i", "[[1,0],[2,0]]");
    try check("UNWIND [3,1,2] AS i UNWIND [1,2,1] AS x WITH i,x ORDER BY x DESC RETURN i,head(collect(x))", "[[3,2],[1,2],[2,2]]");
    try check("UNWIND [3,1,2] AS i UNWIND [1,2,1] AS x WITH i,x ORDER BY x DESC RETURN i,head(collect(x)) AS a ORDER BY a LIMIT 2", "[[3,2],[1,2]]");
    try check("UNWIND [3,1,2] AS i UNWIND [1,2,1] AS x WITH i,x ORDER BY x DESC RETURN i,count(x) ORDER BY i LIMIT 1", "[[1,3]]");
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1,0] AS x WITH i,x ORDER BY x RETURN i,count(1/x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [1,0] AS x WITH i,x ORDER BY x RETURN i,count(1/x) ORDER BY i LIMIT 0",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x WITH i,x,CASE WHEN i=2 THEN 1/0 ELSE 1 END AS a ORDER BY x WITH i,x,CASE WHEN i=1 THEN true+1 ELSE 1 END AS b RETURN i,avg(x)",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {RETURN 1/0+avg(x) AS y} RETURN i,avg(y)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "invocation constant grouping keys preserve empty cardinality and varying keys" {
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH i,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i", "[[1,1.5],[2,1.5]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [] AS k WITH i,count(k) AS n RETURN n} RETURN i,count(*) ORDER BY i", "[]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [] AS k WITH count(k) AS n RETURN n} RETURN i,sum(n) ORDER BY i", "[[1,0],[2,0]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH i AS j UNWIND [1,2] AS k WITH j,count(k) AS n RETURN n} RETURN i,sum(n) ORDER BY i", "[[1,4],[2,4]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [1,2] AS k WITH k,count(*) AS n RETURN n} RETURN i,sum(n) ORDER BY i", "[[1,4],[2,4]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [1,2] AS k WITH k AS j WITH j,count(*) AS n RETURN n} RETURN i,count(*) ORDER BY i", "[[1,4],[2,4]]");
    try check("UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {UNWIND [x,x+1] AS k WITH i,count(k) AS n WHERE i=1 RETURN n} RETURN i,sum(n) ORDER BY i", "[[1,4]]");
}

test "singleton CALL group windows retain filters global reducers ties and errors" {
    try check("UNWIND [3,1,2,3,1] AS i CALL {RETURN i+1 AS y} RETURN i,count(y) ORDER BY i LIMIT 2", "[[1,2],[2,1]]");
    try check("UNWIND [3,1,2,3,1] AS i CALL {CALL {RETURN i+1 AS z} RETURN z+1 AS y} RETURN i,count(y) ORDER BY i%2 LIMIT 2", "[[2,1],[3,2]]");
    try check("UNWIND [1,2,3] AS i CALL {WITH i WHERE i=2 RETURN i+1 AS y} RETURN i,count(y) ORDER BY i LIMIT 1", "[[2,1]]");
    try check("UNWIND [1,2,3] AS i CALL {WITH i WHERE i=2 RETURN count(*) AS y} RETURN i,count(y) ORDER BY i LIMIT 1", "[[1,1]]");
    for ([_][]const u8{
        "UNWIND [1,2] AS i CALL {RETURN 1/(2-i) AS y} RETURN i,count(y) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i CALL {WITH i,1/(2-i) AS z RETURN count(z) AS y} RETURN i,count(y) ORDER BY i LIMIT 0",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "COUNT group windows require logical multiplicity certificates" {
    // Physical work is not a logical count bound. Retry an unpublished fold
    // without eviction so symbolic counts report their exact overflow.
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND range(0,CASE WHEN i=2 THEN 9223372036854775807 ELSE 0 END) AS x RETURN i,count(*) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND range(0,CASE WHEN i=2 THEN 9223372036854775807 ELSE 0 END) AS x RETURN i,count(*) ORDER BY i LIMIT 0",
        "UNWIND [1,2] AS i UNWIND range(0,CASE WHEN i=2 THEN 9223372036854775807 ELSE 0 END) AS x CALL {RETURN 7 AS y} RETURN i,count(y) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND range(0,CASE WHEN i=2 THEN 9223372036854775807 ELSE 0 END) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN i,count(y) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i CALL {UNWIND range(0,CASE WHEN i=2 THEN 9223372036854775807 ELSE 0 END) AS x RETURN 7 AS y} RETURN i,count(y) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND range(1,1000000000000000) AS x UNWIND range(1,1000000000000000) AS y RETURN i,count(*) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND range(-9223372036854775807-1,9223372036854775807) AS x RETURN i,count(*) ORDER BY i LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000 }));
    }
    // Safe compression remains cheap even when logical rows exceed the work
    // budget; physical contributions each have a certified bounded count.
    try check("UNWIND [1,2] AS i UNWIND range(1,1000000) AS x RETURN i,count(*) ORDER BY i LIMIT 1", "[[1,1000000]]");
    try check("UNWIND [3,1,2,1] AS i UNWIND range(1,CASE WHEN i=3 THEN 1 ELSE 1000000000000000 END) AS x RETURN i,count(*) ORDER BY i LIMIT 1", "[[1,2000000000000000]]");
}

test "constant grouping normalization and singleton CALL windows release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            // Keep failure positions independent of adjacent allocator activity.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expect(result.rows.len != 0);
        }
    };
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH i,avg(x) AS y RETURN y} RETURN i,avg(y)",
        "UNWIND [3,1,2] AS i CALL {CALL {RETURN i+1 AS z} RETURN z AS y} RETURN i,count(y) ORDER BY i LIMIT 1",
        "UNWIND [3,1,2,1] AS i UNWIND range(1,CASE WHEN i=3 THEN 1 ELSE 1000000000000000 END) AS x RETURN i,count(*) ORDER BY i LIMIT 1",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "constant entity keys retain backend reads even when their values are dead" {
    const Host = struct {
        fn project(_: *anyopaque, _: std.mem.Allocator, _: *const v.Node) anyerror!?*const v.Node {
            return error.CypherInvalidArgument;
        }
    };
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH $node AS dead,x WITH dead,count(x) AS y RETURN y} RETURN i,sum(y) ORDER BY i",
        "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH $node AS dead,x WITH dead,x RETURN count(x) AS y} RETURN i,sum(y) ORDER BY i",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        callbacks.project_node = Host.project;
        try std.testing.expectError(error.CypherInvalidArgument, runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{.{ .name = "node", .value = .{ .node = &Fixture.nodes[0] } }}, .{}));
    }
}

test "CALL count windows preserve middle stage error precedence over argument errors" {
    for ([_][]const u8{
        "UNWIND [1,2] AS i UNWIND [0] AS x CALL {RETURN CASE WHEN i=2 THEN true+1 ELSE 1 END AS y} RETURN i,count(1/x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [0] AS x CALL {RETURN CASE WHEN i=2 THEN true+1 ELSE 1 END AS y} RETURN i,count(1/x) ORDER BY i LIMIT 0",
        "UNWIND [1,3,2] AS i UNWIND [CASE WHEN i=3 THEN 0 ELSE 1 END] AS x CALL {RETURN CASE WHEN i=2 THEN true+1 ELSE 1 END AS y} RETURN i,count(1/x) ORDER BY i LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherTypeMismatch, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
}

test "dead constant container keys retain structural depth validation" {
    const a = std.testing.allocator;
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(a);
    try text.appendSlice(a, "UNWIND [1,2] AS i UNWIND [1,2] AS x CALL {WITH [] AS dead,x ");
    for (0..65) |_| try text.appendSlice(a, "WITH [dead] AS dead,x ");
    try text.appendSlice(a, "WITH dead,count(x) AS y RETURN y} RETURN i,sum(y)");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, text.items, .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    try std.testing.expectError(error.CypherValueDepthExceeded, runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{}));
}

test "parameter primitive certificates share child domains through forwarding and shadowing" {
    const a = std.testing.allocator;
    var groups: [30]v.Value = undefined;
    var xs: [1000]v.Value = undefined;
    for (&groups, 1..) |*cell, i| cell.* = .{ .integer = @intCast(i) };
    for (&xs, 0..) |*cell, i| cell.* = .{ .integer = @intCast(i) };
    const parameters: []const v.Entry = &.{ .{ .name = "groups", .value = .{ .list = &groups } }, .{ .name = "xs", .value = .{ .list = &xs } }, .{ .name = "unused", .value = .{ .string = "primitive" } } };
    for ([_][]const u8{
        "UNWIND $groups AS i UNWIND $xs AS x CALL {WITH i,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND $groups AS i WITH i,i+1 AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND $groups AS i WITH i,CASE WHEN i>10 THEN i ELSE 0 END AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND $groups AS i WITH i,head([i]) AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND $groups AS i WITH i,last(reverse([i])) AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND $groups AS i WITH i,[[i]][0][0] AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND $groups AS i WITH i,head(CASE WHEN i>10 THEN [i] ELSE [0] END) AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND range(1,30) AS i WITH i,toInteger(i) AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "UNWIND range(1,30) AS i WITH i,size(range(1,i)) AS j UNWIND $xs AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
        "WITH $groups AS gs UNWIND gs AS i UNWIND $xs AS x CALL {WITH i AS j,x CALL {WITH j,avg(x) AS y RETURN y} RETURN y} RETURN i,avg(y) ORDER BY i",
        "WITH $unused AS unused UNWIND $groups AS i UNWIND $xs AS x CALL {WITH * WITH i,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, parameters, .{ .memory_bytes = 4 << 20, .intermediate_states = 100000 }) catch |err| {
            std.debug.print("parameter certificate query failed: {s}\n", .{text});
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 30), result.rows.len);
        for (result.rows, 1..) |row, i| {
            try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
            try std.testing.expectEqual(@as(f64, 499.5), row[1].number);
        }
    }
}

test "independent singleton CALLs preserve compressed range and list observations" {
    for ([_]struct { text: []const u8, expected: []const u8 }{
        .{ .text = "UNWIND range(1,1000000) AS x CALL {RETURN 7 AS y} RETURN count(*),min(y),max(y)", .expected = "[[1000000,7,7]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {RETURN 7 AS y} RETURN count(x),min(x),max(x),sum(x)", .expected = "[[1000000,1,1000000,500000500000]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {WITH 7 AS z WHERE false RETURN count(*) AS y} RETURN count(*),min(y)", .expected = "[[1000000,0]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {CALL {RETURN DISTINCT 7 AS z LIMIT 1} RETURN z AS y} RETURN count(*),min(y)", .expected = "[[1000000,7]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {RETURN 7 AS y LIMIT 0} RETURN count(*)", .expected = "[[0]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {WITH 7 AS z WHERE false RETURN z AS y} RETURN count(*)", .expected = "[[0]]" },
        .{ .text = "UNWIND [] AS x CALL {RETURN 1/0 AS y} RETURN count(*)", .expected = "[[0]]" },
        .{ .text = "UNWIND [1,null,2,1] AS x CALL {RETURN 7 AS y} RETURN count(x),count(DISTINCT x),min(x),max(x),sum(x)", .expected = "[[3,2,1,2,4]]" },
        .{ .text = "UNWIND [1,2] AS i UNWIND range(1,1000000) AS x CALL {RETURN i AS y} RETURN i,count(y) ORDER BY i LIMIT 1", .expected = "[[1,1000000]]" },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000 });
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.rows, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings(case.expected, json);
    }
}

test "singleton axis proofs retain binding conflicts fallible reads and floating order" {
    for ([_][]const u8{
        "UNWIND [7,8] AS x CALL {RETURN 7 AS x} RETURN count(*)",
        "UNWIND [7,8] AS `7` CALL {RETURN 7} RETURN count(*)",
        "UNWIND [7,8] AS x CALL {CALL {RETURN 7 AS x} RETURN 7 AS y} RETURN count(*)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherVariableConflict, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    try check("UNWIND [1,2,3] AS x CALL {RETURN CASE WHEN x=2 THEN 9 ELSE 7 END AS y} RETURN count(*),min(y),max(y)", "[[3,7,9]]");
    try check("UNWIND [1e16,1.0,-1e16] AS i UNWIND [1,2] AS x CALL {RETURN i AS y} RETURN sum(y),avg(y)", "[[0,0]]");
    for ([_][]const u8{
        "UNWIND [1,0] AS x CALL {RETURN 1/x AS y} RETURN count(y)",
        "UNWIND range(1,1000000) AS x CALL {RETURN 1/0 AS y LIMIT 0} RETURN count(*)",
        "UNWIND range(1,1000000) AS x CALL {UNWIND [7,8] AS z RETURN 1/0 AS y LIMIT 0} RETURN count(*)",
        "UNWIND range(1,1000000) AS x CALL {RETURN 1/0 AS y} RETURN count(*) LIMIT 0",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000 }));
    }
}

test "parameter certificates and independent CALL compression release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            const groups = [_]v.Value{ .{ .integer = 1 }, .{ .integer = 2 } };
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "groups", .value = .{ .list = &groups } }}, .{});
            defer result.deinit();
            try std.testing.expect(result.rows.len != 0);
        }
    };
    for ([_][]const u8{
        "WITH $groups AS gs UNWIND gs AS i UNWIND [1,2] AS x CALL {WITH i,avg(x) AS y RETURN y} RETURN i,avg(y)",
        "UNWIND $groups AS i WITH i,i+1 AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y)",
        "UNWIND [1,2] AS i CALL {UNWIND [7,8] AS y RETURN y} RETURN i,count(*) ORDER BY i LIMIT 1",
        "UNWIND $groups AS i WITH i,last(reverse([i])) AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y)",
        "UNWIND range(1,10) AS x CALL {UNWIND [7,8,7] AS y RETURN y} RETURN y,count(x),count(DISTINCT x),sum(x)",
        "UNWIND [3,2,1] AS i UNWIND [0,1,0] AS x RETURN i,count(DISTINCT {v:x}) ORDER BY i LIMIT 1",
        "UNWIND range(1,100) AS x CALL {CALL {RETURN 7 AS z} RETURN z AS y} RETURN count(x),min(x),sum(x)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "independent CALL certificates match enumerated child invocations" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "[]", "[null]", "[7,8]", "[1,1.0,null,-2,3]", "range(1,20)", "range(1,20)+range(10,30)", "[9223372036854775807,-9223372036854775807]", "[-9223372036854775807,9223372036854775807]", "[1e16,1.0,-1e16]" }) |source| {
        for ([_][]const u8{
            "RETURN 7 AS y",
            "RETURN DISTINCT 7 AS y SKIP 0 LIMIT 1",
            "RETURN null AS y",
            "WITH 7 AS z WHERE false RETURN count(*) AS y",
            "WITH 7 AS z WHERE false RETURN z AS y",
            "RETURN 7 AS y LIMIT 0",
            "CALL {RETURN 7 AS z} RETURN z AS y",
            "CALL {RETURN 7 AS z LIMIT 0} RETURN count(*) AS y",
            "RETURN 1/0 AS y",
            "RETURN CASE WHEN x=8 THEN 1/0 ELSE 7 END AS y",
            "RETURN 7 AS x,7 AS y",
            "UNWIND [7,8,7] AS y RETURN y",
            "UNWIND [7,8,7] AS y RETURN DISTINCT y ORDER BY y DESC SKIP 1 LIMIT 1",
            "UNWIND [] AS y RETURN count(*) AS y",
            "UNWIND [7,8] AS z CALL {RETURN z AS y} RETURN y",
            "UNWIND [7,8] AS y RETURN y WHERE y=7",
            "UNWIND range(1,10) AS z RETURN 7 AS y",
            "UNWIND [7,8,7] AS z RETURN z%2 AS y",
            "UNWIND [7,8,7] AS z WITH z,count(*) AS n RETURN n AS y",
            "UNWIND [7,8] AS z RETURN 7 AS x,z AS y",
            "UNWIND [7,8] AS z RETURN CASE WHEN x=8 THEN 1/0 ELSE z END AS y",
        }) |body| {
            for ([_][]const u8{ "count(*),min(y),max(y)", "count(x),count(DISTINCT x),min(x),max(x),sum(x)", "collect(y),sum(y),avg(y)", "count(x),count(DISTINCT x),sum(x),min(x),max(x),count(*)", "y,count(x),count(DISTINCT x),sum(x) ORDER BY y", "sum(DISTINCT x),count(DISTINCT x)", "collect(x),sum(x),avg(x)" }) |projection| {
                const query = try std.fmt.allocPrint(a, "UNWIND {s} AS x CALL {{{s}}} RETURN {s}", .{ source, body, projection });
                defer a.free(query);
                const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x CALL {{MATCH (__guard:Document) WHERE __guard.id='a' {s}}} RETURN {s}", .{ source, body, projection });
                defer a.free(control);
                var diagnostic: c.Diagnostic = .{};
                var fast = try c.compile(a, query, .{}, &diagnostic);
                defer fast.deinit();
                var slow = try c.compile(a, control, .{}, &diagnostic);
                defer slow.deinit();
                var fixture: Fixture = .{};
                var actual = runtime.execute(a, fixture.backend(), graph, &fast, &.{}, .{}) catch |err| {
                    var control_result = runtime.execute(a, fixture.backend(), graph, &slow, &.{}, .{}) catch |control_error| {
                        try std.testing.expectEqual(err, control_error);
                        continue;
                    };
                    defer control_result.deinit();
                    std.debug.print("CALL oracle disagreement ({s}): {s}\n", .{ @errorName(err), query });
                    return error.TestExpectedError;
                };
                defer actual.deinit();
                var expected = try runtime.execute(a, fixture.backend(), graph, &slow, &.{}, .{});
                defer expected.deinit();
                try std.testing.expectEqualDeep(expected.rows, actual.rows);
            }
        }
    }
}

test "parameter element certificates belong to each prepared invocation and retain entity reads" {
    const Host = struct {
        fn project(_: *anyopaque, _: std.mem.Allocator, _: *const v.Node) anyerror!?*const v.Node {
            return error.CypherInvalidArgument;
        }
    };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "UNWIND $groups AS i UNWIND [1,2] AS x CALL {WITH i,count(x) AS y RETURN y} RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    var cells = [_]v.Value{ .{ .integer = 1 }, .{ .integer = 2 } };
    for (0..3) |iteration| {
        cells[1] = if (iteration == 1) .{ .node = &Fixture.nodes[0] } else .{ .integer = 2 };
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        callbacks.project_node = Host.project;
        const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
        const parameters: []const v.Entry = &.{.{ .name = "groups", .value = .{ .list = &cells } }};
        if (iteration == 1) {
            try std.testing.expectError(error.CypherInvalidArgument, runtime.execute(std.testing.allocator, backend, graph, &compiled, parameters, .{}));
        } else {
            var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, parameters, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, 4), result.rows[0][0].integer);
        }
    }
}

test "immutable list type certificates key the actual parameter domain" {
    const Host = struct {
        fn project(_: *anyopaque, _: std.mem.Allocator, _: *const v.Node) anyerror!?*const v.Node {
            return error.CypherInvalidArgument;
        }
    };
    const a = std.testing.allocator;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "WITH $same AS unused UNWIND $groups AS i UNWIND [1,2] AS x CALL {WITH i,count(x) AS y RETURN y} RETURN count(*)", .{}, &diagnostic);
    defer compiled.deinit();
    const integers = [_]v.Value{.{ .integer = 1 }};
    const nodes = [_]v.Value{.{ .node = &Fixture.nodes[0] }};
    // Internal callers use ordered binding arrays. Repeated names must not
    // conflate two immutable list domains when another name aliases the latter.
    const parameters: []const v.Entry = &.{ .{ .name = "same", .value = .{ .list = &integers } }, .{ .name = "same", .value = .{ .list = &nodes } }, .{ .name = "groups", .value = .{ .list = &nodes } } };
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.project_node = Host.project;
    try std.testing.expectError(error.CypherInvalidArgument, runtime.execute(a, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, parameters, .{}));
}

test "pure expanding CALL bags preserve compressed counts and bounded group windows" {
    for ([_]struct { text: []const u8, expected: []const u8, states: usize = 2000 }{
        .{ .text = "UNWIND range(1,1000000) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN count(*),min(y),max(y)", .expected = "[[2000000,7,8]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {UNWIND [7,8,7] AS y RETURN DISTINCT y ORDER BY y DESC SKIP 1 LIMIT 1} RETURN count(*),min(y)", .expected = "[[1000000,7]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {UNWIND [] AS y RETURN count(*) AS y} RETURN count(*),min(y)", .expected = "[[1000000,0]]" },
        .{ .text = "UNWIND range(1,1000000) AS x CALL {CALL {UNWIND [7,8] AS z RETURN z} RETURN z AS y} RETURN count(*),min(y),max(y)", .expected = "[[2000000,7,8]]" },
        .{ .text = "UNWIND range(1,10000) AS i UNWIND [0,1] AS x CALL {UNWIND [x,x+1] AS k RETURN k AS y} RETURN i,count(y) ORDER BY i LIMIT 5", .expected = "[[1,4],[2,4],[3,4],[4,4],[5,4]]", .states = 300000 },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = case.states, .memory_bytes = 2 << 20 });
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(std.testing.allocator, result.rows, .{});
        defer std.testing.allocator.free(json);
        try std.testing.expectEqualStrings(case.expected, json);
    }
}

test "CALL stage cardinalities count materialized memoized and compressed child bags" {
    for ([_]struct { body: []const u8, count: usize }{
        .{ .body = "RETURN 7 AS y", .count = 3 },
        .{ .body = "UNWIND [7,8] AS y RETURN y", .count = 6 },
        .{ .body = "UNWIND [7,8,7] AS y RETURN DISTINCT y", .count = 6 },
        .{ .body = "UNWIND [7,8] AS y RETURN y LIMIT 0", .count = 0 },
    }) |case| {
        const text = try std.fmt.allocPrint(std.testing.allocator, "UNWIND range(1,3) AS x CALL {{{s}}} RETURN count(*),min(x)", .{case.body});
        defer std.testing.allocator.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(case.count)), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(usize, 3), result.stages.len);
        try std.testing.expectEqualStrings("subquery", result.stages[1].operation);
        try std.testing.expectEqual(case.count, result.stages[1].output_rows);
        try std.testing.expectEqual(case.count, result.stages[2].input_rows);
    }
}

test "derived primitive proof DAGs share dependencies through long alias chains" {
    const a = std.testing.allocator;
    var text: std.Io.Writer.Allocating = .init(a);
    defer text.deinit();
    try text.writer.writeAll("UNWIND $groups AS i WITH i,i AS j ");
    for (0..80) |_| try text.writer.writeAll("WITH i,j=j AS j ");
    try text.writer.writeAll("UNWIND range(1,100) AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y) ORDER BY i");
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, text.written(), .{}, &diagnostic);
    defer compiled.deinit();
    const groups = [_]v.Value{ .{ .integer = 1 }, .{ .integer = 2 } };
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "groups", .value = .{ .list = &groups } }}, .{ .intermediate_states = 20000 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(f64, 50.5), result.rows[0][1].number);
    try std.testing.expectEqual(@as(f64, 50.5), result.rows[1][1].number);
}

test "observed axes compose independent CALL bags without multiplying set support" {
    for ([_]struct { body: []const u8, projection: []const u8 = "count(x),count(DISTINCT x),min(x),max(x),sum(x)", expected: []const u8 }{
        .{ .body = "UNWIND [7,8] AS y RETURN y", .expected = "[[2000000,1000000,1,1000000,1000001000000]]" },
        .{ .body = "UNWIND [7,7,8] AS y RETURN y", .expected = "[[3000000,1000000,1,1000000,1500001500000]]" },
        .{ .body = "UNWIND [7,7,8] AS y RETURN DISTINCT y ORDER BY y DESC LIMIT 1", .expected = "[[1000000,1000000,1,1000000,500000500000]]" },
        .{ .body = "UNWIND [7,8] AS y RETURN count(*) AS y", .expected = "[[1000000,1000000,1,1000000,500000500000]]" },
        .{ .body = "CALL {UNWIND [7,8] AS z RETURN z} RETURN z AS y", .expected = "[[2000000,1000000,1,1000000,1000001000000]]" },
        .{ .body = "UNWIND range(1,1000000) AS z RETURN 7 AS y", .expected = "[[1000000000000,1000000,1,1000000,500000500000000000]]" },
        .{ .body = "UNWIND [7,7,8] AS y RETURN y", .projection = "y,count(x),count(DISTINCT x),sum(x) ORDER BY y", .expected = "[[7,2000000,1000000,1000001000000],[8,1000000,1000000,500000500000]]" },
        .{ .body = "UNWIND [] AS y RETURN y", .expected = "[[0,0,null,null,0]]" },
        .{ .body = "UNWIND [] AS y RETURN count(*) AS y", .expected = "[[1000000,1000000,1,1000000,500000500000]]" },
    }) |case| {
        const a = std.testing.allocator;
        const text = try std.fmt.allocPrint(a, "UNWIND range(1,1000000) AS x CALL {{{s}}} RETURN {s}", .{ case.body, case.projection });
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 2 << 20 });
        defer result.deinit();
        const json = try std.json.Stringify.valueAlloc(a, result.rows, .{});
        defer a.free(json);
        try std.testing.expectEqualStrings(case.expected, json);
    }
}

test "bounded DISTINCT group windows own only selected memberships across eviction" {
    for ([_][]const u8{
        "UNWIND range(1,10000) AS i UNWIND [0,1,0] AS x RETURN i,count(DISTINCT x) ORDER BY i LIMIT 5",
        "UNWIND range(10000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,count(DISTINCT [x]) ORDER BY i LIMIT 5",
        "UNWIND range(10000,1,-1) AS i CALL {UNWIND [0,1,0] AS x RETURN x} RETURN i,count(DISTINCT {v:x}) ORDER BY i LIMIT 5",
    }) |text| {
        const a = std.testing.allocator;
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000000, .memory_bytes = 2 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5), result.rows.len);
        for (result.rows, 1..) |row, i| {
            try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
            try std.testing.expectEqual(@as(i64, 2), row[1].integer);
        }
        try std.testing.expect(result.stats.memory_bytes < 200000);
    }
    try check("UNWIND [1,2] AS i UNWIND range(1,1000000000000000) AS x RETURN i,count(DISTINCT 7) ORDER BY i LIMIT 1", "[[1,1]]");
}

test "observed CALL domain sums retain repeated-element integer prefix overflow" {
    try check("MATCH REPEATABLE ELEMENTS (a:Document {id:'a'})-[:CITES*1..3]->(b:Document) UNWIND range(1,3) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN sum(x),min(b.rank),max(b.rank),count(*)", "[[120,1,3,60]]");
    for ([_][]const u8{
        "UNWIND [9223372036854775807,-9223372036854775807] AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN sum(x)",
        "UNWIND [-9223372036854775807,9223372036854775807] AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN sum(x)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000 }));
    }
    try check("UNWIND [[1e16,1.0,-1e16],[1,2,-1]] AS xs UNWIND xs AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN sum(x)", "[[4]]");
    try check("UNWIND [null,null] AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN count(x),count(DISTINCT x),sum(x),count(*)", "[[0,0,0,4]]");
}

test "bounded DISTINCT windows reuse immutable wide argument metadata" {
    const a = std.testing.allocator;
    var cells: [1000]v.Value = undefined;
    for (&cells, 0..) |*cell, i| cell.* = .{ .integer = @intCast(i) };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "UNWIND range(10000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,count(DISTINCT $payload) ORDER BY i LIMIT 5", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "payload", .value = .{ .list = &cells } }}, .{ .intermediate_states = 600000, .memory_bytes = 2 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 5), result.rows.len);
    for (result.rows) |row| try std.testing.expectEqual(@as(i64, 1), row[1].integer);
}

test "branch and element result certificates preserve entity projection on dead reads" {
    const Host = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn project(ptr: *anyopaque, _: std.mem.Allocator, node: *const v.Node) anyerror!?*const v.Node {
            const reads: *usize = @ptrCast(@alignCast(ptr));
            reads.* += 1;
            if (reads.* > 1) return error.CypherInvalidArgument;
            return node;
        }
    };
    for ([_][]const u8{ "head([i])", "last([i])", "[i][0]", "CASE WHEN true THEN i ELSE null END", "({v:i}).v", "head([z IN [i] | z])", "head([{v:i}]).v" }) |expression| {
        const a = std.testing.allocator;
        const text = try std.fmt.allocPrint(a, "UNWIND $groups AS i WITH {s} AS j UNWIND [1,2] AS x CALL {{WITH j,count(x) AS y RETURN y}} RETURN count(*)", .{expression});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        const cells = [_]v.Value{.{ .node = &Fixture.nodes[0] }};
        var fixture: Fixture = .{};
        var callbacks = fixture.backend().vtable.*;
        callbacks.project_node = Host.project;
        callbacks.checkpoint = Host.checkpoint;
        var reads: usize = 0;
        const backend: b.Backend = .{ .ptr = &reads, .vtable = &callbacks };
        try std.testing.expectError(error.CypherInvalidArgument, runtime.execute(a, backend, graph, &compiled, &.{.{ .name = "groups", .value = .{ .list = &cells } }}, .{}));
        try std.testing.expectEqual(@as(usize, 2), reads);
    }
}

test "observed immutable parameter lists separate bag counts and shared DISTINCT support" {
    const a = std.testing.allocator;
    var cells: [1002]v.Value = undefined;
    for (cells[0..1001], 0..) |*cell, i| cell.* = .{ .integer = @intCast(i) };
    cells[1001] = .null;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "UNWIND $xs AS x CALL {UNWIND [7,7,8] AS y RETURN y} RETURN y,count(x),count(DISTINCT x),sum(x),sum(DISTINCT x) ORDER BY y", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &cells } }}, .{ .intermediate_states = 40000, .memory_bytes = 2 << 20 });
    defer result.deinit();
    const json = try std.json.Stringify.valueAlloc(a, result.rows, .{});
    defer a.free(json);
    try std.testing.expectEqualStrings("[[7,2002,1001,1001000,500500],[8,1001,1001,500500,500500]]", json);
}

fn compareReductionPlans(text: []const u8, control: []const u8) !void {
    const a = std.testing.allocator;
    var diagnostic: c.Diagnostic = .{};
    var fast = try c.compile(a, text, .{}, &diagnostic);
    defer fast.deinit();
    var slow = try c.compile(a, control, .{}, &diagnostic);
    defer slow.deinit();
    var fixture: Fixture = .{};
    var actual = runtime.execute(a, fixture.backend(), graph, &fast, &.{}, .{}) catch |err| {
        var expected = runtime.execute(a, fixture.backend(), graph, &slow, &.{}, .{}) catch |other| {
            if (err != other) std.debug.print("reduction error mismatch: {s}\n", .{text});
            try std.testing.expectEqual(err, other);
            return;
        };
        defer expected.deinit();
        std.debug.print("unexpected reduction failure ({s}): {s}\n", .{ @errorName(err), text });
        return error.TestUnexpectedResult;
    };
    defer actual.deinit();
    var expected = try runtime.execute(a, fixture.backend(), graph, &slow, &.{}, .{});
    defer expected.deinit();
    try std.testing.expectEqualDeep(expected.rows, actual.rows);
}

test "affine observed CALL ranges retain constant work and exact set cardinality" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "UNWIND range(1,1000000) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN count(x+1),count(DISTINCT x+1),min(x+1),max(x+1),sum(x+1)",
        "WITH reverse(range(1,500000)+range(500001,1000000)) AS xs UNWIND xs AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN count(x+1),count(DISTINCT x+1),min(x+1),max(x+1),sum(x+1)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expect(result.stats.intermediate_states < 1000);
        const json = try std.json.Stringify.valueAlloc(a, result.rows, .{});
        defer a.free(json);
        try std.testing.expectEqualStrings("[[2000000,1000000,2,1000001,1000003000000]]", json);
    }
    try check("UNWIND range(1,1000000) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN count(DISTINCT x*0+7),sum(x*0+7),min(-2*x+1),max(-2*x+1)", "[[1,14000000,-1999999,-1]]");
}

test "affine observed CALL certificates agree with sequential arithmetic and error order" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "range(-3,3)", "reverse(range(-3,3))", "range(-3,3)+range(2,4)", "reverse(range(-3,3)+range(2,4))", "range(9223372036854775805,9223372036854775807)", "range(-9223372036854775808,-9223372036854775806)", "range(1,0)", "range(1,1)" }) |source| {
        for ([_][]const u8{ "x", "x+1", "-x", "2*x-1", "(x+1)-x+2", "0*x+1", "(x+9223372036854775807)-9223372036854775807", "x*x" }) |argument| {
            for ([_][]const u8{ "UNWIND [7,8,7] AS y RETURN y", "RETURN 1/0 AS y", "RETURN 7 AS y WHERE false", "UNWIND [] AS y RETURN count(*) AS y", "UNWIND [7,8] AS y RETURN DISTINCT y LIMIT 1" }) |body| {
                const projection = try std.fmt.allocPrint(a, "count({s}),count(DISTINCT {s}),min({s}),max({s}),sum({s})", .{ argument, argument, argument, argument, argument });
                defer a.free(projection);
                const text = try std.fmt.allocPrint(a, "UNWIND {s} AS x CALL {{{s}}} RETURN {s}", .{ source, body, projection });
                defer a.free(text);
                const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x CALL {{MATCH (__guard:Document) WHERE __guard.id='a' {s}}} RETURN {s}", .{ source, body, projection });
                defer a.free(control);
                try compareReductionPlans(text, control);
            }
        }
    }
}

test "bounded numeric and extrema windows retain selected state through eviction" {
    const a = std.testing.allocator;
    const group_count: usize = if (@import("builtin").mode == .debug) 500 else 10000;
    for ([_][]const u8{ "min(x)", "max(x)", "min(x),max(x)", "sum(x)", "avg(x)", "sum(DISTINCT x)", "avg(DISTINCT x)" }) |aggregate| {
        const text = try std.fmt.allocPrint(a, "UNWIND range({d},1,-1) AS i UNWIND [0,1,0] AS x RETURN i,{s} ORDER BY i LIMIT 5", .{ group_count, aggregate });
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 1000000, .memory_bytes = 2 << 20 }) catch |err| {
            std.debug.print("bounded reducer resource case: {s}\n", .{text});
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5), result.rows.len);
        try std.testing.expect(result.stats.memory_bytes < 200000);
        for (result.rows, 1..) |row, i| try std.testing.expectEqual(@as(i64, @intCast(i)), row[0].integer);
    }
}

test "bounded numeric windows reuse certified complete domains within the ordinary work budget" {
    const a = std.testing.allocator;
    const group_count: usize = if (@import("builtin").mode == .debug) 500 else 40000;
    for ([_][]const u8{ "min(x)", "sum(x)", "avg(x)" }) |aggregate| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(1,{d}) AS i UNWIND [0,1,0] AS x RETURN i,{s} ORDER BY i LIMIT 5", .{ group_count, aggregate });
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 1000000, .memory_bytes = 2 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5), result.rows.len);
        try std.testing.expect(result.stats.memory_bytes < 200000);
    }
}

test "bounded reducer packets retain compressed range and immutable list proofs" {
    const a = std.testing.allocator;
    var xs: [1000]v.Value = undefined;
    for (&xs, 0..) |*value, i| value.* = .{ .integer = @intCast(i + 1) };
    for ([_][]const u8{ "range(1,1000000)", "reverse(range(1,500000)+range(500001,1000000))", "$xs" }) |source| {
        for ([_][]const u8{ "", "CALL {UNWIND [7,8] AS y RETURN y} " }) |child| {
            const text = try std.fmt.allocPrint(a, "UNWIND [1,2] AS i UNWIND {s} AS x {s}RETURN i,min(x),max(x),sum(x) ORDER BY i LIMIT 1", .{ source, child });
            defer a.free(text);
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &xs } }}, .{ .intermediate_states = 100000, .memory_bytes = 2 << 20 });
            defer result.deinit();
            const n: i64 = if (std.mem.eql(u8, source, "$xs")) 1000 else 1000000;
            try std.testing.expectEqual(@as(usize, 1), result.rows.len);
            try std.testing.expectEqual(@as(i64, 1), result.rows[0][1].integer);
            try std.testing.expectEqual(n, result.rows[0][2].integer);
            try std.testing.expectEqual(@divExact(n * (n + 1), 2) * @as(i64, if (child.len == 0) 1 else 2), result.rows[0][3].integer);
        }
    }
}

test "unsafe compressed packet bounds retry the original symbolic reduction" {
    const a = std.testing.allocator;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "UNWIND [1,2] AS i UNWIND range(1,1000000) AS x RETURN i,sum(x+9000000000000) ORDER BY i LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 2 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 9000000500000500000), result.rows[0][1].integer);
}

test "bounded reducer certificates preserve rejected argument failures and deferred precedence" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "[1,2,3,4]", "[4,3,2,1]", "[3,1,2,3,4,2,1]" }) |groups| {
        for ([_][]const u8{ "[null,0,1,0]", "[1,2.5,-3]", "[1,'bad',2]", "[[1],[2],[]]", "[{v:1},{v:2}]", "[9223372036854775807,1]", "[1e308,1e308,-1e308]" }) |source| {
            for ([_][]const u8{ "min(x)", "max(x)", "sum(x)", "avg(x)", "sum(DISTINCT x)", "avg(DISTINCT x)" }) |aggregate| {
                const text = try std.fmt.allocPrint(a, "UNWIND {s} AS i UNWIND {s} AS x RETURN i,{s} AS n ORDER BY i SKIP 1 LIMIT 2", .{ groups, source, aggregate });
                defer a.free(text);
                const control = try std.fmt.allocPrint(a, "UNWIND {s} AS i UNWIND {s} AS x WITH i,{s} AS n RETURN i,n ORDER BY i SKIP 1 LIMIT 2", .{ groups, source, aggregate });
                defer a.free(control);
                try compareReductionPlans(text, control);
            }
        }
    }
    try compareReductionPlans("UNWIND [1,2] AS i UNWIND [1,'bad'] AS x WITH i,x,1/(2-i) AS z RETURN i,sum(x) AS n ORDER BY i LIMIT 1", "UNWIND [1,2] AS i UNWIND [1,'bad'] AS x WITH i,x,1/(2-i) AS z WITH i,sum(x) AS n RETURN i,n ORDER BY i LIMIT 1");
    try compareReductionPlans("UNWIND [1,2] AS i UNWIND [9223372036854775807,1] AS x RETURN i,sum(x) AS n ORDER BY i LIMIT 0", "UNWIND [1,2] AS i UNWIND [9223372036854775807,1] AS x WITH i,sum(x) AS n RETURN i,n ORDER BY i LIMIT 0");
}

test "structured scalar certificates remove dead keys without rescanning each captured domain" {
    const a = std.testing.allocator;
    var xs: [1000]v.Value = undefined;
    for (&xs, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
    for ([_][]const u8{ "({v:i}).v", "({v:i})['v']", "({v:i}).missing", "head([z IN [i] | z])", "head([z IN [i] WHERE z>0 | {v:z}]).v", "({v:{k:[i]}}).v.k[0]", "head([{v:i}]).v" }) |expression| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(1,30) AS i WITH i,{s} AS j UNWIND $xs AS x CALL {{WITH j,avg(x) AS y RETURN y}} RETURN i,avg(y) ORDER BY i", .{expression});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = &xs } }}, .{ .intermediate_states = 100000, .memory_bytes = 2 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 30), result.rows.len);
        for (result.rows) |row| try std.testing.expectEqual(@as(f64, 499.5), row[1].number);
    }
}

test "observed expression and bounded reducer certificates release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "WITH range(1,4) AS xs WITH reverse(xs+xs) AS xs UNWIND xs AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN count(DISTINCT x+1),sum(2*x-1),min(-x)",
        "UNWIND [4,3,2,1] AS i UNWIND [0,1,0] AS x RETURN i,sum(x),avg(DISTINCT x),min(x) ORDER BY i LIMIT 1",
        "UNWIND [3,1,2] AS i UNWIND [0,1,0] AS x RETURN i,sum(x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i UNWIND range(1,4) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN i,min(x),sum(x) ORDER BY i LIMIT 1",
        "UNWIND [1,2,3] AS i WITH i,head([{v:i}]).v AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y)",
    }) |text| std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text}) catch |err| {
        std.debug.print("certificate allocation campaign: {s}\n", .{text});
        return err;
    };
}

test "structured certificates preserve map producer and comprehension predicate failures" {
    for ([_][]const u8{ "({unused:1/0,v:i}).v", "head([z IN [i] WHERE 1/0=0 | z])" }) |expression| {
        const a = std.testing.allocator;
        const text = try std.fmt.allocPrint(a, "UNWIND [1,2] AS i WITH i,{s} AS j UNWIND [1,2] AS x CALL {{WITH j,avg(x) AS y RETURN y}} RETURN i,avg(y)", .{expression});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{}));
    }
    const Host = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn project(_: *anyopaque, _: std.mem.Allocator, _: *const v.Node) anyerror!?*const v.Node {
            return error.CypherInvalidArgument;
        }
    };
    const a = std.testing.allocator;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "UNWIND $groups AS i WITH ({unused:i,v:1}).v AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN avg(y)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.checkpoint = Host.checkpoint;
    callbacks.project_node = Host.project;
    const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
    try std.testing.expectError(error.CypherInvalidArgument, runtime.execute(a, backend, graph, &compiled, &.{.{ .name = "groups", .value = .{ .list = &.{.{ .node = &Fixture.nodes[0] }} } }}, .{}));
}

test "bounded numeric windows retain failures in rejected path statistic groups" {
    const Failure = struct {
        fn check(text: []const u8, control: []const u8) !void {
            for ([_][]const u8{ text, control }) |query| {
                var diagnostic: c.Diagnostic = .{};
                var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
                defer compiled.deinit();
                var fixture: Fixture = .{};
                try std.testing.expectError(error.CypherNumericOverflow, runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{}));
            }
        }
    };
    try Failure.check("UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..2]->(a) RETURN g,sum(length(p)*3000000000000000000) AS n ORDER BY g DESC LIMIT 1", "UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..2]->(a) WITH g,sum(length(p)*3000000000000000000) AS n RETURN g,n ORDER BY g DESC LIMIT 1");
    try Failure.check("UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..2]->(a) RETURN g,sum(length(p)*length(p)*1500000000000000000) AS n ORDER BY g DESC LIMIT 1", "UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..2]->(a) WITH g,sum(length(p)*length(p)*1500000000000000000) AS n RETURN g,n ORDER BY g DESC LIMIT 1");
    try Failure.check("UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..100]->(b) RETURN g,sum(length(p)*length(p)) AS n ORDER BY g DESC LIMIT 1", "UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..100]->(b) WITH g,sum(length(p)*length(p)) AS n RETURN g,n ORDER BY g DESC LIMIT 1");
    try Failure.check("UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..3]->(b) RETURN g,sum(length(p)*1000000000000000000) AS n ORDER BY g DESC LIMIT 1", "UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..3]->(b) WITH g,sum(length(p)*1000000000000000000) AS n RETURN g,n ORDER BY g DESC LIMIT 1");
    try Failure.check("UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..2]->(b) RETURN g,sum(length(p)*length(p)*1000000000000000000) AS n ORDER BY g DESC LIMIT 1", "UNWIND ['c','a'] AS g MATCH REPEATABLE ELEMENTS p=(a:Document {id:g})-[:CITES*0..2]->(b) WITH g,sum(length(p)*length(p)*1000000000000000000) AS n RETURN g,n ORDER BY g DESC LIMIT 1");
}

test "symbolic DISTINCT ranges keep million value reducers and grouped CALL packets bounded" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "UNWIND range(1,1000000) AS x RETURN sum(DISTINCT x)",
        "UNWIND range(1,1000000) AS x RETURN sum(DISTINCT x+1),count(DISTINCT x+1)",
        "UNWIND [1,2] AS g UNWIND range(1,1000000) AS x RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g",
        "UNWIND [1,2] AS g UNWIND range(1,1000000) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g",
        "UNWIND [1,2,1,2] AS g UNWIND range(1,1000000) AS x RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g",
        "UNWIND [1,2,1,2] AS g UNWIND range(1,1000000) AS x RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g LIMIT 1",
        "WITH range(1,1000000) AS xs WITH reverse(xs+xs) AS xs UNWIND xs AS x RETURN count(DISTINCT x),sum(DISTINCT x)",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expect(result.rows.len != 0);
        try std.testing.expect(result.stats.memory_bytes < 200000);
        const offset: i64 = if (std.mem.indexOf(u8, text, "x+1") != null) 1000000 else 0;
        for (result.rows) |row| {
            const total = row[if (offset != 0) 0 else row.len - 1].integer;
            try std.testing.expectEqual(@as(i64, 500000500000) + offset, total);
        }
    }
}

test "certified ordered numeric windows keep only selected reducer states" {
    const a = std.testing.allocator;
    const groups: usize = if (@import("builtin").mode == .debug) 500 else 10000;
    for ([_][]const u8{ "min(x)", "max(x)", "sum(x)", "avg(x)", "sum(DISTINCT x)", "avg(DISTINCT x)" }) |aggregate| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(1,{d}) AS i UNWIND [0,1,0] AS x WITH i,x ORDER BY x DESC RETURN i,{s} ORDER BY i LIMIT 5", .{ groups, aggregate });
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .memory_bytes = 2 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5), result.rows.len);
        try std.testing.expect(result.stats.memory_bytes < 200000);
    }
}

test "symbolic DISTINCT supports preserve ordered overlap and scalar transitions" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "range(-4,4)",                                      "reverse(range(-4,4))",          "range(-4,4)+range(2,7)", "reverse(range(-4,4)+range(2,7))",
        "range(-8,8,2)+range(-6,9,3)",                      "range(8,-8,-2)+range(9,-6,-3)", "range(1,0)",             "range(9223372036854775805,9223372036854775807)",
        "range(-9223372036854775808,-9223372036854775806)",
    }) |source| for ([_][]const u8{ "x", "x+1", "-x", "2*x-1", "0*x+7" }) |argument| {
        for ([_][]const u8{ "", "CALL {UNWIND [7,8] AS y RETURN y} " }) |child| {
            const text = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND {s} AS x {s}RETURN g,count(DISTINCT {s}),sum(DISTINCT {s}) ORDER BY g", .{ source, child, argument, argument });
            defer a.free(text);
            // A fallible dependent predicate prevents arithmetic compression.
            const control = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND {s} AS x WITH g,x WHERE x IS NOT NULL {s}RETURN g,count(DISTINCT {s}),sum(DISTINCT {s}) ORDER BY g", .{ source, child, argument, argument });
            defer a.free(control);
            try compareReductionPlans(text, control);
        }
    };
    for ([_][]const u8{ "range(1,5)", "[2,3,6]", "[2.0,3.5,7.0]", "[null,1,2]", "['bad',2]" }) |first| for ([_][]const u8{ "range(3,8)", "range(8,3,-1)", "[1.0,2.5,3]", "[null,1,2]" }) |second| {
        const text = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND [0,1] AS k UNWIND CASE WHEN k=0 THEN {s} ELSE {s} END AS x RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g", .{ first, second });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND [0,1] AS k UNWIND CASE WHEN k=0 THEN {s} ELSE {s} END AS x WITH g,x WHERE x IS NOT NULL RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g", .{ first, second });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
}

test "DISTINCT COUNT preserves ordinary fractional members across repeated groups" {
    const a = std.testing.allocator;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "UNWIND [1,2,1,2] AS g UNWIND [0,1] AS k UNWIND CASE WHEN k=0 THEN [null,1.5,1.0] ELSE range(1,100) END AS x RETURN g,count(DISTINCT x) ORDER BY g LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 100000, .memory_bytes = 1 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(i64, 101), result.rows[0][1].integer);
}

test "symbolic DISTINCT and ordered window snapshots release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "UNWIND [1,2,1] AS g UNWIND range(-3,3)+range(1,5) AS x RETURN g,count(DISTINCT x),sum(DISTINCT x) ORDER BY g LIMIT 1",
        "UNWIND [1,2,1] AS g UNWIND [0,1] AS k UNWIND CASE WHEN k=0 THEN [1.5,1.0] ELSE range(1,4) END AS x RETURN g,count(DISTINCT x) ORDER BY g LIMIT 1",
        "UNWIND [1,2] AS i UNWIND [0,1,0] AS x WITH i,x ORDER BY x DESC RETURN i,sum(x),avg(x) ORDER BY i LIMIT 1",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "arithmetic averages certify exact recurrences without enumerating ranges" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "UNWIND range(1,1000000) AS x RETURN avg(x)",
        "UNWIND range(1,1000000) AS x RETURN count(x),sum(x),avg(x)",
        "UNWIND reverse(range(1,1000000)) AS x RETURN avg(x)",
        "UNWIND range(1,500000)+range(500001,1000000) AS x RETURN avg(x)",
        "UNWIND range(1,1000000) AS x RETURN avg(x+1)-1",
        "UNWIND range(1,1000000) AS x RETURN avg(2*x)/2",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(f64, 500000.5), result.rows[0][result.rows[0].len - 1].number);
    }
    for ([_][]const u8{ "range(-17,19,3)", "reverse(range(-17,19,3))", "range(1,10)+range(3,8)", "range(9223372036854775780,9223372036854775807)", "range(1,0)", "range(-4,4)+range(5,8)" }) |source| {
        for ([_][]const u8{ "x", "-x", "2*x-1", "0*x+7", "x/3.0" }) |argument| {
            const text = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND {s} AS x RETURN g,avg({s}),count(x) ORDER BY g", .{ source, argument });
            defer a.free(text);
            const control = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND {s} AS x WITH g,x WHERE x IS NOT NULL RETURN g,avg({s}),count(x) ORDER BY g", .{ source, argument });
            defer a.free(control);
            try compareReductionPlans(text, control);
        }
    }
}

test "safe unused container captures do not correlate scalar subqueries" {
    const a = std.testing.allocator;
    const values = try a.alloc(v.Value, 1000);
    defer a.free(values);
    for (values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
    for ([_][]const u8{ "[i]+[]", "{v:i}", "[[i]]", "reverse([i])", "[z IN [i] | [z]]" }) |expression| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(1,100) AS i WITH i,{s} AS j UNWIND $xs AS x CALL {{WITH j,avg(x) AS y RETURN y}} RETURN i,avg(y) ORDER BY i", .{expression});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "xs", .value = .{ .list = values } }}, .{ .intermediate_states = 100000, .memory_bytes = 2 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 100), result.rows.len);
        for (result.rows) |row| try std.testing.expectEqual(@as(f64, 499.5), row[1].number);
    }
    try compareReductionPlans("UNWIND [1,2] AS i WITH i,[i] AS j CALL {UNWIND [] AS x WITH j,avg(x) AS y RETURN y} RETURN i,count(*)", "UNWIND [1,2] AS i WITH i,[i] AS j CALL {UNWIND [] AS x WITH i AS keep,j,avg(x) AS y RETURN y} RETURN i,count(*)");
}

test "monotone numeric group windows select certified rank intervals" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "count(x)", "min(x)", "sum(x)", "avg(x)", "sum(DISTINCT x)", "avg(DISTINCT x)" }) |aggregate| {
        for ([_][]const u8{ "ASC", "DESC" }) |direction| {
            const text = try std.fmt.allocPrint(a, "UNWIND range(40000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,{s} ORDER BY i {s} SKIP 2 LIMIT 5", .{ aggregate, direction });
            defer a.free(text);
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 5), result.rows.len);
            try std.testing.expectEqual(@as(i64, if (std.mem.eql(u8, direction, "ASC")) 3 else 39998), result.rows[0][0].integer);
            try std.testing.expect(result.stats.memory_bytes < 200000);
        }
    }
    for ([_][]const u8{ "ASC", "DESC" }) |direction| for ([_][]const u8{ "0", "1", "4" }) |limit| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(20,-20,-3) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) AS n ORDER BY i {s} SKIP 2 LIMIT {s}", .{ direction, limit });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND range(20,-20,-3) AS i UNWIND [0,1,0] AS x WITH i,avg(x) AS n RETURN i,n ORDER BY i {s} SKIP 2 LIMIT {s}", .{ direction, limit });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
    try compareReductionPlans("UNWIND range(20,1,-1) AS i UNWIND [0,1] AS x RETURN i,sum(1/x) ORDER BY i LIMIT 1", "UNWIND range(20,1,-1) AS i UNWIND [0,1] AS x WITH i,sum(1/x) AS n RETURN i,n ORDER BY i LIMIT 1");
}

test "arithmetic average and monotone window proofs release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var callbacks = backing.vtable.*;
            callbacks.resize = std.mem.Allocator.noResize;
            callbacks.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &callbacks };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "UNWIND range(1,1000) AS x RETURN avg(x),count(x),sum(x)",
        "UNWIND range(10,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x),sum(x) ORDER BY i LIMIT 1",
        "UNWIND [1,2] AS i WITH i,[i]+[] AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN i,avg(y)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "monotone window packet bounds retry exact large averages without eviction" {
    const a = std.testing.allocator;
    for ([_][]const u8{ "avg(0*x)", "count(*)" }) |aggregate| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(4,1,-1) AS i UNWIND range(1,1000000000000000) AS x RETURN i,{s} ORDER BY i LIMIT 1", .{aggregate});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 10000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
        if (std.mem.startsWith(u8, aggregate, "avg")) try std.testing.expectEqual(@as(f64, 0), result.rows[0][1].number) else try std.testing.expectEqual(@as(i64, 1000000000000000), result.rows[0][1].integer);
    }
}

test "unreachable container reads never resolve parameter structural proofs" {
    const a = std.testing.allocator;
    const payload = try a.alloc(v.Value, 50000);
    defer a.free(payload);
    @memset(payload, .{ .integer = 0 });
    for ([_][]const u8{ "UNWIND [] AS i", "UNWIND range(1,0) AS i", "WITH 1 AS i WHERE false" }) |prefix| {
        const text = try std.fmt.allocPrint(a, "{s} WITH i,$payload AS j UNWIND [1,2] AS x CALL {{WITH j,avg(x) AS y RETURN y}} RETURN count(*)", .{prefix});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "payload", .value = .{ .list = payload } }}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 0), result.rows[0][0].integer);
        try std.testing.expect(result.stats.intermediate_states < 100);
    }
    {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, "UNWIND [1] AS i WITH i,$payload AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN count(*)", .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "payload", .value = .{ .list = payload[0..1000] } }}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2), result.rows[0][0].integer);
    }
    var nested: [66]v.Value = undefined;
    nested[0] = .{ .integer = 0 };
    for (1..nested.len) |i| nested[i] = .{ .list = nested[i - 1 .. i] };
    for ([_][]const u8{ "[]", "[1]" }) |input| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS i WITH i,$payload AS j CALL {{WITH j,avg(i) AS y RETURN y}} RETURN count(*)", .{input});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        const result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "payload", .value = nested[nested.len - 1] }}, .{});
        if (std.mem.eql(u8, input, "[]")) {
            var actual = try result;
            defer actual.deinit();
            try std.testing.expectEqual(@as(i64, 0), actual.rows[0][0].integer);
        } else try std.testing.expectError(error.CypherValueDepthExceeded, result);
    }
}

test "affine group keys select checked injective monotone windows" {
    const a = std.testing.allocator;
    for ([_]struct { expression: []const u8, first: i64 }{
        .{ .expression = "i+1", .first = 4 },
        .{ .expression = "-i", .first = -39998 },
        .{ .expression = "3-2*i", .first = -79993 },
        .{ .expression = "(i+7)-7", .first = 3 },
    }) |item| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(40000,1,-1) AS i UNWIND [0,1,0] AS x RETURN {s} AS g,avg(x),sum(x) ORDER BY g SKIP 2 LIMIT 5", .{item.expression});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5), result.rows.len);
        try std.testing.expectEqual(item.first, result.rows[0][0].integer);
    }
    for ([_][]const u8{ "i+1", "-i", "3-2*i", "0*i", "(i+9223372036854775807)-9223372036854775807", "i+'bad'" }) |expression| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x RETURN {s} AS g,avg(x) AS n ORDER BY g LIMIT 2", .{expression});
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x WITH {s} AS g,avg(x) AS n RETURN g,n ORDER BY g LIMIT 2", .{expression});
        defer a.free(control);
        try compareReductionPlans(text, control);
    }
}

test "distinct arithmetic averages retain compressed membership and packet order" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "UNWIND range(1,1000000) AS x RETURN avg(DISTINCT x)",
        "UNWIND reverse(range(1,1000000)) AS x RETURN avg(DISTINCT x)",
        "UNWIND range(1,500000)+range(250000,1000000) AS x RETURN avg(DISTINCT x)",
        "UNWIND reverse(range(1,500000)+range(250000,1000000)) AS x RETURN avg(DISTINCT x)",
        "WITH range(1,1000000) AS xs WITH xs+xs AS xs UNWIND xs AS x RETURN avg(DISTINCT x)",
        "UNWIND [1,2,1] AS g UNWIND range(1,1000000) AS x RETURN g,count(DISTINCT x),sum(DISTINCT x),avg(DISTINCT x) ORDER BY g",
        "UNWIND [1,2,1] AS g UNWIND range(1,1000000) AS x CALL {UNWIND [7,8] AS y RETURN y} RETURN g,avg(DISTINCT x) ORDER BY g LIMIT 1",
    }) |text| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 3000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expect(result.rows.len != 0);
        for (result.rows) |row| try std.testing.expectEqual(@as(f64, 500000.5), row[row.len - 1].number);
        try std.testing.expect(result.stats.memory_bytes < 200000);
    }
    for ([_][]const u8{ "range(-4,4)", "reverse(range(-4,4))", "range(-4,4)+range(1,8)", "reverse(range(-4,4)+range(1,8))", "range(-8,8,2)+range(-6,9,3)", "range(9223372036854775780,9223372036854775807)" }) |source| {
        for ([_][]const u8{ "x", "-x", "2*x-1", "0*x+7" }) |argument| {
            const text = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND {s} AS x CALL {{UNWIND [7,8] AS y RETURN y}} RETURN g,avg(DISTINCT {s}),count(DISTINCT {s}) ORDER BY g", .{ source, argument, argument });
            defer a.free(text);
            const control = try std.fmt.allocPrint(a, "UNWIND [1,2,1] AS g UNWIND {s} AS x WITH g,x WHERE x IS NOT NULL CALL {{UNWIND [7,8] AS y RETURN y}} RETURN g,avg(DISTINCT {s}),count(DISTINCT {s}) ORDER BY g", .{ source, argument, argument });
            defer a.free(control);
            compareReductionPlans(text, control) catch |err| {
                std.debug.print("DISTINCT AVG mismatch: {s}\n", .{text});
                return err;
            };
        }
    }
}

test "singleton children preserve exact arithmetic average axes" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "RETURN 7 AS y",
        "CALL {RETURN 7 AS z} RETURN z AS y",
        "WITH 7 AS z WHERE false RETURN count(*) AS y",
    }) |body| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(1,1000000) AS x CALL {{{s}}} RETURN count(x),avg(x)", .{body});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1000000), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(f64, 500000.5), result.rows[0][1].number);
    }
    for ([_][]const u8{ "RETURN 7 AS y LIMIT 0", "WITH 7 AS z WHERE false RETURN z AS y", "RETURN 1/0 AS y", "RETURN 7 AS x", "RETURN 7 AS y" }) |body| {
        for ([_][]const u8{ "range(1,10)", "range(1,0)" }) |source| {
            const text = try std.fmt.allocPrint(a, "UNWIND {s} AS x CALL {{{s}}} RETURN count(x),avg(x)", .{ source, body });
            defer a.free(text);
            const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x WITH x WHERE x IS NOT NULL CALL {{{s}}} RETURN count(x),avg(x)", .{ source, body });
            defer a.free(control);
            try compareReductionPlans(text, control);
        }
    }
}

test "reached container certificates preserve authority and fallible key expressions" {
    const a = std.testing.allocator;
    const Host = struct {
        fn project(_: *anyopaque, _: std.mem.Allocator, _: *const v.Node) anyerror!?*const v.Node {
            return error.CypherInvalidArgument;
        }
    };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(a, "UNWIND $groups AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN avg(y)", .{}, &diagnostic);
    defer compiled.deinit();
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.project_node = Host.project;
    const backend: b.Backend = .{ .ptr = &fixture, .vtable = &callbacks };
    try std.testing.expectError(error.CypherInvalidArgument, runtime.execute(a, backend, graph, &compiled, &.{.{ .name = "groups", .value = .{ .list = &.{ .{ .list = &.{.{ .integer = 0 }} }, .{ .list = &.{.{ .node = &Fixture.nodes[0] }} } } } }}, .{}));
    var nested: [66]v.Value = undefined;
    nested[0] = .{ .integer = 0 };
    for (1..nested.len) |i| nested[i] = .{ .list = nested[i - 1 .. i] };
    for ([_][]const u8{ "1/0 AS bad,j", "j,1/0 AS bad" }) |keys| {
        const text = try std.fmt.allocPrint(a, "WITH $payload AS j UNWIND [1] AS x CALL {{WITH {s},avg(x) AS y RETURN y}} RETURN avg(y)", .{keys});
        defer a.free(text);
        var parsed = try c.compile(a, text, .{}, &diagnostic);
        defer parsed.deinit();
        // The original CALL memo validates captured structure before running
        // its body. Valid structure must still execute an unused failing key.
        try std.testing.expectError(error.CypherValueDepthExceeded, runtime.execute(a, fixture.backend(), graph, &parsed, &.{.{ .name = "payload", .value = nested[nested.len - 1] }}, .{}));
        try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(a, fixture.backend(), graph, &parsed, &.{.{ .name = "payload", .value = .{ .list = &.{.{ .integer = 0 }} } }}, .{}));
    }
}

test "composed arithmetic and reached-read certificates release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var callbacks = backing.vtable.*;
            callbacks.resize = std.mem.Allocator.noResize;
            callbacks.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &callbacks };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{.{ .name = "payload", .value = .{ .list = &.{ .{ .integer = 1 }, .{ .integer = 2 } } } }}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "UNWIND [1,2,1] AS g UNWIND range(-4,4)+range(-2,6) AS x RETURN g,avg(DISTINCT x),count(DISTINCT x) ORDER BY g",
        "UNWIND [1,2,1] AS g UNWIND range(-8,8,2)+range(-6,9,3) AS x RETURN g,avg(DISTINCT x),count(DISTINCT x) ORDER BY g",
        "UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x RETURN 3-2*i AS g,avg(x) ORDER BY g LIMIT 1",
        "WITH $payload AS j UNWIND [1,2] AS x CALL {WITH j,avg(x) AS y RETURN y} RETURN avg(y)",
        "UNWIND range(1,10) AS x CALL {RETURN 7 AS y} RETURN count(x),avg(x)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "range pipeline certificates compose affine aliases restrictions and invariant grouping" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, expected: []const u8 }{
        .{ .text = "UNWIND range(1,1000000) AS x WITH x+1 AS y RETURN count(*),sum(y),avg(y),count(DISTINCT y)", .expected = "[[1000000,500001500000,500001.5,1000000]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x>=999991 RETURN count(*),sum(x),avg(x)", .expected = "[[10,9999955,999995.5]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x%2=0 RETURN count(*),sum(x),avg(x)", .expected = "[[500000,250000500000,500001]]" },
        .{ .text = "UNWIND reverse(range(1,1000000)) AS x WITH x+1 AS y WHERE y>=999992 WITH 2*y-1 AS z RETURN count(*),min(z),max(z),sum(z),avg(z),avg(DISTINCT z)", .expected = "[[10,1999983,2000001,19999920,1999992,1999992]]" },
        .{ .text = "UNWIND range(1,1000000) AS i UNWIND [0,1,0] AS x RETURN i-i AS g,count(*)", .expected = "[[0,3000000]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x<0 RETURN 0 AS g,count(*)", .expected = "[]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x<0 RETURN count(*)", .expected = "[[0]]" },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expect(result.stats.intermediate_states < 500);
        const encoded = try std.json.Stringify.valueAlloc(a, result.rows, .{});
        defer a.free(encoded);
        try std.testing.expectEqualStrings(item.expected, encoded);
    }
}

test "tuple and singleton window certificates preserve rank and source ties" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "RETURN i AS g,i+1 AS h,avg(x) AS a ORDER BY g LIMIT 5",
        "RETURN i AS g,7 AS h,avg(x) AS a ORDER BY g LIMIT 5",
        "RETURN i AS g,-i AS h,avg(x) AS a ORDER BY h DESC,g DESC SKIP 2 LIMIT 5",
        "RETURN i AS g,7 AS h,avg(x) AS a ORDER BY h,g LIMIT 5",
        "RETURN i AS g,7 AS h,avg(x) AS a ORDER BY h LIMIT 5",
        "CALL {RETURN 7 AS y} RETURN i,avg(x) ORDER BY i LIMIT 5",
        "CALL {CALL {RETURN 7 AS z} RETURN z AS y} RETURN i,avg(x) ORDER BY i LIMIT 5",
        "CALL {WITH 7 AS z WHERE false RETURN count(*) AS y} RETURN i,avg(x) ORDER BY i LIMIT 5",
        "CALL {RETURN 7 AS y LIMIT 0} RETURN i,avg(x) ORDER BY i LIMIT 5",
    }) |tail| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x {s}", .{tail});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expect(result.rows.len <= 5);
        try std.testing.expect(result.stats.intermediate_states < 1000);
        const small = try std.fmt.allocPrint(a, "UNWIND range(30,1,-1) AS i UNWIND [0,1,0] AS x {s}", .{tail});
        defer a.free(small);
        const control = try std.fmt.allocPrint(a, "UNWIND range(30,1,-1) AS i WITH i WHERE i IS NOT NULL UNWIND [0,1,0] AS x {s}", .{tail});
        defer a.free(control);
        try compareReductionPlans(small, control);
    }
}

test "range pipeline restrictions retain eager errors rounding and stage scopes" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "range(-12,12)",                                    "range(12,-12,-2)",                               "reverse(range(-12,12,3))", "range(1,0)",
        "range(-9223372036854775808,-9223372036854775798)", "range(9223372036854775797,9223372036854775807)",
    }) |source| for ([_][]const u8{
        "WITH x+1 AS y RETURN count(*),min(y),max(y),sum(y),avg(y),avg(DISTINCT y)",
        "WITH -x AS y WHERE y>=-3 AND y<7 RETURN count(*),sum(y),avg(y),count(DISTINCT y)",
        "WITH x AS y WHERE y%3=0 WITH y*2+1 AS z RETURN count(*),sum(z),avg(z),sum(DISTINCT z)",
        "WITH x WHERE (3*x+1)%-5=-2 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE x%4=1 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE x%-4=-1 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE x%0=0 RETURN count(*)",
        "WITH x WHERE x%2=3 RETURN 0 AS g,count(*)",
        "WITH x WHERE x!=0 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE x>=0 WITH x+9223372036854775807 AS y RETURN sum(y)",
        "WITH x WHERE x=0 WITH x+9223372036854775807 AS y RETURN count(*),sum(y),avg(y)",
        "WITH 1/0 AS bad,x WHERE false RETURN count(*)",
        "WITH x WHERE false AND 1/0=1 RETURN count(*)",
        "WITH x WHERE null AND 1/0=1 RETURN count(*)",
        "WITH x WHERE x>0 AND x+9223372036854775807>0 RETURN count(*)",
        "WITH x+1 AS y WHERE x>=0 RETURN count(*),sum(y),avg(y)",
        "WITH x AS y,x+1 AS x WHERE x<5 RETURN count(*),sum(y),avg(y)",
        "WITH x WHERE false RETURN 1 AS g,count(*)",
    }) |tail| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ source, tail });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x WITH x WHERE x IS NOT NULL {s}", .{ source, tail });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
    for ([_][]const u8{ "RETURN i-i AS g,count(*)", "RETURN (i+9223372036854775807)-i AS g,count(*)", "RETURN i AS g,i+9223372036854775807 AS h,avg(x) ORDER BY g LIMIT 2", "CALL {RETURN 1/0 AS y} RETURN i AS g,7 AS h,avg(x) ORDER BY g LIMIT 2", "CALL {RETURN 7 AS i} RETURN i AS g,avg(x) ORDER BY g LIMIT 2" }) |tail| {
        const text = try std.fmt.allocPrint(a, "UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x {s}", .{tail});
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND range(20,1,-1) AS i WITH i WHERE i IS NOT NULL UNWIND [0,1,0] AS x {s}", .{tail});
        defer a.free(control);
        try compareReductionPlans(text, control);
    }
}

test "range pipeline and tuple CALL windows retain full logical stage counts" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, counts: []const usize }{
        .{ .text = "UNWIND range(1,1000000) AS x WITH x+1 AS y WHERE y>=999992 WITH y*2 AS z WHERE z%4=0 RETURN count(*),sum(z),avg(z)", .counts = &.{ 1000000, 10, 5 } },
        .{ .text = "UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x CALL {RETURN 7 AS y} RETURN i AS g,7 AS h,avg(x) ORDER BY h,g LIMIT 5", .counts = &.{ 1000000, 3000000, 3000000 } },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        for (item.counts, 0..) |count, index| try std.testing.expectEqual(count, result.stages[index].output_rows);
    }
}

test "range pipeline certificates and tuple window proofs release failed allocations" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var callbacks = backing.vtable.*;
            callbacks.resize = std.mem.Allocator.noResize;
            callbacks.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &callbacks };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "UNWIND range(1,30) AS x WITH x+1 AS y WHERE y%3=1 WITH y*2 AS z RETURN count(*),sum(z),avg(z),avg(DISTINCT z)",
        "UNWIND range(-30,30) AS x WITH x WHERE x<0 AND x%4=-1 RETURN count(*),sum(x),avg(x)",
        "UNWIND range(1,30) AS x WITH x WHERE x<0 RETURN count(*)",
        "UNWIND range(30,1,-1) AS i UNWIND [0,1,0] AS x CALL {RETURN 7 AS y} RETURN i AS g,-i AS h,avg(x) ORDER BY h,g LIMIT 1",
        "UNWIND range(1,30) AS i UNWIND [0,1,0] AS x RETURN i-i AS g,count(*)",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "cohort stages and symbolic cardinality stay bounded at a million rows" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, control: []const u8 }{
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x%10 AS g RETURN count(*),avg(g)", .control = "UNWIND range(1,9)+range(0,0) AS g RETURN count(*),avg(g)" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN count(DISTINCT x%10)", .control = "UNWIND range(0,9) AS g RETURN count(*)" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN (x%10)%2 AS g,count(*) ORDER BY g", .control = "UNWIND range(1,1000000) AS x RETURN x%2 AS g,count(*) ORDER BY g" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x%10 AS g ORDER BY g LIMIT 5 RETURN avg(g)", .control = "UNWIND range(0,4) AS g RETURN avg(g)" },
        .{ .text = "UNWIND range(1,1000000,2)+range(2,1000000,2) AS x WITH x ORDER BY x LIMIT 5 RETURN avg(x)", .control = "UNWIND range(1,5) AS x RETURN avg(x)" },
        .{ .text = "UNWIND range(7,8) AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .control = "UNWIND [7,8] AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5" },
        .{ .text = "UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 5", .control = "UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) AS a ORDER BY a,i LIMIT 5" },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y RETURN count(*)", .control = "UNWIND range(1,1000000,3)+range(1,1000000) AS x RETURN count(DISTINCT x)" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN avg(9223372036854775807)", .control = "UNWIND [1] AS x RETURN avg(9223372036854775807)" },
        .{ .text = "WITH 10 AS m UNWIND range(1,1000000) AS x RETURN x%m AS g,count(*) ORDER BY g", .control = "UNWIND range(1,1000000) AS x RETURN x%10 AS g,count(*) ORDER BY g" },
        .{ .text = "UNWIND range(7,8) AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 5", .control = "UNWIND range(1,5) AS i UNWIND [0,1,0,0,1,0] AS x RETURN i,avg(x) ORDER BY i" },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var reference = try c.compile(a, item.control, .{}, &diagnostic);
        defer reference.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 }) catch |err| {
            std.debug.print("\n{s}\n", .{item.text});
            return err;
        };
        defer result.deinit();
        var expected = try runtime.execute(a, fixture.backend(), graph, &reference, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualDeep(expected.rows, result.rows);
    }
}

test "cohort global barriers retain shared membership stable order and failures" {
    const a = std.testing.allocator;
    for ([_]struct { source: []const u8, values: []const u8 }{
        .{ .source = "range(-4,4)", .values = "[-4,-3,-2,-1,0,1,2,3,4]" },
        .{ .source = "range(4,-4,-1)", .values = "[4,3,2,1,0,-1,-2,-3,-4]" },
        .{ .source = "range(1,4)+range(1,4)", .values = "[1,2,3,4,1,2,3,4]" },
        .{ .source = "range(1,9,3)+range(1,9)", .values = "[1,4,7,1,2,3,4,5,6,7,8,9]" },
        .{ .source = "range(1,9,2)+range(2,10,2)", .values = "[1,3,5,7,9,2,4,6,8,10]" },
        .{ .source = "range(1,0)", .values = "[]" },
    }) |source| for ([_][]const u8{
        "WITH DISTINCT x%3 AS g RETURN count(*),avg(g),collect(g)",
        "RETURN count(DISTINCT x%3),avg(DISTINCT x%3)",
        "WITH DISTINCT x%6 AS g WITH DISTINCT g%2 AS h RETURN count(*),avg(h),collect(h)",
        "WITH x,x%6 AS g RETURN g%2 AS h,count(*),sum(x),min(x),max(x),avg(x)",
        "WITH DISTINCT x%3 AS g ORDER BY g DESC SKIP 1 LIMIT 2 RETURN avg(g),collect(g)",
        "WITH DISTINCT x%3 AS g ORDER BY 0 LIMIT 3 WITH g ORDER BY g DESC RETURN collect(g)",
        "WITH x WHERE x>2 WITH x%3 AS g ORDER BY 0 LIMIT 3 RETURN collect(g)",
        "WITH x WHERE x>2 WITH DISTINCT x%3 AS g ORDER BY g LIMIT 3 RETURN collect(g)",
        "WITH x WHERE x>2 WITH x%3 AS g ORDER BY x LIMIT 3 RETURN collect(g)",
        "WITH 3 AS m,x RETURN x%m AS g,count(*) ORDER BY g",
        "WITH x AS y, 2 AS m WITH y,m+1 AS k RETURN y%k AS g,count(*)",
        "WITH DISTINCT x AS y WITH y+1 AS z WHERE z>0 RETURN count(*),count(z),count(DISTINCT z)",
        "WITH DISTINCT x AS y WITH 7 AS g RETURN count(*),count(DISTINCT g)",
        "WITH DISTINCT x AS y WITH DISTINCT y-y AS z RETURN count(*),count(z)",
        "WITH x ORDER BY x SKIP 1 LIMIT 5 RETURN count(*),avg(x),collect(x)",
        "WITH x ORDER BY x DESC SKIP 1 LIMIT 5 RETURN count(*),avg(x),collect(x)",
        "WITH DISTINCT x%3 AS g WHERE x>1 RETURN count(*)",
        "WITH DISTINCT x%3 AS g ORDER BY g LIMIT -1 RETURN count(*)",
        "WITH DISTINCT x%3 AS g ORDER BY g LIMIT $missing RETURN avg(g)",
        "WITH DISTINCT x%3 AS g ORDER BY 1/0 LIMIT 2 RETURN count(*)",
    }) |tail| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ source.source, tail });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ source.values, tail });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
    // Regrouping these alternating signs would overflow SUM and change AVG
    // rounding. The cohort merger must retain the original contribution order.
    try compareReductionPlans(
        "UNWIND range(9223372036854775807,9223372036854775807)+range(-9223372036854775807,-9223372036854775807)+range(9223372036854775804,9223372036854775804)+range(-9223372036854775804,-9223372036854775804) AS x RETURN (x%3)%1 AS g,sum(x),avg(x),count(*)",
        "UNWIND [9223372036854775807,-9223372036854775807,9223372036854775804,-9223372036854775804] AS x RETURN (x%3)%1 AS g,sum(x),avg(x),count(*)",
    );
    for ([_][]const u8{ "range(7,8)", "range(8,7,-1)", "range(7,7)", "range(7,6)" }) |outer| for ([_][]const u8{ "i", "avg(x),i", "count(*),i", "a DESC,i" }) |order| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS z UNWIND range(5,1,-1) AS i UNWIND [z,1,z] AS x RETURN i,avg(x) AS a,count(*) ORDER BY {s} LIMIT 2", .{ outer, order });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS z UNWIND [5,4,3,2,1] AS i UNWIND [z,1,z] AS x RETURN i,avg(x) AS a,count(*) ORDER BY {s} LIMIT 2", .{ outer, order });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
}

test "bounded observer domains compose windows periodic keys and repeated ranks" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, expected: []const u8 }{
        .{ .text = "UNWIND range(1,1000000) AS x RETURN avg(7)", .expected = "[[7]]" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN avg(7) AS a ORDER BY a LIMIT 1", .expected = "[[7]]" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN x%10 AS g,count(*) AS n ORDER BY n,g LIMIT 5", .expected = "[[0,100000],[1,100000],[2,100000],[3,100000],[4,100000]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x SKIP 999995 LIMIT 5 RETURN avg(x)", .expected = "[[999998]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x ORDER BY x DESC LIMIT 5 RETURN avg(x)", .expected = "[[999998]]" },
        .{ .text = "UNWIND range(1,1000000)+range(1,1000000) AS x WITH DISTINCT x+1 AS y RETURN avg(y)", .expected = "[[500001.5]]" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN x%10 AS g,count(*) ORDER BY g", .expected = "[[0,100000],[1,100000],[2,100000],[3,100000],[4,100000],[5,100000],[6,100000],[7,100000],[8,100000],[9,100000]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x%10 AS g RETURN g,count(*) ORDER BY g", .expected = "[[0,1],[1,1],[2,1],[3,1],[4,1],[5,1],[6,1],[7,1],[8,1],[9,1]]" },
        .{ .text = "UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i AS g,avg(x) AS a ORDER BY a,g LIMIT 5", .expected = "[[1,0.33333333333333337],[2,0.33333333333333337],[3,0.33333333333333337],[4,0.33333333333333337],[5,0.33333333333333337]]" },
        .{ .text = "UNWIND [7,8] AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .expected = "[[1,0.33333333333333337],[2,0.33333333333333337],[3,0.33333333333333337],[4,0.33333333333333337],[5,0.33333333333333337]]" },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 }) catch |err| {
            std.debug.print("\n{s}\n", .{item.text});
            return err;
        };
        defer result.deinit();
        const encoded = try std.json.Stringify.valueAlloc(a, result.rows, .{});
        defer a.free(encoded);
        try std.testing.expectEqualStrings(item.expected, encoded);
    }
}

test "bounded observer domains retain signed bags scopes and eager failures" {
    const a = std.testing.allocator;
    for ([_]struct { source: []const u8, values: []const u8 }{
        .{ .source = "range(-4,4)", .values = "[-4,-3,-2,-1,0,1,2,3,4]" },
        .{ .source = "range(4,-4,-1)", .values = "[4,3,2,1,0,-1,-2,-3,-4]" },
        .{ .source = "range(1,4)+range(2,5)", .values = "[1,2,3,4,2,3,4,5]" },
        .{ .source = "range(1,9,2)+range(2,10,2)", .values = "[1,3,5,7,9,2,4,6,8,10]" },
        .{ .source = "range(1,0)", .values = "[]" },
    }) |source| for ([_][]const u8{
        "RETURN x%3 AS g,count(*),sum(x),avg(x),min(x),max(x) ORDER BY g",
        "WITH DISTINCT x%3 AS g RETURN g,count(*)",
        "WITH DISTINCT x%3 AS g RETURN g,count(*) ORDER BY g LIMIT 2",
        "WITH DISTINCT 2*x%3 AS g WITH g%2 AS h RETURN h,count(*)",
        "WITH x,x%3 AS g RETURN g,count(*),avg(x) ORDER BY g LIMIT 2",
        "WITH x,x%3 AS g WHERE g<>1 RETURN g,count(*)",
        "WITH x WHERE x>2 RETURN x%3 AS g,count(*)",
        "WITH x WHERE x>2 WITH DISTINCT x%3 AS g RETURN g,count(*)",
        "WITH DISTINCT x%3 AS g WHERE x>2 RETURN g,count(*)",
        "WITH DISTINCT x+1 AS y WHERE x>2 RETURN count(*),avg(y)",
        "WITH DISTINCT x%3 AS g WHERE g>0 RETURN g,count(*)",
        "WITH DISTINCT x+1 AS y RETURN avg(y),collect(y)",
        "WITH DISTINCT x AS y WITH y ORDER BY y DESC SKIP 2 LIMIT 3 RETURN avg(y),collect(y)",
        "WITH x+1 AS y ORDER BY x DESC SKIP 1 LIMIT 3 RETURN avg(y),collect(y)",
        "WITH x ORDER BY x DESC LIMIT 3 WITH x SKIP 1 RETURN avg(x),collect(x)",
        "WITH x LIMIT $missing RETURN count(*)",
        "WITH x SKIP -1 RETURN count(*)",
        "WITH x ORDER BY missing LIMIT 2 RETURN avg(x)",
        "WITH x/0 AS y LIMIT 0 RETURN count(*)",
        "WITH DISTINCT x%0 AS g RETURN g,count(*)",
        "WITH x WHERE x<0 WITH x LIMIT $missing RETURN count(*)",
    }) |tail| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ source.source, tail });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ source.values, tail });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
    for ([_][]const u8{ "[7,8]", "[0,1,0]", "[null,8]" }) |outer| for ([_][]const u8{ "i", "a,i", "a DESC,i DESC", "n,i" }) |order| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS z UNWIND range(5,1,-1) AS i UNWIND [z,1,z] AS x RETURN i,avg(x) AS a,count(*) AS n ORDER BY {s} LIMIT 2", .{ outer, order });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS z UNWIND [5,4,3,2,1] AS i UNWIND [z,1,z] AS x RETURN i,avg(x) AS a,count(*) AS n ORDER BY {s} LIMIT 2", .{ outer, order });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
}

test "ordered lineage composes DISTINCT singleton stages domains and ranked windows" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, expected: []const u8 }{
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x+1 AS y RETURN avg(y)", .expected = "[[500001.5]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT 7 AS y RETURN count(*),sum(y),avg(y)", .expected = "[[1,7,7]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x+1 AS y CALL {RETURN 7 AS z} RETURN avg(y)", .expected = "[[500001.5]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x+1 AS y UNWIND [7] AS z RETURN avg(y)", .expected = "[[500001.5]]" },
        .{ .text = "UNWIND range(1,500000)+range(500001,1000000) AS x WITH x WHERE x%2=0 RETURN count(*),sum(x),avg(x)", .expected = "[[500000,250000500000,500001]]" },
        .{ .text = "UNWIND reverse(range(1,500000)+range(500001,1000000)) AS x WITH x+1 AS y RETURN avg(y)", .expected = "[[500001.5]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x<6 OR x>999995 RETURN count(*),sum(x)", .expected = "[[10,5000005]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x%2=0 RETURN x,count(*) ORDER BY x LIMIT 5", .expected = "[[2,1],[4,1],[6,1],[8,1],[10,1]]" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x+1 AS y RETURN y,count(*) ORDER BY y LIMIT 5", .expected = "[[2,1],[3,1],[4,1],[5,1],[6,1]]" },
        .{ .text = "UNWIND [7] AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .expected = "[[1,0.33333333333333337],[2,0.33333333333333337],[3,0.33333333333333337],[4,0.33333333333333337],[5,0.33333333333333337]]" },
        .{ .text = "UNWIND range(900001,1000000)+range(1,900000) AS x WITH x+1 AS y RETURN y,count(*) ORDER BY y SKIP 2 LIMIT 5", .expected = "[[4,1],[5,1],[6,1],[7,1],[8,1]]" },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expect(result.stats.intermediate_states < 1500);
        const encoded = try std.json.Stringify.valueAlloc(a, result.rows, .{});
        defer a.free(encoded);
        try std.testing.expectEqualStrings(item.expected, encoded);
    }
    for ([_][]const u8{ "UNWIND [7] AS z", "CALL {RETURN 7 AS z}", "WITH 7 AS z", "UNWIND [7] AS z WITH z" }) |prefix| {
        const text = try std.fmt.allocPrint(a, "{s} UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i SKIP 2 LIMIT 5", .{prefix});
        defer a.free(text);
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 3), result.rows[0][0].integer);
        try std.testing.expect(result.stats.intermediate_states < 1500);
    }
}

test "ordered lineage retains bag order empty scopes eager errors and scalar fallbacks" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "range(-10,10)", "reverse(range(-10,10))",                         "range(-10,0)+range(1,10)",                         "reverse(range(-10,0)+range(1,10))", "range(-5,5)+range(-2,8)",
        "range(1,0)",    "range(9223372036854775790,9223372036854775807)", "range(-9223372036854775808,-9223372036854775790)",
    }) |source| for ([_][]const u8{
        "WITH DISTINCT x+1 AS y RETURN count(*),sum(y),avg(y),avg(DISTINCT y)",
        "WITH DISTINCT x-x AS y RETURN count(*),sum(y),avg(y)",
        "WITH x+1 AS y CALL {RETURN 7 AS z} RETURN count(*),sum(y),avg(y)",
        "WITH x AS y CALL {WITH 1 AS z WHERE false RETURN count(*) AS z} RETURN count(*),sum(y),avg(y)",
        "WITH x AS y CALL {RETURN 7 AS z LIMIT 0} RETURN count(*),avg(y)",
        "WITH x AS y CALL {RETURN 1/0 AS z} RETURN count(*),sum(y)",
        "WITH x AS y CALL {RETURN 7 AS y} RETURN count(*),avg(y)",
        "WITH x+1 AS y UNWIND [7] AS z RETURN count(*),sum(y),avg(y)",
        "WITH x+1 AS y UNWIND [] AS z RETURN count(*),sum(y),avg(y)",
        "WITH x WHERE x<0 OR x>3 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE x!=2 AND x%3=0 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE x<0 OR x%3=1 RETURN count(*),sum(x),avg(x)",
        "WITH x WHERE true OR 1/0=1 RETURN count(*)",
        "WITH x WHERE null OR 1/0=1 RETURN count(*)",
        "WITH x WHERE x<0 OR x+9223372036854775807>0 RETURN count(*)",
        "WITH x+1 AS y WHERE x>0 RETURN y,count(*) ORDER BY y LIMIT 2",
        "WITH x WHERE x%2=0 RETURN x,count(*) ORDER BY x DESC SKIP 1 LIMIT 3",
        "WITH DISTINCT x+1 AS y RETURN y,count(*) ORDER BY y SKIP 1 LIMIT 2",
    }) |tail| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ source, tail });
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x WITH x WHERE x IS NOT NULL {s}", .{ source, tail });
        defer a.free(control);
        try compareReductionPlans(text, control);
    };
    for ([_][]const u8{ "[7]", "[7,8]", "[]" }) |prefix| {
        const text = try std.fmt.allocPrint(a, "UNWIND {s} AS z UNWIND range(30,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 2", .{prefix});
        defer a.free(text);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS z UNWIND range(30,1,-1) AS i WITH i,z WHERE i IS NOT NULL UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 2", .{prefix});
        defer a.free(control);
        try compareReductionPlans(text, control);
    }
}

test "ordered lineage records logical DISTINCT CALL filter and singleton prefix stages" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, counts: []const usize }{
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x+1 AS y CALL {RETURN 7 AS z} RETURN avg(y)", .counts = &.{ 1000000, 1000000, 1000000 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT 7 AS y RETURN count(*)", .counts = &.{ 1000000, 1 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x%10 AS g ORDER BY g LIMIT 5 RETURN avg(g)", .counts = &.{ 1000000, 5 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT (x%10)%2 AS g WITH g ORDER BY g LIMIT 1 RETURN avg(g)", .counts = &.{ 1000000, 2, 1 } },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y RETURN count(*)", .counts = &.{ 1333334, 1000000 } },
        .{ .text = "UNWIND range(1,1000000,2)+range(2,1000000,2) AS x WITH x ORDER BY x RETURN avg(x)", .counts = &.{ 1000000, 1000000 } },
        .{ .text = "UNWIND range(1,1000000,2)+range(2,1000000,2) AS x WITH x ORDER BY x LIMIT 500000 RETURN avg(x)", .counts = &.{ 1000000, 500000 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x%10 AS g ORDER BY g LIMIT 500000 RETURN count(*),sum(g)", .counts = &.{ 1000000, 500000 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x,x%10 AS g ORDER BY x LIMIT 5 RETURN avg(g)", .counts = &.{ 1000000, 5 } },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y ORDER BY 0 LIMIT 5 RETURN avg(y)", .counts = &.{ 1333334, 5 } },
        .{ .text = "UNWIND [7,8] AS z WITH z+1 AS k UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .counts = &.{ 2, 2, 2000000, 6000000 } },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y RETURN sum(y),min(y)", .counts = &.{ 1333334, 1000000 } },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y ORDER BY y LIMIT 5 RETURN avg(y)", .counts = &.{ 1333334, 5 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x%10 AS g ORDER BY g LIMIT 5 RETURN avg(g)", .counts = &.{ 1000000, 5 } },
        .{ .text = "WITH reverse(range(7,8)) AS zs UNWIND zs AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .counts = &.{ 2, 2, 2000000, 6000000 } },
        .{ .text = "UNWIND range(1,1000000,2)+range(2,1000000,2) AS x WITH x ORDER BY x LIMIT 5 RETURN avg(x)", .counts = &.{ 1000000, 5 } },
        .{ .text = "UNWIND range(7,8) AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 5", .counts = &.{ 2, 2000000, 6000000 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x SKIP 999995 LIMIT 5 RETURN avg(x)", .counts = &.{ 1000000, 5 } },
        .{ .text = "UNWIND range(1,1000000)+range(1,1000000) AS x WITH DISTINCT x+1 AS y RETURN avg(y)", .counts = &.{ 2000000, 1000000 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH DISTINCT x%10 AS g RETURN g,count(*) ORDER BY g", .counts = &.{ 1000000, 10 } },
        .{ .text = "UNWIND [7,8] AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) AS a ORDER BY a,i LIMIT 5", .counts = &.{ 2, 2000000, 6000000 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x AS y CALL {RETURN 7 AS z LIMIT 0} RETURN count(*)", .counts = &.{ 1000000, 1000000, 0 } },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x WHERE x%2=0 RETURN x,count(*) ORDER BY x LIMIT 5", .counts = &.{ 1000000, 500000 } },
        .{ .text = "UNWIND [7] AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .counts = &.{ 1, 1000000, 3000000 } },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 }) catch |err| {
            std.debug.print("\n{s}\n", .{item.text});
            return err;
        };
        defer result.deinit();
        for (item.counts, 0..) |count, index| try std.testing.expectEqual(count, result.stages[index].output_rows);
    }
}

test "ordered lineage programs release allocations after proof and packet failures" {
    const Campaign = struct {
        fn run(backing: std.mem.Allocator, text: []const u8) !void {
            var callbacks = backing.vtable.*;
            callbacks.resize = std.mem.Allocator.noResize;
            callbacks.remap = std.mem.Allocator.noRemap;
            const a: std.mem.Allocator = .{ .ptr = backing.ptr, .vtable = &callbacks };
            var diagnostic: c.Diagnostic = .{};
            var compiled = try c.compile(a, text, .{}, &diagnostic);
            defer compiled.deinit();
            var fixture: Fixture = .{};
            var table = fixture.backend().vtable.*;
            table.get_many = struct {
                fn getMany(ptr: *anyopaque, allocator: std.mem.Allocator, identities: []const v.Identity) ![]const ?*const v.Node {
                    const nodes = try allocator.alloc(?*const v.Node, identities.len);
                    for (identities, nodes) |identity, *node| node.* = try Fixture.get(ptr, allocator, identity);
                    return nodes;
                }
            }.getMany;
            var result = try runtime.execute(a, .{ .ptr = &fixture, .vtable = &table }, graph, &compiled, &.{}, .{});
            defer result.deinit();
        }
    };
    for ([_][]const u8{
        "MATCH (n:Document) WHERE elementId(n) IN ['{\"kind\":\"node\",\"table\":\"docs\",\"key\":\"a\"}',null,'invalid'] RETURN n.id",
        "UNWIND range(1,150) AS x WITH x,x%3 AS g ORDER BY g%2,x+g LIMIT 5 RETURN avg(x)",
        "UNWIND range(1,150) AS x WITH x,x%3 AS g ORDER BY g,x LIMIT 140 WITH x,g SKIP 130 LIMIT 5 RETURN avg(x)",
        "UNWIND range(1,150) AS x WITH x,x%3 AS g ORDER BY g,x LIMIT 90 RETURN head(collect(x)),last(collect(x)),collect(x)[..3],collect(x)[-3..]",
        "MATCH (n:Document) WHERE antfly.documentKey(n) IN ['a','b','a',null] RETURN n.id ORDER BY n.id",
        "UNWIND range(1,150) AS x WITH x,x%3 AS g ORDER BY g,x SKIP 45 LIMIT 10 RETURN avg(x)",
        "UNWIND range(1,150) AS x WITH x%3 AS g ORDER BY g LIMIT 120 RETURN head(collect(g)),last(collect(g))",
        "UNWIND range(1,150)+range(1,150) AS x WITH x ORDER BY x LIMIT 150 RETURN count(*),sum(x)",
        "UNWIND range(1,20) AS x WITH DISTINCT (x%6)%2 AS g ORDER BY g LIMIT 1 RETURN avg(g)",
        "UNWIND range(1,20) AS x RETURN count(DISTINCT x%3)",
        "UNWIND range(1,20,3)+range(1,20) AS x WITH DISTINCT x AS y RETURN count(*),count(DISTINCT y+1)",
        "UNWIND range(1,20,2)+range(2,20,2) AS x WITH x ORDER BY x SKIP 2 LIMIT 5 RETURN avg(x)",
        "UNWIND range(7,8) AS z UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 2",
        "UNWIND range(1,20) AS x WITH x ORDER BY x DESC SKIP 2 LIMIT 5 RETURN avg(x)",
        "UNWIND range(1,20)+range(1,20) AS x WITH DISTINCT x+1 AS y RETURN avg(y)",
        "UNWIND range(1,20) AS x WITH DISTINCT x%3 AS g RETURN g,count(*) ORDER BY g LIMIT 2",
        "UNWIND [7,8] AS z UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) AS a ORDER BY a,i LIMIT 2",
        "UNWIND range(1,20) AS x WITH DISTINCT x+1 AS y CALL {RETURN 7 AS z} RETURN avg(y)",
        "UNWIND range(1,140) AS x WITH x%3 AS g ORDER BY g SKIP 40 LIMIT 3 RETURN avg(g),collect(g)",
        "UNWIND range(1,150) AS x WITH x%3 AS g ORDER BY g LIMIT 90 RETURN count(*),sum(g)",
        "UNWIND range(1,150) AS x WITH x,x%3 AS g ORDER BY x LIMIT 5 RETURN avg(g)",
        "UNWIND range(1,150,2)+range(2,150,2) AS x WITH x ORDER BY x RETURN avg(x)",
        "UNWIND range(1,450,3)+range(1,450) AS x WITH DISTINCT x AS y ORDER BY 0 SKIP 160 LIMIT 3 RETURN avg(y)",
        "UNWIND [7,8] AS z WITH z+1 AS k UNWIND range(5,1,-1) AS i RETURN i,sum(k) ORDER BY i LIMIT 2",
        "UNWIND range(1,130) AS x RETURN x%65 AS g,count(*) ORDER BY g",
        "UNWIND range(1,450,3)+range(1,450) AS x WITH DISTINCT x AS y RETURN sum(y),min(y),max(y)",
        "UNWIND range(1,450,3)+range(1,450) AS x WITH DISTINCT x AS y ORDER BY y SKIP 40 LIMIT 3 RETURN avg(y)",
        "WITH reverse(range(7,8)) AS zs UNWIND zs AS z UNWIND range(5,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 2",
        "UNWIND range(-20,0)+range(1,20) AS x WITH x WHERE x<3 OR x>17 RETURN count(*),sum(x),avg(x)",
        "UNWIND range(1,20) AS x WITH x+1 AS y RETURN y,count(*) ORDER BY y LIMIT 2",
        "UNWIND [7] AS z UNWIND range(20,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 2",
    }) |text| try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{text});
}

test "ordered lineage singleton probes preserve nested CALL recursion limits" {
    const a = std.testing.allocator;
    // The four-call case reaches the optimized pipeline at depth four. Its
    // child probe must still count the outer CALL, requiring depth five.
    for ([_]struct { text: []const u8, depth: usize }{
        .{ .text = "UNWIND range(1,20) AS x WITH x+1 AS y CALL {CALL {RETURN 7 AS z} RETURN z} RETURN avg(y)", .depth = 3 },
        .{ .text = "UNWIND range(1,20) AS x WITH x+1 AS y CALL {CALL {CALL {CALL {RETURN 7 AS z} RETURN z} RETURN z} RETURN z} RETURN avg(y)", .depth = 5 },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        for (0..item.depth) |depth| {
            try std.testing.expectError(error.CypherRecursionBudgetExceeded, runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .recursion_depth = depth }));
        }
        var result = try runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .recursion_depth = item.depth });
        defer result.deinit();
        try std.testing.expectEqual(@as(f64, 11.5), result.rows[0][0].number);
    }
}

test "symbolic set reducers and weighted cohort windows stay bounded" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, control: []const u8 }{
        .{ .text = "UNWIND range(1,1000000) AS x WITH x%10 AS g ORDER BY g LIMIT 5 RETURN count(*),avg(g)", .control = "UNWIND [0,0,0,0,0] AS g RETURN count(*),avg(g)" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x%10 AS g LIMIT 5 RETURN count(*),avg(g)", .control = "UNWIND range(1,5) AS g RETURN count(*),avg(g)" },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y RETURN min(y),max(y)", .control = "UNWIND range(1,1000000) AS y RETURN min(y),max(y)" },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y RETURN sum(y)", .control = "UNWIND range(1,1000000) AS y RETURN sum(y)" },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y ORDER BY y LIMIT 5 RETURN avg(y)", .control = "UNWIND range(1,5) AS y RETURN avg(y)" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN collect(DISTINCT x%10)", .control = "UNWIND range(1,9)+range(0,0) AS x RETURN collect(DISTINCT x)" },
        .{ .text = "UNWIND range(1,1000000) AS x RETURN sum(DISTINCT x%10)", .control = "UNWIND range(0,9) AS x RETURN sum(x)" },
        .{ .text = "UNWIND range(1,1000000,2)+range(2,1000000,2) AS x WITH x ORDER BY x LIMIT 129 RETURN avg(x)", .control = "UNWIND range(1,129) AS x RETURN avg(x)" },
        .{ .text = "WITH reverse(range(7,8)) AS zs UNWIND zs AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .control = "UNWIND [8,7] AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5" },
        .{ .text = "WITH 10 AS m UNWIND range(1,1000000) AS x WITH m,x RETURN x%m AS g,count(*) ORDER BY g", .control = "UNWIND range(1,1000000) AS x RETURN x%10 AS g,count(*) ORDER BY g" },
        .{ .text = "UNWIND range(1,1000000) AS x WITH x%10 AS g ORDER BY g SKIP 999995 LIMIT 5 RETURN count(*),avg(g)", .control = "UNWIND [9,9,9,9,9] AS g RETURN count(*),avg(g)" },
        .{ .text = "UNWIND range(1,1000000,3)+range(1,1000000) AS x WITH DISTINCT x AS y ORDER BY y DESC SKIP 500000 LIMIT 129 RETURN avg(y)", .control = "UNWIND range(500000,499872,-1) AS y RETURN avg(y)" },
        .{ .text = "WITH reverse(range(7,8)) AS zs UNWIND zs AS z UNWIND range(1000000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 5", .control = "UNWIND range(1,5) AS i UNWIND [0,1,0,0,1,0] AS x RETURN i,avg(x) ORDER BY i" },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var reference = try c.compile(a, item.control, .{}, &diagnostic);
        defer reference.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = 2000, .memory_bytes = 1 << 20 }) catch |err| {
            std.debug.print("\n{s}\n", .{item.text});
            return err;
        };
        defer result.deinit();
        var expected = try runtime.execute(a, fixture.backend(), graph, &reference, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualDeep(expected.rows, result.rows);
    }
}

test "set summaries and weighted ranks preserve encounters scopes and failures" {
    const a = std.testing.allocator;
    for ([_]struct { source: []const u8, first: i64, step: i64 }{
        .{ .source = "range(1,450)", .first = 1, .step = 1 },
        .{ .source = "range(450,1,-1)", .first = 450, .step = -1 },
        .{ .source = "range(-225,224)", .first = -225, .step = 1 },
    }) |source| {
        var values: [450]i64 = undefined;
        for (&values, 0..) |*value, index| value.* = source.first + @as(i64, @intCast(index)) * source.step;
        const literal = try std.json.Stringify.valueAlloc(a, values, .{});
        defer a.free(literal);
        for ([_][]const u8{
            "WITH x%3 AS g ORDER BY g SKIP 140 LIMIT 9 RETURN count(*),sum(g),avg(g),collect(g)",
            "WITH x%3 AS g ORDER BY g DESC LIMIT 5 RETURN collect(g)",
            "WITH x%10 AS g ORDER BY g LIMIT 300 RETURN count(*),sum(g)",
            "WITH x,x%10 AS g ORDER BY x SKIP 130 LIMIT 5 RETURN avg(g),collect(g)",
            "WITH x,x%10 AS g ORDER BY x DESC SKIP 130 LIMIT 5 RETURN avg(g),collect(g)",
            "WITH x,x%10 AS g ORDER BY 0,x LIMIT 5 RETURN avg(g),collect(g)",
            "WITH x,x%10 AS g ORDER BY x LIMIT 5 RETURN g,avg(x),count(*) ORDER BY g",
            "RETURN x%65 AS g,count(*) ORDER BY g",
            "RETURN x%97 AS g,count(*) ORDER BY g",
            "WITH x%3 AS g ORDER BY 0 SKIP 440 LIMIT 20 RETURN collect(g)",
            "WITH x%3 AS g ORDER BY g SKIP 1000 LIMIT 1 RETURN count(*),collect(g)",
            "RETURN collect(DISTINCT x%3),count(*),sum(x)",
            "RETURN x%2 AS g,collect(DISTINCT x%3),count(*) ORDER BY g",
            "WITH x%3 AS g ORDER BY 1/0 LIMIT 1 RETURN count(*)",
            "WITH x%3 AS g ORDER BY g LIMIT -1 RETURN count(*)",
        }) |tail| {
            const query = try std.fmt.allocPrint(a, "UNWIND {s}+{s} AS x {s}", .{ source.source, source.source, tail });
            defer a.free(query);
            const control = try std.fmt.allocPrint(a, "UNWIND {s}+{s} AS x {s}", .{ literal, literal, tail });
            defer a.free(control);
            try compareReductionPlans(query, control);
        }
        for ([_][]const u8{
            "WITH DISTINCT x AS y RETURN count(*),sum(y),min(y),max(y),sum(7),sum(DISTINCT 7)",
            "WITH DISTINCT x AS y WITH 2*y+7 AS z RETURN sum(z),min(z),max(z),count(z)",
            "WITH DISTINCT x AS y ORDER BY y SKIP 220 LIMIT 129 RETURN avg(y),collect(y)",
            "WITH DISTINCT x AS y ORDER BY y DESC SKIP 220 LIMIT 129 RETURN avg(y),collect(y)",
            "WITH DISTINCT x AS y ORDER BY 0 SKIP 160 LIMIT 5 RETURN avg(y),collect(y)",
            "WITH DISTINCT x AS y ORDER BY 0 SKIP 430 LIMIT 20 RETURN avg(y),collect(y)",
        }) |tail| {
            const query = try std.fmt.allocPrint(a, "UNWIND range({d},{d},{d})+{s} AS x {s}", .{ source.first, source.first + 449 * source.step, 3 * source.step, source.source, tail });
            defer a.free(query);
            const sparse = try a.alloc(i64, 150);
            defer a.free(sparse);
            for (sparse, 0..) |*value, index| value.* = values[index * 3];
            const sparse_literal = try std.json.Stringify.valueAlloc(a, sparse, .{});
            defer a.free(sparse_literal);
            const control = try std.fmt.allocPrint(a, "UNWIND {s}+{s} AS x {s}", .{ sparse_literal, literal, tail });
            defer a.free(control);
            try compareReductionPlans(query, control);
        }
    }
    for ([_][]const u8{
        "WITH reverse(range(7,8)) AS zs",
        "WITH reverse(range(7,8)) AS ys,3 AS k WITH ys AS zs,k+1 AS k",
    }) |prefix| {
        const query = try std.fmt.allocPrint(a, "{s} UNWIND zs AS z UNWIND range(5,1,-1) AS i UNWIND [z,1,z] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 2", .{prefix});
        defer a.free(query);
        const control = try std.fmt.allocPrint(a, "{s} UNWIND zs AS z UNWIND [5,4,3,2,1] AS i UNWIND [z,1,z] AS x RETURN i,avg(x) ORDER BY avg(x),i LIMIT 2", .{prefix});
        defer a.free(control);
        try compareReductionPlans(query, control);
    }
    for ([_][]const u8{ "WITH z+1 AS k", "WITH z AS k,z+1 AS j WITH k+1 AS k,j+2 AS j", "WITH z/0 AS k" }) |scope| {
        for ([_][]const u8{ "RETURN i,sum(k),avg(k) ORDER BY i LIMIT 3", "WITH k AS h,i RETURN h,count(*) ORDER BY h LIMIT 3" }) |tail| {
            const query = try std.fmt.allocPrint(a, "UNWIND [7,8] AS z {s} UNWIND range(5,1,-1) AS i {s}", .{ scope, tail });
            defer a.free(query);
            const control = try std.fmt.allocPrint(a, "UNWIND [7,8] AS z {s} UNWIND [5,4,3,2,1] AS i {s}", .{ scope, tail });
            defer a.free(control);
            try compareReductionPlans(query, control);
        }
    }
    try compareReductionPlans("UNWIND range(9223372036854775807,9223372036854775807)+range(1,1)+range(-1,-1) AS x WITH DISTINCT x AS y RETURN sum(y)", "UNWIND [9223372036854775807,1,-1] AS x WITH DISTINCT x AS y RETURN sum(y)");
}

test "retained ordered domains stay compressed through reducers and ranking" {
    const a = std.testing.allocator;
    for ([_]struct { text: []const u8, control: []const u8, states: usize = 2000 }{
        .{ .text = "UNWIND range(1,1300000,2)+range(2,1300000,2) AS x WITH x ORDER BY x RETURN avg(x)", .control = "UNWIND range(1,1300000) AS x RETURN avg(x)" },
        .{ .text = "UNWIND range(1,1300000,2)+range(2,1300000,2) AS x WITH x ORDER BY x LIMIT 650000 RETURN avg(x)", .control = "UNWIND range(1,650000) AS x RETURN avg(x)" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x%10 AS g ORDER BY g LIMIT 650000 RETURN count(*),sum(g)", .control = "UNWIND range(0,4) AS g RETURN 650000,sum(g)*130000" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g ORDER BY x LIMIT 5 RETURN avg(g)", .control = "UNWIND range(1,5) AS g RETURN avg(g)" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g ORDER BY x DESC LIMIT 5 RETURN avg(g)", .control = "UNWIND [0,9,8,7,6] AS g RETURN avg(g)" },
        .{ .text = "UNWIND range(1,1300000,3)+range(1,1300000) AS x WITH DISTINCT x AS y ORDER BY 0 LIMIT 5 RETURN avg(y)", .control = "UNWIND range(1,13,3) AS y RETURN avg(y)" },
        .{ .text = "UNWIND [7,8] AS z WITH z+1 AS k UNWIND range(1300000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5", .control = "UNWIND [8,9] AS k UNWIND range(1300000,1,-1) AS i UNWIND [0,1,0] AS x RETURN i,avg(x) ORDER BY i LIMIT 5" },
        .{ .text = "UNWIND range(1,1300000) AS x RETURN x%65 AS g,count(*) ORDER BY g", .control = "UNWIND range(0,64) AS g RETURN g,20000", .states = 3000 },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g RETURN g,count(*) ORDER BY g", .control = "UNWIND range(0,9) AS g RETURN g,130000" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g ORDER BY g,x LIMIT 5 RETURN avg(x)", .control = "UNWIND [10,20,30,40,50] AS x RETURN avg(x)" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x%10 AS g ORDER BY g LIMIT 650000 RETURN head(collect(g)),last(collect(g))", .control = "UNWIND [0,4] AS g RETURN head(collect(g)),last(collect(g))" },
        .{ .text = "UNWIND range(1,1300000)+range(1,1300000) AS x WITH x ORDER BY x LIMIT 1300000 RETURN count(*),sum(x)", .control = "UNWIND range(1,650000) AS x RETURN 1300000,sum(x)*2" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g ORDER BY g,x LIMIT 650000 RETURN head(collect(x)),last(collect(x))", .control = "UNWIND [10,1299994] AS x RETURN head(collect(x)),last(collect(x))" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g ORDER BY g%2,x+g LIMIT 5 RETURN avg(x)", .control = "UNWIND [2,4,10,6,12] AS x RETURN avg(x)" },
        .{ .text = "UNWIND range(1,1300000) AS x WITH x,x%10 AS g ORDER BY g,x LIMIT 650000 WITH x,g SKIP 500000 LIMIT 5 RETURN avg(x)", .control = "RETURN 1100023.0" },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(a, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var reference = try c.compile(a, item.control, .{}, &diagnostic);
        defer reference.deinit();
        var fixture: Fixture = .{};
        var result = runtime.execute(a, fixture.backend(), graph, &compiled, &.{}, .{ .intermediate_states = item.states, .memory_bytes = 1 << 20 }) catch |err| {
            std.debug.print("\n{s}\n", .{item.text});
            return err;
        };
        defer result.deinit();
        var expected = try runtime.execute(a, fixture.backend(), graph, &reference, &.{}, .{});
        defer expected.deinit();
        try std.testing.expectEqualDeep(expected.rows, result.rows);
    }
}

test "terminal rank packets preserve lexicographic ties multiplicity endpoints and failures" {
    const a = std.testing.allocator;
    for ([_]i64{ 1, -1 }) |step| {
        var values: [180]i64 = undefined;
        for (&values, 0..) |*value, index| value.* = if (step > 0) @as(i64, @intCast(index)) - 90 else 90 - @as(i64, @intCast(index));
        const literal = try std.json.Stringify.valueAlloc(a, values, .{});
        defer a.free(literal);
        for ([_][]const u8{ "g,x", "g DESC,x DESC", "g,-x", "g%2,g,x DESC", "g%2,x+g", "g%2,x-g DESC,g DESC", "g%3,2*x+g DESC,-g,x", "g%2,-x+g,x DESC", "x,g", "0" }) |order| {
            for ([_][]const u8{ "LIMIT 5", "SKIP 65 LIMIT 125", "SKIP 400 LIMIT 5", "LIMIT 0" }) |page| {
                for ([_][]const u8{
                    "RETURN count(*),sum(x),min(x),max(x)",
                    "RETURN avg(x),collect(x)",
                    "RETURN head(collect(g)),last(collect(g)),collect(g)[..3],collect(g)[-3..]",
                    "RETURN head(collect(x)),last(collect(x)),collect(x)[..3],collect(x)[-3..]",
                    "RETURN g,head(collect(x)),last(collect(x)),count(*) ORDER BY g",
                }) |tail| {
                    const query = try std.fmt.allocPrint(a, "UNWIND range({d},{d},{d})+range({d},{d},{d}) AS x WITH x,x%3 AS g ORDER BY {s} {s} {s}", .{ values[0], values[179], step, values[0], values[179], step, order, page, tail });
                    defer a.free(query);
                    const control = try std.fmt.allocPrint(a, "UNWIND {s}+{s} AS x WITH x,x%3 AS g ORDER BY {s} {s} {s}", .{ literal, literal, order, page, tail });
                    defer a.free(control);
                    try compareReductionPlans(query, control);
                }
            }
        }
    }
    for ([_][]const u8{
        "WITH x,x%3 AS g ORDER BY g%2,x+g SKIP 65 LIMIT 150 WITH x,g SKIP 4 LIMIT 130 RETURN head(collect(x)),last(collect(x)),collect(x)[..3],collect(x)[-3..]",
        "WITH x,x%3 AS g ORDER BY g%2,x+g LIMIT 160 WITH x,g WHERE x%2=0 RETURN head(collect(x)),last(collect(x)),collect(x)[..3],collect(x)[-3..]",
        "WITH x,x%3 AS g ORDER BY g,x SKIP 7 LIMIT 150 WITH x,g ORDER BY x DESC SKIP 2 LIMIT 129 RETURN avg(x),collect(x)",
    }) |tail| {
        var values: [180]i64 = undefined;
        for (&values, 1..) |*value, index| value.* = @intCast(index);
        const literal = try std.json.Stringify.valueAlloc(a, values, .{});
        defer a.free(literal);
        const query = try std.fmt.allocPrint(a, "UNWIND range(1,180) AS x {s}", .{tail});
        defer a.free(query);
        const control = try std.fmt.allocPrint(a, "UNWIND {s} AS x {s}", .{ literal, tail });
        defer a.free(control);
        try compareReductionPlans(query, control);
    }
    for ([_][]const u8{
        "WITH x ORDER BY x LIMIT 5 RETURN sum(x)",
        "WITH x ORDER BY x DESC LIMIT 5 RETURN sum(x)",
        "WITH x ORDER BY x RETURN head(collect(x)),last(collect(x))",
        "WITH x ORDER BY x SKIP 1 LIMIT 3 RETURN avg(x)",
        "WITH x ORDER BY x SKIP 1 LIMIT 3 WITH x LIMIT 2 RETURN sum(x)",
    }) |tail| {
        const query = try std.fmt.allocPrint(a, "UNWIND range(9223372036854775805,9223372036854775807)+range(-1,1) AS x {s}", .{tail});
        defer a.free(query);
        const control = try std.fmt.allocPrint(a, "UNWIND [9223372036854775805,9223372036854775806,9223372036854775807,-1,0,1] AS x {s}", .{tail});
        defer a.free(control);
        try compareReductionPlans(query, control);
    }
}

test "qualified element identities use authorized point domains with exact residuals" {
    for ([_]struct { text: []const u8, count: i64, reads: usize }{
        .{ .text = "MATCH (n) WHERE elementId(n)=$id RETURN count(*)", .count = 1, .reads = 1 },
        .{ .text = "MATCH (n:Document) WHERE elementId(n) IN [$id,$id,null,42,'invalid'] RETURN count(*)", .count = 1, .reads = 1 },
        .{ .text = "MATCH (n:Person) WHERE elementId(n)=$id RETURN count(*)", .count = 0, .reads = 0 },
        .{ .text = "MATCH (n:Document) WHERE elementId(n)=$id OR antfly.documentKey(n)='c' RETURN count(*)", .count = 2, .reads = 2 },
        .{ .text = "MATCH (n:Document) WHERE elementId(n)='invalid' RETURN count(*)", .count = 0, .reads = 0 },
    }) |item| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "id", .value = .{ .string = "{\"kind\":\"node\",\"table\":\"docs\",\"key\":\"b\"}" } }}, .{ .scanned_anchors = item.reads });
        defer result.deinit();
        try std.testing.expectEqual(item.count, result.rows[0][0].integer);
        try std.testing.expectEqual(item.reads, result.stats.scanned_anchors);
    }
    try check("MATCH (n) WHERE elementId(n)='{ \"kind\":\"node\",\"table\":\"docs\",\"key\":\"a\"}' RETURN count(*)", "[[0]]");
    try check("MATCH (n) WHERE elementId(n)='{\"kind\":\"relationship\",\"table\":\"docs\",\"key\":\"a\"}' RETURN count(*)", "[[0]]");
}

test "ordered node certificates bound scans preserve residuals and decline fallible projections" {
    const Indexed = struct {
        fixture: Fixture = .{},
        calls: usize = 0,
        certifications: usize = 0,
        fn read(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, max_examined: usize) !?b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (order.len != 1 or !std.mem.eql(u8, order[0].property, "rank")) return null;
            if (limit == 0) {
                self.certifications += 1;
                return .{ .exact_keys = true, .items = &.{}, .next = "0", .examined = 0 };
            }
            const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
            const end = @min(3, start + @min(limit, max_examined));
            var nodes: std.ArrayList(*const v.Node) = .empty;
            for (start..end) |index| {
                const node = &Fixture.nodes[if (order[0].descending) 2 - index else index];
                if (self.fixture.deny_b and std.mem.eql(u8, node.identity.key, "b")) continue;
                try nodes.append(a, node);
            }
            return .{ .exact_keys = true, .items = nodes.items, .examined = end - start, .next = if (end < 3) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn bind(_: *anyopaque, _: std.mem.Allocator, _: catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return Fixture.scan(&self.fixture, a, table, cursor, limit);
        }
    };
    for ([_]struct { text: []const u8, expected: i64, reads: usize, denied: bool = false, unlimited: bool = false }{
        .{ .text = "MATCH (n:Document) RETURN max(n.rank)", .expected = 3, .reads = 1, .unlimited = true },
        .{ .text = "MATCH (n:Document) RETURN n.rank ORDER BY n.rank DESC LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY n.rank DESC LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) RETURN DISTINCT n.rank,n.id ORDER BY n.rank DESC LIMIT 3", .expected = 3, .reads = 3 },
        .{ .text = "MATCH (n:Document) RETURN DISTINCT n.rank AS r ORDER BY r DESC SKIP 1 LIMIT 1", .expected = 2, .reads = 2 },
        .{ .text = "MATCH (n:Document) RETURN n.rank ORDER BY 1,n.rank DESC,n.rank LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) WITH n RETURN n.rank ORDER BY n.rank DESC LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) WITH n AS m WITH m.rank AS s RETURN s AS r ORDER BY r DESC LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) WITH * WHERE n.rank>1 RETURN n.rank ORDER BY n.rank DESC LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) WHERE n.rank IN [1,3] RETURN n.rank ORDER BY n.rank DESC LIMIT 1", .expected = 3, .reads = 1 },
        .{ .text = "MATCH (n:Document) RETURN n.rank AS r ORDER BY r DESC SKIP 1 LIMIT 1", .expected = 2, .reads = 2 },
        .{ .text = "MATCH (n:Document) WHERE n.rank<3 RETURN n.rank ORDER BY n.rank DESC LIMIT 1", .expected = 1, .reads = 3, .denied = true },
    }) |item| {
        var fixture: Indexed = .{ .fixture = .{ .deny_b = item.denied } };
        var callbacks = fixture.fixture.backend().vtable.*;
        callbacks.bind = Indexed.bind;
        callbacks.checkpoint = Indexed.checkpoint;
        callbacks.scan = Indexed.scan;
        callbacks.scan_ordered = Indexed.read;
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, item.text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{}, .{ .scanned_anchors = if (item.unlimited) std.math.maxInt(usize) else item.reads });
        defer result.deinit();
        try std.testing.expectEqual(item.expected, result.rows[0][0].integer);
        try std.testing.expectEqual(item.reads, result.stats.scanned_anchors);
        try std.testing.expect(fixture.calls != 0);
    }
    // Every observer is negotiated before any reads. Equivalent COUNT
    // expressions share one domain traversal, including through aliases.
    {
        var fixture: Indexed = .{};
        var callbacks = fixture.fixture.backend().vtable.*;
        callbacks.bind = Indexed.bind;
        callbacks.checkpoint = Indexed.checkpoint;
        callbacks.scan = Indexed.scan;
        callbacks.scan_ordered = Indexed.read;
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN count(DISTINCT n.rank) AS c,min(n.rank),max(n.rank),count(DISTINCT n.rank) AS again,count(null)", .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{}, .{ .scanned_anchors = 5 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        for (result.rows[0], [_]i128{ 3, 1, 3, 3, 0 }) |actual, expected| try std.testing.expectEqual(expected, actual.integer);
        try std.testing.expectEqual(@as(usize, 5), result.stats.scanned_anchors);
        try std.testing.expectEqual(@as(usize, 3), fixture.certifications);
    }
    for ([_]struct { text: []const u8, expected: []const i128, reads: usize }{
        .{ .text = "MATCH(n:Document) RETURN count(DISTINCT n.rank)+0,coalesce(min(n.rank),0),max(n.rank)-min(n.rank) LIMIT 1", .expected = &.{ 3, 1, 2 }, .reads = 5 },
        .{ .text = "MATCH(n:Document) RETURN min(n.rank) AS lo ORDER BY lo", .expected = &.{1}, .reads = 1 },
        .{ .text = "MATCH(n:Document) RETURN CASE WHEN false THEN max(n.rank) ELSE min(n.rank) END", .expected = &.{1}, .reads = 1 },
        .{ .text = "MATCH(n:Document) RETURN coalesce(min(n.rank),max(n.rank))", .expected = &.{1}, .reads = 1 },
        .{ .text = "MATCH(n:Document) RETURN min(n.rank) LIMIT 0", .expected = &.{}, .reads = 1 },
        .{ .text = "MATCH(n:Document) RETURN min(n.rank) SKIP 1", .expected = &.{}, .reads = 1 },
    }) |item| {
        var indexed: Indexed = .{};
        var provider_callbacks = indexed.fixture.backend().vtable.*;
        provider_callbacks.bind = Indexed.bind;
        provider_callbacks.checkpoint = Indexed.checkpoint;
        provider_callbacks.scan = Indexed.scan;
        provider_callbacks.scan_ordered = Indexed.read;
        var case_diagnostic: c.Diagnostic = .{};
        var case_compiled = try c.compile(std.testing.allocator, item.text, .{}, &case_diagnostic);
        defer case_compiled.deinit();
        var case_result = try runtime.execute(std.testing.allocator, .{ .ptr = &indexed, .vtable = &provider_callbacks }, graph, &case_compiled, &.{}, .{ .scanned_anchors = item.reads });
        defer case_result.deinit();
        try std.testing.expectEqual(@as(usize, @intFromBool(item.expected.len != 0)), case_result.rows.len);
        if (item.expected.len != 0) for (case_result.rows[0], item.expected) |actual, expected| try std.testing.expectEqual(expected, actual.integer);
        try std.testing.expectEqual(item.reads, case_result.stats.scanned_anchors);
    }
    var fixture: Indexed = .{};
    var callbacks = fixture.fixture.backend().vtable.*;
    callbacks.bind = Indexed.bind;
    callbacks.checkpoint = Indexed.checkpoint;
    callbacks.scan = Indexed.scan;
    callbacks.scan_ordered = Indexed.read;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN 1/(n.rank-1) ORDER BY n.rank DESC LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    try std.testing.expectError(error.CypherDivisionByZero, runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    var shadowed = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN n.rank AS r,null AS n ORDER BY n.rank DESC LIMIT 1", .{}, &diagnostic);
    defer shadowed.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &shadowed, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    fixture.calls = 0;
    var representatives = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN DISTINCT n.rank=2 AS k ORDER BY n.rank DESC LIMIT 2", .{}, &diagnostic);
    defer representatives.deinit();
    try std.testing.expectError(error.UnboundCypherVariable, runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &representatives, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.calls);
    var plain = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN n.rank AS r", .{}, &diagnostic);
    defer plain.deinit();
    const stream = (try runtime.NodeCursor.open(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, plain.query, &.{}, .{ .scanned_anchors = 1 })).?;
    defer stream.close();
    try std.testing.expect(try stream.certifyOrder(&.{.{ .property = "r", .descending = true }}));
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const rows = try stream.next(arena.allocator(), 1);
    try std.testing.expectEqual(@as(i64, 3), rows[0][0].value.integer);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
}

test "constant order streams DISTINCT through authenticated primary-order RPC owners" {
    const root = @import("root.zig");
    const test_graph: catalog.Graph = .{ .name = "single", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs" }}, .relationships = &.{} };
    const Owner = struct {
        index: usize,
        records: usize = 0,
        fn bind(_: *anyopaque, _: std.mem.Allocator, _: catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn close(_: *anyopaque) void {}
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            return error.CypherInvalidBackendPage;
        }
        fn get(_: *anyopaque, _: std.mem.Allocator, _: v.Identity) !?*const v.Node {
            return null;
        }
        fn expand(_: *anyopaque, _: std.mem.Allocator, _: catalog.Relationship, _: v.Identity, _: @import("ast.zig").Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
            return .{ .items = &.{} };
        }
        fn ordered(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, maximum: usize) !?b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (order.len != 0 or filters.len != 1 or filters[0].op != .in) return null;
            if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .examined = 0, .next = "0" };
            const count: usize = if (self.index == 0) 2 else 1;
            const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
            const end = @min(count, start + @min(limit, maximum));
            const nodes = try a.alloc(*const v.Node, end - start);
            const cursors = try a.alloc(?[]const u8, nodes.len);
            for (nodes, cursors, start..) |*node, *cursor, ordinal| {
                node.* = &Fixture.nodes[(if (self.index == 0) @as(usize, 0) else 2) + ordinal];
                cursor.* = if (ordinal + 1 < count) try std.fmt.allocPrint(a, "{d}", .{ordinal + 1}) else null;
            }
            self.records += nodes.len;
            return .{ .items = nodes, .cursors = cursors, .examined = nodes.len, .next = if (end < count) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
        fn backend(self: *@This()) b.Backend {
            return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .scan_ordered = ordered } };
        }
        fn invoke(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            const endpoint: *root.rpc.Owner = @ptrCast(@alignCast(ptr));
            return endpoint.execute(a, body);
        }
    };
    var owners = [_]Owner{ .{ .index = 0 }, .{ .index = 1 } };
    var endpoints = [_]root.rpc.Owner{ .{ .backend = owners[0].backend(), .graph = test_graph, .table = "docs" }, .{ .backend = owners[1].backend(), .graph = test_graph, .table = "docs" } };
    var clients = [_]root.rpc.Client{ .{ .channel = .{ .ptr = &endpoints[0], .call = Owner.invoke }, .incarnation = test_graph.incarnation, .revision = test_graph.revision }, .{ .channel = .{ .ptr = &endpoints[1], .call = Owner.invoke }, .incarnation = test_graph.incarnation, .revision = test_graph.revision } };
    const targets = [_]root.distributed.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    const snapshots = [_]root.distributed.Snapshot{ .{ .backend = clients[0].backend(), .ptr = &owners[0], .validate = Owner.checkpoint, .close = Owner.close }, .{ .backend = clients[1].backend(), .ptr = &owners[1], .validate = Owner.checkpoint, .close = Owner.close } };
    var set: root.distributed.ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &owners, .checkpoint = Owner.checkpoint, .admit = undefined, .capture = undefined } };
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) WHERE n.rank IN [1,2,3] RETURN DISTINCT n ORDER BY 1 SKIP 1 LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, set.backend(), test_graph, &compiled, &.{}, .{ .scanned_anchors = 4 });
    defer result.deinit();
    try std.testing.expectEqualStrings("b", result.rows[0][0].node.identity.key);
    try std.testing.expectEqual(owners[0].records + owners[1].records, result.stats.scanned_anchors);
}

test "coarse RPC prefixes refine owner ties and bound partial DISTINCT across windows" {
    const root = @import("root.zig");
    const A = std.mem.Allocator;
    const g: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs" }}, .relationships = &.{} };
    const F = struct {
        const Self = @This();
        nodes: [8]v.Node = undefined,
        properties: [8][2]v.Entry = undefined,
        fn init(self: *@This()) void {
            const ranks = [_]?i64{ 2, 1, 2, null, 2, 1, 0, null };
            const ids = [_][]const u8{ "x", "p", "x", "n", "y", "p", "q", "n" };
            const keys = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" };
            for (&self.nodes, &self.properties, ranks, ids, keys) |*node, *properties, rank, id, key| {
                properties.* = .{ .{ .name = "rank", .value = if (rank) |number| .{ .integer = number } else .null }, .{ .name = "id", .value = .{ .string = id } } };
                node.* = .{ .identity = .{ .table = "docs", .key = key }, .labels = &.{"Document"}, .properties = properties };
            }
        }
        fn check(_: *anyopaque) !void {}
        fn close(_: *anyopaque) void {}
        fn bind(_: *anyopaque, _: A, _: catalog.Graph) !void {}
        fn get(_: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
            return null;
        }
        fn expand(_: *anyopaque, _: A, _: catalog.Relationship, _: v.Identity, _: @import("ast.zig").Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
            return .{ .items = &.{} };
        }
        const Owner = struct {
            fixture: *Self,
            partition: usize,
            records: usize = 0,
            probes: usize = 0,
            exact_keys: bool = true,
            fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
                return page(ptr, a, after, limit, null);
            }
            fn ordered(ptr: *anyopaque, a: A, _: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, maximum: usize) !?b.Page(*const v.Node) {
                if (order.len == 0 or order.len > 2) return null;
                for (order) |field| if (!std.mem.eql(u8, field.property, "rank") and !std.mem.eql(u8, field.property, "id")) return null;
                if (limit == 0) {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.probes += 1;
                    return .{ .items = &.{}, .cursors = &.{}, .next = "0", .examined = 0, .exact_keys = self.exact_keys, .stable_ties = false };
                }
                return try page(ptr, a, after, @min(limit, maximum), order);
            }
            fn page(ptr: *anyopaque, a: A, after: ?[]const u8, limit: usize, order: ?[]const b.PropertyOrder) !b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
                const end = @min(4, start + limit);
                const nodes = try a.alloc(*const v.Node, end - start);
                const cursors = try a.alloc(?[]const u8, nodes.len);
                var sorted = [_]usize{ 0, 1, 2, 3 };
                const SortContext = struct { owner: @TypeOf(self), fields: []const b.PropertyOrder };
                if (order) |fields| std.mem.sort(usize, &sorted, SortContext{ .owner = self, .fields = fields }, struct {
                    fn less(context: SortContext, left: usize, right: usize) bool {
                        for (context.fields) |field| {
                            const l = (v.Value{ .node = &context.owner.fixture.nodes[context.owner.partition * 4 + left] }).property(field.property);
                            const r = (v.Value{ .node = &context.owner.fixture.nodes[context.owner.partition * 4 + right] }).property(field.property);
                            const comparison = v.Value.order(l, r) catch unreachable;
                            if (comparison != .eq) return (comparison == .lt) != field.descending;
                        }
                        return left > right;
                    }
                }.less);
                for (nodes, cursors, start..) |*node, *cursor, ordinal| {
                    const position = if (order != null) sorted[ordinal] else ordinal;
                    node.* = &self.fixture.nodes[self.partition * 4 + position];
                    cursor.* = if (ordinal + 1 < 4) try std.fmt.allocPrint(a, "{d}", .{ordinal + 1}) else null;
                }
                self.records += nodes.len;
                return .{ .items = nodes, .cursors = cursors, .next = if (end < 4) try std.fmt.allocPrint(a, "{d}", .{end}) else null, .examined = nodes.len, .exact_keys = self.exact_keys, .stable_ties = order == null };
            }
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = Self.bind, .checkpoint = Self.check, .scan = scan, .get = Self.get, .expand = Self.expand, .scan_ordered = ordered } };
            }
        };
        fn invoke(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const owner: *root.rpc.Owner = @ptrCast(@alignCast(ptr));
            return owner.execute(a, body);
        }
        fn run(a: A, backend: b.Backend, text: []const u8) ![]const u8 {
            var diagnostic: c.Diagnostic = .{};
            var compiled = c.compile(a, text, .{}, &diagnostic) catch |err| return a.dupe(u8, @errorName(err));
            defer compiled.deinit();
            var result = runtime.execute(a, backend, g, &compiled, &.{}, .{}) catch |err| return a.dupe(u8, @errorName(err));
            defer result.deinit();
            return std.json.Stringify.valueAlloc(a, result.rows, .{});
        }
    };
    var fixture: F = .{};
    fixture.init();
    // Catalog encounter order intentionally differs from global key order.
    var owners = [_]F.Owner{ .{ .fixture = &fixture, .partition = 1 }, .{ .fixture = &fixture, .partition = 0 } };
    var endpoints = [_]root.rpc.Owner{ .{ .backend = owners[0].backend(), .graph = g, .table = "docs" }, .{ .backend = owners[1].backend(), .graph = g, .table = "docs" } };
    var clients = [_]root.rpc.Client{ .{ .channel = .{ .ptr = &endpoints[0], .call = F.invoke }, .incarnation = 1, .revision = 1 }, .{ .channel = .{ .ptr = &endpoints[1], .call = F.invoke }, .incarnation = 1, .revision = 1 } };
    const targets = [_]root.distributed.Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    const snapshots = [_]root.distributed.Snapshot{ .{ .backend = clients[0].backend(), .ptr = &fixture, .validate = F.check, .close = F.close }, .{ .backend = clients[1].backend(), .ptr = &fixture, .validate = F.check, .close = F.close } };
    var set: root.distributed.ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixture, .checkpoint = F.check, .admit = undefined, .capture = undefined } };
    // Owners share NULL and non-NULL domains. Cardinality observes the global
    // merged key stream, including legacy providers that ignore tuple bounds.
    for ([_][]const u8{
        "MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY n.rank DESC",
        "MATCH (n:Document) RETURN count(DISTINCT n.rank)",
        "MATCH (n:Document) RETURN count(DISTINCT n.id)",
        "MATCH (n:Document) WHERE false RETURN count(DISTINCT 7)",
        "MATCH (n:Document) RETURN count(DISTINCT 7),count(null)",
        "MATCH (n:Document) RETURN count(DISTINCT n.rank)+0,coalesce(min(n.rank),0),max(n.rank)-min(n.rank) LIMIT 1",
        "MATCH (n:Document) RETURN min(n.rank) AS lo ORDER BY lo",
        "MATCH (n:Document) WITH DISTINCT n.rank AS r ORDER BY r DESC RETURN r",
        "MATCH (n:Document) WITH DISTINCT n.rank AS r ORDER BY r DESC RETURN count(r)",
    }) |query| for ([_]bool{ true, false }) |exact_keys| {
        for (&owners) |*owner| owner.exact_keys = exact_keys;
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var ordinary_callbacks = set.backend().vtable.*;
        ordinary_callbacks.scan_ordered = null;
        const ordinary: b.Backend = .{ .ptr = &set, .vtable = &ordinary_callbacks };
        try std.testing.expectEqualStrings(try F.run(a, ordinary, query), try F.run(a, set.backend(), query));
    };
    // Legacy coarse providers may mix numeric representations inside a key
    // group. Retain the primary representative until every tie was observed.
    fixture.properties[3][0].value = .{ .number = 1.0 };
    for (&owners) |*owner| owner.exact_keys = false;
    for ([_][]const u8{
        "MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY n.rank DESC",
        "MATCH (n:Document) RETURN DISTINCT n.rank ORDER BY n.rank DESC LIMIT 9",
    }) |query| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var ordinary_callbacks = set.backend().vtable.*;
        ordinary_callbacks.scan_ordered = null;
        const ordinary: b.Backend = .{ .ptr = &set, .vtable = &ordinary_callbacks };
        try std.testing.expectEqualStrings(try F.run(a, ordinary, query), try F.run(a, set.backend(), query));
    }
    fixture.properties[3][0].value = .null;
    for (&owners) |*owner| owner.exact_keys = true;
    for ([_][]const u8{ "", "WITH DISTINCT n ", "WITH n AS m WITH DISTINCT m WITH m AS n " }) |stage|
        for ([_][]const u8{ "n AS r", "n.id AS r,n.rank AS s", "DISTINCT n.id AS r,n.rank AS s", "DISTINCT n.rank AS r" }) |projection|
            for ([_][]const u8{ "ASC", "DESC" }) |direction|
                for ([_]usize{ 0, 1, 3 }) |skip|
                    for ([_]usize{ 0, 1, 3, 9 }) |limit| {
                        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
                        defer arena.deinit();
                        const a = arena.allocator();
                        const query = try std.fmt.allocPrint(a, "MATCH (n:Document) {s}RETURN {s} ORDER BY n.rank {s} SKIP {d} LIMIT {d}", .{ stage, projection, direction, skip, limit });
                        const control = try std.fmt.allocPrint(a, "MATCH (n:Document) {s}RETURN {s} ORDER BY CASE WHEN true THEN n.rank ELSE n.rank END {s} SKIP {d} LIMIT {d}", .{ stage, projection, direction, skip, limit });
                        try std.testing.expectEqualStrings(try F.run(a, set.backend(), control), try F.run(a, set.backend(), query));
                    };
    for ([_][]const u8{ "", "WITH DISTINCT n ", "WITH n AS m WITH m AS n " }) |stage| {
        for ([_][]const u8{ "", "WHERE n.rank IS NULL ", "WHERE false " }) |predicate| {
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const query = try std.fmt.allocPrint(a, "MATCH (n:Document) {s}{s}RETURN min(n.rank) AS lo,max(n.rank) AS hi", .{ predicate, stage });
            const control = try std.fmt.allocPrint(a, "MATCH (n:Document) {s}{s}RETURN min(CASE WHEN true THEN n.rank ELSE n.rank END) AS lo,max(n.rank) AS hi", .{ predicate, stage });
            try std.testing.expectEqualStrings(try F.run(a, set.backend(), control), try F.run(a, set.backend(), query));
        }
    }
    for ([_]struct { query: []const u8, depth: usize }{
        .{ .query = "MATCH (n:Document) RETURN min(n.rank),max(n.rank)", .depth = 1 },
        .{ .query = "MATCH (n:Document) WITH n WITH n RETURN min(n.rank),max(n.rank)", .depth = 3 },
    }) |case| {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, case.query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, set.backend(), g, &compiled, &.{}, .{ .recursion_depth = case.depth });
        defer result.deinit();
        try std.testing.expectEqual(@as(i128, 0), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i128, 2), result.rows[0][1].integer);
    }
    for ([_][]const u8{ "n.id", "n.rank", "n.rank,n.id" }) |group|
        for ([_][]const u8{ "count(*)", "count(n)", "count(n.rank),min(n.rank),max(n.rank)", "count(DISTINCT n.id)", "min(n.rank),max(n.rank)" }) |observers|
            for ([_][]const u8{ "ASC", "DESC" }) |direction|
                for ([_]usize{ 0, 1, 3 }) |skip|
                    for ([_]usize{ 0, 1, 9 }) |limit| {
                        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
                        defer arena.deinit();
                        const a = arena.allocator();
                        // Include every grouping key in its corresponding order.
                        const order = if (std.mem.eql(u8, group, "n.rank,n.id")) try std.fmt.allocPrint(a, "n.rank {s},n.id {s}", .{ direction, direction }) else try std.fmt.allocPrint(a, "{s} {s}", .{ group, direction });
                        const query = try std.fmt.allocPrint(a, "MATCH (n:Document) RETURN {s},{s} ORDER BY {s} SKIP {d} LIMIT {d}", .{ group, observers, order, skip, limit });
                        const control_order = if (std.mem.eql(u8, group, "n.rank,n.id")) try std.fmt.allocPrint(a, "CASE WHEN true THEN n.rank ELSE n.rank END {s},n.id {s}", .{ direction, direction }) else try std.fmt.allocPrint(a, "CASE WHEN true THEN {s} ELSE {s} END {s}", .{ group, group, direction });
                        const control = try std.fmt.allocPrint(a, "MATCH (n:Document) RETURN {s},{s} ORDER BY {s} SKIP {d} LIMIT {d}", .{ group, observers, control_order, skip, limit });
                        try std.testing.expectEqualStrings(try F.run(a, set.backend(), control), try F.run(a, set.backend(), query));
                    };
    for ([_][]const u8{
        "MATCH (n:Document) RETURN n.id AS k,count(*) AS k ORDER BY k LIMIT 2",
        "MATCH (n:Document) WITH n.id AS k,n RETURN n.id,count(*) AS k ORDER BY k LIMIT 2",
        "MATCH (n:Document) RETURN n.id,count(*) AS n ORDER BY n.id LIMIT 2",
        "MATCH (n:Document) WITH n.id AS k,count(*) AS copies ORDER BY k LIMIT 2 UNWIND range(1,1000) AS i RETURN count(*),sum(copies)",
        "MATCH (n:Document) WITH n.id AS k,max(n.rank) AS r ORDER BY k LIMIT 3 RETURN sum(r)",
    }) |query| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const order_start = std.mem.indexOf(u8, query, "ORDER BY ").? + "ORDER BY ".len;
        const order_end = std.mem.indexOf(u8, query, " LIMIT").?;
        const control = try std.fmt.allocPrint(arena.allocator(), "{s}CASE WHEN true THEN {s} END{s}", .{ query[0..order_start], query[order_start..order_end], query[order_end..] });
        try std.testing.expectEqualStrings(try F.run(arena.allocator(), set.backend(), control), try F.run(arena.allocator(), set.backend(), query));
    }
    {
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN n.id AS id,n.rank AS rank", .{}, &diagnostic);
        defer compiled.deinit();
        const cursor = (try runtime.NodeCursor.open(std.testing.allocator, set.backend(), g, compiled.query, &.{}, .{})).?;
        defer cursor.close();
        try std.testing.expect(!try cursor.certifyOrder(&.{.{ .property = "rank", .descending = true }}));
        try std.testing.expect(try cursor.certifyWindow(&.{.{ .property = "rank", .descending = true }}, 3));
        const probes = owners[0].probes + owners[1].probes;
        try std.testing.expect(!try cursor.certifyOrder(&.{.{ .property = "rank", .descending = true }}));
        try std.testing.expectEqual(probes, owners[0].probes + owners[1].probes);
        try std.testing.expectEqual(@as(?u64, null), try cursor.countRows());
        try std.testing.expect(!try cursor.enableWeights(.exact));
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        var values: std.ArrayList([]const v.Value) = .empty;
        while (!cursor.done) {
            const rows = try cursor.next(arena.allocator(), 1);
            for (rows) |row| {
                const cells = try arena.allocator().alloc(v.Value, row.len);
                for (row, cells) |entry, *cell| cell.* = entry.value;
                try values.append(arena.allocator(), cells);
            }
        }
        try std.testing.expectEqualStrings(try F.run(arena.allocator(), set.backend(), "MATCH (n:Document) RETURN n.id,n.rank ORDER BY CASE WHEN true THEN n.rank ELSE n.rank END DESC LIMIT 3"), try std.json.Stringify.valueAlloc(arena.allocator(), values.items, .{}));
    }
}

test "equality-fixed extrema finish at their exact coarse witness budget" {
    const F = struct {
        fn ordered(_: *anyopaque, a: std.mem.Allocator, _: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, _: ?[]const u8, limit: usize, maximum: usize) !?b.Page(*const v.Node) {
            if (order.len != 1 or !std.mem.eql(u8, order[0].property, "rank")) return null;
            if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .next = "witness", .examined = 0, .exact_keys = true, .stable_ties = false };
            // Match native admission: a data request needs a positive remaining
            // allowance, even when its exclusive tuple would discover EOF.
            if (maximum == 0) return error.CypherAnchorBudgetExceeded;
            if (order[0].after != null) return .{ .items = &.{}, .examined = 0, .exact_keys = true, .stable_ties = false };
            return .{ .items = try a.dupe(*const v.Node, &.{&Fixture.nodes[1]}), .next = "witness", .examined = 1, .exact_keys = true, .stable_ties = false };
        }
    };
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.scan_ordered = F.ordered;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) WHERE n.rank=2 RETURN min(n.rank),max(n.rank)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{}, .{ .scanned_anchors = 2 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.stats.scanned_anchors);
    try std.testing.expectEqual(@as(i128, 2), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(i128, 2), result.rows[0][1].integer);
}

test "group observers preserve shared values when retiring invocation arenas" {
    const F = struct {
        fn ordered(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, _: usize) !?b.Page(*const v.Node) {
            if (order.len != 1 or order[0].descending or !std.mem.eql(u8, order[0].property, "id")) return null;
            if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .next = "", .examined = 0 };
            return try Fixture.scan(ptr, a, table, after, limit);
        }
    };
    var layers: [18][2]v.Value = undefined;
    var shared: v.Value = .{ .integer = 1 };
    for (&layers) |*layer| {
        layer.* = .{ shared, shared };
        shared = .{ .list = layer };
    }
    var fixture: Fixture = .{};
    var callbacks = fixture.backend().vtable.*;
    callbacks.scan_ordered = F.ordered;
    var suffix: [18 * 3]u8 = undefined;
    for (0..18) |index| @memcpy(suffix[index * 3 ..][0..3], "[0]");
    const query = try std.fmt.allocPrint(std.testing.allocator, "MATCH (n:Document) WITH n.id AS k,$p AS p,min($p) AS lo,max($p) AS hi ORDER BY k LIMIT 2 RETURN k,lo{s},hi{s}", .{ &suffix, &suffix });
    defer std.testing.allocator.free(query);
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, graph, &compiled, &.{.{ .name = "p", .value = shared }}, .{ .memory_bytes = 2 << 20 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    for (result.rows) |row| {
        try std.testing.expectEqual(@as(i128, 1), row[1].integer);
        try std.testing.expectEqual(@as(i128, 1), row[2].integer);
    }
    try std.testing.expect(result.stats.memory_bytes <= 2 << 20);
}

test "ordered groups retire reducers across a broad exact-budget prefix" {
    const F = struct {
        nodes: []const v.Node,
        reads: usize = 0,
        requests: usize = 0,
        ordered_requests: usize = 0,
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(ptr: *anyopaque, a: std.mem.Allocator, _: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const start = if (after) |text| try std.fmt.parseInt(usize, text, 10) else 0;
            const end = @min(self.nodes.len, start + limit);
            const items = try a.alloc(*const v.Node, end - start);
            for (items, start..) |*item, index| item.* = &self.nodes[index];
            self.reads += items.len;
            self.requests += 1;
            return .{ .items = items, .examined = items.len, .next = if (end < self.nodes.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null, .exact_keys = true, .stable_ties = false };
        }
        fn ordered(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, _: []const b.PropertyFilter, fields: []const b.PropertyOrder, after: ?[]const u8, limit: usize, _: usize) !?b.Page(*const v.Node) {
            if (fields.len == 0 or fields.len > 2 or !std.mem.eql(u8, fields[0].property, "id") or fields[0].descending) return null;
            if (fields.len == 2 and !std.mem.eql(u8, fields[1].property, "rank")) return null;
            if (limit == 0) return .{ .items = &.{}, .examined = 0, .next = "0", .exact_keys = true, .stable_ties = false };
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.ordered_requests += 1;
            return try scan(ptr, a, table, after, limit);
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const nodes = try a.alloc(v.Node, 5000);
    for (nodes, 0..) |*node, index| {
        const key = try std.fmt.allocPrint(a, "{d:0>6}", .{index});
        node.* = .{ .identity = .{ .table = "docs", .key = key }, .labels = &.{"Document"}, .properties = try a.dupe(v.Entry, &.{ .{ .name = "id", .value = .{ .string = key } }, .{ .name = "rank", .value = .{ .integer = @intCast(index) } } }) };
    }
    var fixture: F = .{ .nodes = nodes };
    const backend: b.Backend = .{ .ptr = &fixture, .vtable = &.{ .bind = Fixture.bind, .checkpoint = F.checkpoint, .scan = F.scan, .get = Fixture.get, .expand = Fixture.expand, .scan_ordered = F.ordered } };
    for ([_][]const u8{ "", " LIMIT 5000" }) |suffix| {
        fixture.reads = 0;
        fixture.requests = 0;
        var diagnostic: c.Diagnostic = .{};
        const text = try std.fmt.allocPrint(a, "MATCH (n:Document) RETURN DISTINCT n.id ORDER BY n.id{s}", .{suffix});
        var compiled = try c.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .scanned_anchors = 5000, .memory_bytes = 16 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5000), result.rows.len);
        try std.testing.expectEqual(@as(usize, 5000), fixture.reads);
        try std.testing.expect(fixture.requests < 100);
    }
    {
        fixture.reads = 0;
        fixture.requests = 0;
        fixture.ordered_requests = 0;
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) WHERE n.rank<0 OR n.rank>10000 RETURN DISTINCT n.id ORDER BY n.id", .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .scanned_anchors = 5000, .memory_bytes = 16 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 0), result.rows.len);
        try std.testing.expectEqual(@as(usize, 5000), fixture.reads);
        try std.testing.expect(fixture.requests < 100);
        try std.testing.expectEqual(fixture.requests, fixture.ordered_requests);
    }
    for ([_][]const u8{ "count(*)", "max(n.rank)", "count(n.rank),min(n.rank),max(n.rank)" }) |observer| for ([_][]const u8{ "", " LIMIT 5000" }) |suffix| {
        fixture.reads = 0;
        const query = try std.fmt.allocPrint(a, "MATCH (n:Document) RETURN n.id,{s} ORDER BY n.id{s}", .{ observer, suffix });
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .scanned_anchors = 5000, .memory_bytes = 16 << 20 });
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 5000), fixture.reads);
        try std.testing.expectEqual(@as(usize, 5000), result.stats.scanned_anchors);
        try std.testing.expectEqual(@as(usize, 5000), result.rows.len);
        try std.testing.expect(result.stats.intermediate_states < 900000);
        try std.testing.expectEqualStrings("000000", result.rows[0][0].string);
        try std.testing.expectEqualStrings("004999", result.rows[4999][0].string);
        for (result.rows, 0..) |row, index| {
            const expected: i128 = if (std.mem.eql(u8, observer, "count(*)")) 1 else @intCast(index);
            try std.testing.expectEqual(if (row.len == 4) @as(i128, 1) else expected, row[1].integer);
            if (row.len == 4) {
                try std.testing.expectEqual(@as(i128, @intCast(index)), row[2].integer);
                try std.testing.expectEqual(@as(i128, @intCast(index)), row[3].integer);
            }
        }
    };
    for ([_][]const u8{ " SKIP 4900 LIMIT 5", " SKIP 4999", " SKIP 6000 LIMIT 5" }) |suffix| {
        fixture.reads = 0;
        const query = try std.fmt.allocPrint(a, "MATCH (n:Document) RETURN n.id,count(*),min(n.rank),max(n.rank) ORDER BY n.id{s}", .{suffix});
        var diagnostic: c.Diagnostic = .{};
        var compiled = try c.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .scanned_anchors = 5000, .memory_bytes = 256 << 10 });
        defer result.deinit();
        const expected: usize = if (std.mem.indexOf(u8, suffix, "4900") != null) 5 else if (std.mem.indexOf(u8, suffix, "4999") != null) 1 else 0;
        try std.testing.expectEqual(expected, result.rows.len);
        if (expected != 0) try std.testing.expectEqualStrings(if (expected == 5) "004900" else "004999", result.rows[0][0].string);
    }
    // The offset is deliberately much larger than the result window.
    // Retiring groups must release state without retaining skipped output.
    const large = try a.alloc(v.Node, 50000);
    for (large, 0..) |*node, index| {
        const key = try std.fmt.allocPrint(a, "{d:0>6}", .{index});
        node.* = .{ .identity = .{ .table = "docs", .key = key }, .labels = &.{"Document"}, .properties = try a.dupe(v.Entry, &.{ .{ .name = "id", .value = .{ .string = key } }, .{ .name = "rank", .value = .{ .integer = @intCast(index) } } }) };
    }
    fixture.nodes = large;
    var diagnostic: c.Diagnostic = .{};
    var compiled = try c.compile(std.testing.allocator, "MATCH (n:Document) RETURN n.id,count(*),min(n.rank),max(n.rank) ORDER BY n.id SKIP 49900 LIMIT 5", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .scanned_anchors = 50000, .intermediate_states = 4000000, .memory_bytes = 256 << 10 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 5), result.rows.len);
    try std.testing.expectEqualStrings("049900", result.rows[0][0].string);
}
