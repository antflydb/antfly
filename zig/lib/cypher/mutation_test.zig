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
const v = root.value;
const b = root.backend;
const A = std.mem.Allocator;
const graph: root.catalog.Graph = .{ .name = "people", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Person", .table = "people" }}, .relationships = &.{.{ .name = "KNOWS", .index = "links", .source_label = "Person", .target_label = "Person", .stored_type = "knows" }} };
const Fixture = struct {
    arena: std.heap.ArenaAllocator,
    nodes: []const *const v.Node = &.{},
    relationships: []const *const v.Relationship = &.{},
    saved_nodes: []const *const v.Node = &.{},
    saved_relationships: []const *const v.Relationship = &.{},
    next: usize = 1,
    commits: usize = 0,
    rollbacks: usize = 0,
    fail_commit: bool = false,
    active: bool = false,
    checkpoints: usize = 0,
    cancel_after: ?usize = null,
    fn init() Fixture {
        return .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    }
    fn self(ptr: *anyopaque) *Fixture {
        return @ptrCast(@alignCast(ptr));
    }
    fn bind(_: *anyopaque, _: A, _: root.catalog.Graph) !void {}
    fn checkpoint(ptr: *anyopaque) !void {
        const fixture = self(ptr);
        fixture.checkpoints += 1;
        if (fixture.cancel_after) |stop| if (fixture.checkpoints >= stop) return error.Canceled;
    }
    fn scan(ptr: *anyopaque, a: A, _: []const u8, cursor: ?[]const u8, _: usize) !b.Page(*const v.Node) {
        if (cursor != null) return error.UnexpectedCursor;
        return .{ .items = try a.dupe(*const v.Node, self(ptr).nodes) };
    }
    fn get(ptr: *anyopaque, _: A, id: v.Identity) !?*const v.Node {
        for (self(ptr).nodes) |node| if (node.identity.eql(id)) return node;
        return null;
    }
    fn getRelationship(ptr: *anyopaque, _: A, relationship_id: v.RelationshipIdentity) !?*const v.Relationship {
        for (self(ptr).relationships) |relationship| {
            if (relationship.identity.eql(relationship_id.identity) and std.mem.eql(u8, relationship.index, relationship_id.index)) return relationship;
        }
        return null;
    }
    fn expand(ptr: *anyopaque, a: A, _: root.catalog.Relationship, id: v.Identity, direction: root.ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
        var out: std.ArrayList(*const v.Relationship) = .empty;
        for (self(ptr).relationships) |r| if ((direction != .in and r.source.eql(id)) or (direction != .out and r.target.eql(id))) try out.append(a, r);
        return .{ .items = try out.toOwnedSlice(a) };
    }
    fn begin(ptr: *anyopaque) !void {
        const fixture = self(ptr);
        if (fixture.active) return error.NestedTransaction;
        fixture.saved_nodes = fixture.nodes;
        fixture.saved_relationships = fixture.relationships;
        fixture.active = true;
    }
    fn commit(ptr: *anyopaque) !void {
        const fixture = self(ptr);
        if (fixture.fail_commit) return error.CommitRejected;
        fixture.commits += 1;
        fixture.active = false;
    }
    fn rollback(ptr: *anyopaque) void {
        const fixture = self(ptr);
        fixture.nodes = fixture.saved_nodes;
        fixture.relationships = fixture.saved_relationships;
        fixture.active = false;
        fixture.rollbacks += 1;
    }
    fn identity(ptr: *anyopaque, a: A, table: []const u8, _: bool) !v.Identity {
        const fixture = self(ptr);
        defer fixture.next += 1;
        return .{ .table = table, .key = try std.fmt.allocPrint(a, "{d}", .{fixture.next}) };
    }
    fn properties(a: A, prior: []const v.Entry, updates: []const v.Entry, replace_all: bool, removed: []const []const u8) ![]const v.Entry {
        var out: std.ArrayList(v.Entry) = .empty;
        if (!replace_all) for (prior) |entry| {
            var skip = false;
            for (updates) |update| if (std.mem.eql(u8, entry.name, update.name)) {
                skip = true;
                break;
            };
            for (removed) |name| if (std.mem.eql(u8, entry.name, name)) {
                skip = true;
                break;
            };
            if (!skip) try out.append(a, entry);
        };
        for (updates) |entry| if (entry.value != .null) try out.append(a, entry);
        return out.toOwnedSlice(a);
    }
    fn patch(fixture: *Fixture, target: v.EntityIdentity, updates: []const v.Entry, replace_all: bool, removed: []const []const u8) !void {
        const a = fixture.arena.allocator();
        if (target == .node) {
            const nodes = try a.dupe(*const v.Node, fixture.nodes);
            for (nodes) |*node| if (node.*.identity.eql(target.node)) {
                const copy = try a.create(v.Node);
                copy.* = node.*.*;
                copy.properties = (try (v.Value{ .map = try properties(a, copy.properties, updates, replace_all, removed) }).clone(a)).map;
                node.* = copy;
            };
            fixture.nodes = nodes;
        } else {
            const relationships = try a.dupe(*const v.Relationship, fixture.relationships);
            for (relationships) |*relationship| if ((try v.EntityIdentity.fromValue(.{ .relationship = relationship.* })).eql(target)) {
                const copy = try a.create(v.Relationship);
                copy.* = relationship.*.*;
                copy.properties = (try (v.Value{ .map = try properties(a, copy.properties, updates, replace_all, removed) }).clone(a)).map;
                relationship.* = copy;
            };
            fixture.relationships = relationships;
        }
    }
    fn mutate(ptr: *anyopaque, _: A, mutation: b.Mutation) !void {
        const fixture = self(ptr);
        if (!fixture.active) return error.TransactionRequired;
        const a = fixture.arena.allocator();
        switch (mutation) {
            .create_node => |node| {
                const nodes = try a.alloc(*const v.Node, fixture.nodes.len + 1);
                @memcpy(nodes[0..fixture.nodes.len], fixture.nodes);
                nodes[fixture.nodes.len] = (try (v.Value{ .node = node }).clone(a)).node;
                fixture.nodes = nodes;
            },
            .create_relationship => |r| {
                const edges = try a.alloc(*const v.Relationship, fixture.relationships.len + 1);
                @memcpy(edges[0..fixture.relationships.len], fixture.relationships);
                edges[fixture.relationships.len] = (try (v.Value{ .relationship = r }).clone(a)).relationship;
                fixture.relationships = edges;
            },
            .set_properties => |change| try fixture.patch(change.target, change.properties, change.replace, &.{}),
            .remove_properties => |change| try fixture.patch(change.target, &.{}, false, change.names),
            .delete_node => |change| {
                for (fixture.relationships) |r| if (r.source.eql(change.identity) or r.target.eql(change.identity)) {
                    if (!change.detach) return error.NodeHasRelationships;
                };
                var nodes: std.ArrayList(*const v.Node) = .empty;
                for (fixture.nodes) |node| if (!node.identity.eql(change.identity)) try nodes.append(a, node);
                var relationships: std.ArrayList(*const v.Relationship) = .empty;
                for (fixture.relationships) |r| if (!r.source.eql(change.identity) and !r.target.eql(change.identity)) try relationships.append(a, r);
                fixture.nodes = try nodes.toOwnedSlice(a);
                fixture.relationships = try relationships.toOwnedSlice(a);
            },
            .delete_relationship => |target| {
                var relationships: std.ArrayList(*const v.Relationship) = .empty;
                for (fixture.relationships) |r| if (!(try v.EntityIdentity.fromValue(.{ .relationship = r })).eql(.{ .relationship = target })) try relationships.append(a, r);
                fixture.relationships = try relationships.toOwnedSlice(a);
            },
        }
    }
    fn backend(fixture: *Fixture) b.Backend {
        return .{ .ptr = fixture, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .get_relationship = getRelationship, .expand = expand, .begin = begin, .commit = commit, .rollback = rollback, .new_identity = identity, .mutate = mutate } };
    }
    fn run(fixture: *Fixture, text: []const u8) !root.runtime.Result {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        return root.runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{}, .{});
    }
};
test "portable graph materialization retains owned node and edge postimages" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seeded = try fixture.run("CREATE (a:Person {name:'Ada'})-[:KNOWS {since:2020}]->(b:Person {name:'Bob'}) RETURN a");
    seeded.deinit();
    const cut = try root.snapshot.Materialized.create(std.testing.allocator, fixture.backend(), graph, .{});
    defer cut.deinit();
    var changed = try fixture.run("MATCH (a:Person {name:'Ada'})-[r:KNOWS]->(b) SET a.name='Changed',r.since=2026 CREATE (c:Person {name:'Later'}) RETURN c");
    changed.deinit();
    var diagnostic: root.compiler.Diagnostic = .{};
    var query = try root.compiler.compile(std.testing.allocator, "MATCH (a:Person)-[r:KNOWS]->(b) RETURN a.name,r.since,b.name", .{}, &diagnostic);
    defer query.deinit();
    var result = try root.runtime.execute(std.testing.allocator, cut.backend(), graph, &query, &.{}, cut.remainingLimits(.{}));
    defer result.deinit();
    try std.testing.expectEqualStrings("Ada", result.rows[0][0].string);
    try std.testing.expectEqual(@as(i64, 2020), result.rows[0][1].integer);
    try std.testing.expectEqualStrings("Bob", result.rows[0][2].string);
    try std.testing.expectEqual(@as(usize, 2), cut.overlay.nodes.items.len);
    try std.testing.expectError(error.CypherMemoryBudgetExceeded, root.snapshot.Materialized.create(std.testing.allocator, fixture.backend(), graph, .{ .memory_bytes = 1 }));
    const Capture = struct {
        fn run(backing: A, source: *Fixture) !void {
            // Failure positions must not depend on adjacent arena allocations
            // making an opportunistic resize/remap succeed on a later replay.
            var vtable = backing.vtable.*;
            vtable.resize = std.mem.Allocator.noResize;
            vtable.remap = std.mem.Allocator.noRemap;
            const a: A = .{ .ptr = backing.ptr, .vtable = &vtable };
            const retained = try root.snapshot.Materialized.create(a, source.backend(), graph, .{});
            defer retained.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Capture.run, .{&fixture});
}

test "UNION write branches share one transaction including a read first branch" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("RETURN 'before' AS name UNION ALL CREATE (n:Person {name:'Ada'}) RETURN n.name AS name UNION ALL MATCH (n:Person) RETURN n.name AS name");
    defer result.deinit();
    try std.testing.expect(!result.read_only);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqualStrings("Ada", result.rows[2][0].string);
    try std.testing.expectError(error.CypherDivisionByZero, fixture.run("CREATE (n:Person {name:'rollback'}) RETURN n.name AS name UNION ALL RETURN 1/0 AS name"));
    try std.testing.expectEqual(@as(usize, 1), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.rollbacks);
}

test "Cypher writes update aliases and paths, remove null properties, and read staged MERGE results" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var created = try fixture.run("CREATE p=(a:Person {name:'Ada'})-[r:KNOWS {since:2020}]->(b:Person {name:'Bob'}) WITH p,a,b,r,a AS alias SET a.age=37, r.since=2021 REMOVE a.name RETURN alias.age, a.name, relationships(p)[0].since, nodes(p)[0].age");
    defer created.deinit();
    try std.testing.expectEqual(@as(i64, 37), created.rows[0][0].integer);
    try std.testing.expect(created.rows[0][1] == .null);
    try std.testing.expectEqual(@as(i64, 2021), created.rows[0][2].integer);
    try std.testing.expectEqual(@as(i64, 37), created.rows[0][3].integer);
    var merged = try fixture.run("MERGE (n:Person {name:'Bob'}) ON CREATE SET n.created=true ON MATCH SET n.seen=true RETURN n.seen");
    defer merged.deinit();
    try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
    try std.testing.expect(merged.rows[0][0].boolean);
    try std.testing.expectEqual(@as(usize, 2), fixture.commits);
}
test "Cypher write errors and commit rejection roll back every staged change" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    try std.testing.expectError(error.CypherTypeMismatch, fixture.run("CREATE (n:Person {name:'Ada'}) SET n.age=1 RETURN 1 + {}"));
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.rollbacks);
    fixture.fail_commit = true;
    try std.testing.expectError(error.CommitRejected, fixture.run("CREATE (n:Person {name:'Ada'}) RETURN n.name"));
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 2), fixture.rollbacks);
}

test "Cypher oversized escaped RETURN rolls back before durable commit" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const payload = try std.testing.allocator.alloc(u8, 6 << 20);
    defer std.testing.allocator.free(payload);
    @memset(payload, 0);
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "CREATE (n:Person {payload:$payload}) RETURN n", .{}, &diagnostic);
    defer compiled.deinit();
    try std.testing.expectError(error.CypherResponseTooLarge, root.runtime.execute(std.testing.allocator, fixture.backend(), graph, &compiled, &.{.{ .name = "payload", .value = .{ .string = payload } }}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 1), fixture.rollbacks);
}

test "unit write subqueries preserve the incoming outer row" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("UNWIND [1,2] AS x CALL { CREATE (n:Person {age:x}) } RETURN x ORDER BY x");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
}

test "standalone unit subqueries propagate writes through nested calls and UNION" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    for ([_][]const u8{
        "CALL { CREATE (:Person {age:1}) }",
        "CALL { CALL { CREATE (:Person {age:2}) } }",
        "CALL { MATCH (n:Person) SET n.age=3 }",
        "CALL { MATCH (n:Person) REMOVE n.age }",
        "CALL { MERGE (:Person {age:4}) } UNION CALL { CREATE (:Person {age:5}) }",
        "CALL { MATCH (n:Person) DELETE n }",
    }) |text| {
        var result = try fixture.run(text);
        defer result.deinit();
        try std.testing.expect(!result.read_only);
        try std.testing.expectEqual(@as(usize, 0), result.columns.len);
        try std.testing.expectEqual(@as(usize, 0), result.rows.len);
    }
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    var explained = try fixture.run("EXPLAIN CALL { CALL { CREATE (:Person) } }");
    defer explained.deinit();
    try std.testing.expect(!explained.read_only);
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectError(error.CypherQueryRequiresReturn, fixture.run("CALL { MATCH (n:Person) RETURN n }"));
    try std.testing.expectError(error.CypherExistsMutation, fixture.run("RETURN EXISTS { CALL { CREATE (:Person) } }"));
}

test "relationship-only plans never rewrite unchanged source rows" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seeded = try fixture.run("CREATE (a:Person)-[:KNOWS]->(:Person)");
    seeded.deinit();
    @constCast(fixture.relationships[0]).identity.key = "cypher:edge";
    const a = fixture.arena.allocator();
    var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit } };
    const backend = overlay.backend();
    try backend.vtable.bind(backend.ptr, a, graph);
    try backend.vtable.begin.?(backend.ptr);
    const edge = overlay.relationships.items[0].value;
    try backend.vtable.mutate.?(backend.ptr, a, .{ .set_properties = .{ .target = .{ .relationship = .{ .identity = edge.identity, .index = edge.index } }, .properties = &.{.{ .name = "since", .value = .{ .integer = 2026 } }}, .replace = false } });
    const plans = try root.mutations.build(a, graph, &overlay);
    try std.testing.expectEqual(@as(usize, 0), plans[0].writes.items.len);
    try std.testing.expectEqual(@as(usize, 1), plans[0].edges.items.len);
}

test "created relationship postimages survive unit subqueries and drive later writes" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE p=(a:Person)-[r:KNOWS {x:1}]->(b:Person) WITH p,r,[r] AS list,{edge:r} AS map CALL { WITH r SET r.x=2 } SET r.y=r.x RETURN r.x,r.y,relationships(p)[0].x,list[0].x,map.edge.x");
    defer result.deinit();
    for (result.rows[0]) |cell| try std.testing.expectEqual(@as(i64, 2), cell.integer);
    var stored = try fixture.run("MATCH ()-[r:KNOWS]->() RETURN r.x,r.y");
    defer stored.deinit();
    for (stored.rows[0]) |cell| try std.testing.expectEqual(@as(i64, 2), cell.integer);
}

test "MERGE rejects null node and relationship keys and rolls back earlier writes" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    for ([_][]const u8{
        "MERGE (n:Person {name:null}) RETURN n",
        "CREATE (a:Person) MERGE (n:Person {name:a.missing}) RETURN n",
        "MERGE (a:Person)-[r:KNOWS {since:null}]->(b:Person) RETURN r",
    }) |query| {
        try std.testing.expectError(error.CypherInvalidPropertyValue, fixture.run(query));
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
}

test "relationship CREATE omits entity null properties while preserving nested null data" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE (:Person)-[r:KNOWS {absent:null,data:{nested:null}}]->(:Person) RETURN keys(r),properties(r)");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows[0][0].list.len);
    try std.testing.expectEqualStrings("data", result.rows[0][0].list[0].string);
    try std.testing.expect(result.rows[0][1].property("data").property("nested") == .null);
    try std.testing.expectEqual(@as(usize, 1), fixture.relationships[0].properties.len);
}

test "MERGE checks relationship keys after their pattern node references are bound" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const query = "MERGE (a:Person {name:'Ada'})-[r:KNOWS {owner:a.name}]->(b:Person {name:'Bob'}) RETURN r.owner";
    for (0..2) |_| {
        var result = try fixture.run(query);
        defer result.deinit();
        try std.testing.expectEqualStrings("Ada", result.rows[0][0].string);
    }
    try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.relationships.len);
    try std.testing.expectError(error.CypherInvalidPropertyValue, fixture.run("MERGE (a:Person {name:'Other'})-[r:KNOWS {owner:a.missing}]->(b:Person {name:'New'}) RETURN r"));
    try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.relationships.len);
}

test "deleting a relationship preserves its row binding for aggregate counts" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE (:Person)-[r:KNOWS]->(:Person) DELETE r RETURN count(r)");
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
}

test "CREATE and MERGE reject redeclaration of bound node labels and properties" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    for ([_][]const u8{
        "CREATE (n:Person {name:'Ada'}) CREATE (n:Person) RETURN n",
        "CREATE (n:Person {name:'Ada'}) CREATE (n {name:'Bob'}) RETURN n",
        "CREATE (n:Person {name:'Ada'}) MERGE (n:Person {name:'Bob'}) RETURN n",
        "CREATE (n:Person {name:'Ada'}) MERGE (n {name:'Bob'})-[r:KNOWS]->(m:Person) ON CREATE SET n.name='Changed' RETURN n",
        "CREATE (n:Person)-[:KNOWS]->(n:Person) RETURN n",
        "MERGE (n:Person)-[:KNOWS]->(n {name:'Ada'}) RETURN n",
        "CREATE (n:Person) WITH n CALL { WITH n CREATE (n:Person) } RETURN n",
    }) |query| {
        try std.testing.expectError(error.CypherVariableConflict, fixture.run(query));
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
    var valid = try fixture.run("CREATE (a:Person {name:'Ada'}), (b:Person {name:'Bob'}) CREATE (a)-[:KNOWS]->(b) MERGE (a)-[:KNOWS]->(b) RETURN a.name,b.name");
    defer valid.deinit();
    try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.relationships.len);
}

test "deleted endpoints cannot be reused by CREATE or MERGE across subqueries" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seeded = try fixture.run("CREATE (a:Person {name:'Ada'}),(b:Person {name:'Bob'}) RETURN a,b");
    seeded.deinit();
    for ([_][]const u8{
        "MATCH (a:Person {name:'Ada'}),(b:Person {name:'Bob'}) DELETE b CREATE (a)-[:KNOWS]->(b) RETURN a",
        "MATCH (a:Person {name:'Ada'}),(b:Person {name:'Bob'}) DELETE a CREATE (a)-[:KNOWS]->(b) RETURN b",
        "MATCH (a:Person {name:'Ada'}),(b:Person {name:'Bob'}) DELETE b MERGE (a)-[:KNOWS]->(b) RETURN a",
        "MATCH (a:Person {name:'Ada'}),(b:Person {name:'Bob'}) CALL { WITH b DELETE b } CREATE (a)-[:KNOWS]->(b) RETURN a",
        "MATCH (a:Person {name:'Ada'}) DELETE a CREATE (a)-[:KNOWS]->(b:Person {name:'New'}) RETURN b",
    }) |query| {
        try std.testing.expectError(error.CypherDeletedEntity, fixture.run(query));
        try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
        try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    }
}

test "deleted contents reject reads through properties functions paths and containers" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    for ([_][]const u8{
        "CREATE (a:Person {x:1}) DELETE a RETURN a.x",
        "CREATE (a:Person {x:1}) DELETE a RETURN a['x']",
        "CREATE (a:Person {x:1}) DELETE a RETURN properties(a)",
        "CREATE (a:Person {x:1}) DELETE a RETURN a{.x}",
        "CREATE (a:Person {x:1}) DELETE a RETURN a{.*}",
        "CREATE (a:Person {x:1}) DELETE a RETURN keys(a)",
        "CREATE (a:Person {x:1}) DELETE a RETURN labels(a)",
        "CREATE (a:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r SET a.x=r.x RETURN a.x",
        "CREATE (:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r RETURN r['x']",
        "CREATE (:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r RETURN properties(r)",
        "CREATE (:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r RETURN keys(r)",
        "CREATE (:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r RETURN r{.*}",
        "CREATE (:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r SET r.x=1 RETURN r",
        "CREATE (:Person)-[r:KNOWS {x:42}]->(:Person) DELETE r REMOVE r.x RETURN r",
        "CREATE p=(a:Person)-[r:KNOWS {x:42}]->(:Person) DETACH DELETE a RETURN relationships(p)[0].x",
        "CREATE (a:Person {x:1}) WITH a,[a] AS list,{n:a} AS map CALL { WITH a DELETE a } RETURN list[0].x",
        "CREATE (a:Person {x:1}) WITH a,{n:a} AS map CALL { WITH a DELETE a } RETURN map.n.x",
    }) |query| {
        try std.testing.expectError(error.CypherDeletedEntity, fixture.run(query));
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
}

test "deleted bindings preserve identity and counts but serialize no cached contents" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE p=(a:Person {x:1})-[r:KNOWS {x:2}]->(:Person) DETACH DELETE a RETURN count(a),count(r),elementId(a),elementId(r),a,r,relationships(p)[0],nodes(p)[0],[a,r],{node:a,edge:r}");
    defer result.deinit();
    const row = result.rows[0];
    try std.testing.expectEqual(@as(i64, 1), row[0].integer);
    try std.testing.expectEqual(@as(i64, 1), row[1].integer);
    try std.testing.expect(row[2] == .string and row[3] == .string);
    try std.testing.expectEqual(@as(usize, 0), row[4].node.properties.len);
    try std.testing.expectEqual(@as(usize, 0), row[4].node.labels.len);
    try std.testing.expectEqual(@as(usize, 0), row[5].relationship.properties.len);
    try std.testing.expectEqual(@as(usize, 0), row[6].relationship.properties.len);
    try std.testing.expectEqual(@as(usize, 0), row[7].node.properties.len);
    try std.testing.expectEqual(@as(usize, 0), row[8].list[0].node.properties.len);
    try std.testing.expectEqual(@as(usize, 0), row[9].map[1].value.relationship.properties.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
}

test "overlay admission and durable plans independently reject deleted endpoints" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
        fn existed(_: *anyopaque, _: []const u8, _: []const u8) bool {
            return true;
        }
    };
    for ([_]usize{ 0, 1 }) |deleted_index| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var seeded = try fixture.run("CREATE (a:Person),(b:Person) RETURN a,b");
        seeded.deinit();
        const a = fixture.arena.allocator();
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit } };
        const backend = overlay.backend();
        try backend.vtable.bind(backend.ptr, a, graph);
        try backend.vtable.begin.?(backend.ptr);
        defer backend.vtable.rollback.?(backend.ptr);
        const source = overlay.nodes.items[0].value.identity;
        const target = overlay.nodes.items[1].value.identity;
        try backend.vtable.mutate.?(backend.ptr, a, .{ .delete_node = .{ .identity = overlay.nodes.items[deleted_index].value.identity, .detach = false } });
        const relationship = try a.create(v.Relationship);
        relationship.* = .{ .identity = .{ .table = "people", .key = "cypher:test" }, .index = "links", .type = "KNOWS", .source = source, .target = target };
        try std.testing.expectError(error.CypherDeletedEntity, backend.vtable.mutate.?(backend.ptr, a, .{ .create_relationship = relationship }));
        // Simulate a malformed adapter that bypasses overlay admission. The
        // durable postimage planner must still reject the orphan edge.
        try overlay.relationships.append(a, .{ .value = relationship, .dirty = true, .created = true });
        try overlay.mutations.append(a, .{ .create_relationship = relationship });
        try std.testing.expectError(error.CypherDeletedEntity, root.mutations.build(a, graph, &overlay));
    }
}

test "properties captured before deletion remain ordinary data" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE (a:Person {x:1})-[r:KNOWS {x:42}]->(:Person) WITH a,r,properties(r) AS before DELETE r SET a.x=before.x RETURN a.x,count(r)");
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 42), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][1].integer);
    try std.testing.expectEqual(@as(usize, 2), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
}

test "MATCH does not resurrect a deleted bound node" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE (n:Person {x:1}) DELETE n WITH n MATCH (n) RETURN count(n)");
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 0), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
}

test "reading missing postimages does not delete restored relationship identities" {
    const Adapter = struct {
        fn getRelationship(ptr: *anyopaque, a: A, identity: v.RelationshipIdentity) !?*const v.Relationship {
            const fixture = Fixture.self(ptr);
            const visible = (v.Value{ .node = fixture.nodes[0] }).property("visible");
            if (visible == .boolean and !visible.boolean) return null;
            return Fixture.getRelationship(ptr, a, identity);
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var vtable = fixture.backend().vtable.*;
    vtable.get_relationship = Adapter.getRelationship;
    const backend: b.Backend = .{ .ptr = &fixture, .vtable = &vtable };
    for ([_][]const u8{
        "CREATE (a:Person {visible:true})-[r:KNOWS {x:42}]->(b:Person) SET a.visible=false WITH a,b,r,count(r) AS seen SET a.visible=true WITH a,b,r MATCH (a)-[s:KNOWS]->(b) RETURN r.x,s.x",
        "CREATE p=(a:Person {visible:true})-[r:KNOWS {x:42}]->(b:Person) WITH a,b,p,[r] AS list,{r:r} AS map SET a.visible=false WITH a,b,p,list,map,relationships(p)[0] AS missing SET a.visible=true RETURN missing.x,list[0].x,map.r.x,relationships(p)[0].x",
    }) |query| {
        // Each scenario uses a separate fixture so the adapter observes its
        // source node, independently of graph cardinality from earlier runs.
        fixture.nodes = &.{};
        fixture.relationships = &.{};
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{});
        defer result.deinit();
        for (result.rows[0]) |cell| try std.testing.expectEqual(@as(i64, 42), cell.integer);
    }
    for ([_][]const u8{ "r.x", "properties(r)", "r{.*}" }) |expression| {
        fixture.nodes = &.{};
        fixture.relationships = &.{};
        const query = try std.fmt.allocPrint(std.testing.allocator, "CREATE (a:Person {{visible:true}})-[r:KNOWS {{x:42}}]->(:Person) SET a.visible=false RETURN {s}", .{expression});
        defer std.testing.allocator.free(query);
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(error.CypherDeletedEntity, root.runtime.execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    }
}

test "invalid property access and decimal overflow abort all writes" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    try std.testing.expectError(error.CypherTypeMismatch, fixture.run("CREATE (:Person {name:'rollback'}) WITH 42 AS x RETURN x.foo"));
    try std.testing.expectError(error.CypherNumericOverflow, fixture.run("CREATE (:Person {name:'rollback'}) RETURN toInteger('-9223372036854775809')"));
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 2), fixture.rollbacks);
}

test "entity property validation precedes mutations and rejects nested handles" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    for ([_][]const u8{
        "CREATE (a:Person) CREATE (b:Person {payload:a}) RETURN b",
        "CREATE (a:Person) SET a.payload=a RETURN a",
        "CREATE (a:Person) SET a.payload={nested:[a]} RETURN a",
        "CREATE p=(a:Person)-[r:KNOWS]->(b:Person) SET a.payload=p RETURN a",
        "CREATE (a:Person)-[r:KNOWS]->(b:Person) SET r.payload=r RETURN r",
    }) |text| {
        try std.testing.expectError(error.CypherInvalidPropertyValue, fixture.run(text));
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.relationships.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    }
}

test "relationship and path creation aliases reject prebound declarations on empty inputs" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    for ([_][]const u8{
        "MATCH (a:Person)-[r:KNOWS]->(b) CREATE (a)-[r:KNOWS]->(b) RETURN r",
        "EXPLAIN MATCH (a:Person)-[r:KNOWS]->(b) MERGE (a)-[r:KNOWS]->(b) RETURN r",
        "CREATE (a:Person)-[r:KNOWS]->(b:Person)-[r:KNOWS]->(c:Person) RETURN r",
        "MATCH p=(a:Person)-[:KNOWS]->(b) CREATE p=(a)-[:KNOWS]->(b) RETURN p",
    }) |text| {
        try std.testing.expectError(error.CypherVariableConflict, fixture.run(text));
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.rollbacks);
    }
}

test "self-loop traversal validates the endpoint label selected by orientation" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seed = try fixture.run("CREATE (a:Person {s:false,t:true})-[:KNOWS]->(a), (b:Person {s:true,t:true})-[:KNOWS]->(b), (c:Person {s:true,t:false})-[:KNOWS]->(c)");
    seed.deinit();
    const filtered: root.catalog.Graph = .{ .name = "filtered", .incarnation = 1, .revision = 1, .labels = &.{
        .{ .name = "Person", .table = "people" },
        .{ .name = "Source", .table = "people", .predicate = &.{.{ .name = "s", .value = .{ .boolean = true } }} },
        .{ .name = "Target", .table = "people", .predicate = &.{.{ .name = "t", .value = .{ .boolean = true } }} },
    }, .relationships = &.{.{ .name = "KNOWS", .index = "links", .source_label = "Source", .target_label = "Target", .stored_type = "knows" }} };
    for ([_][]const u8{ "MATCH (n:Target)<-[r:KNOWS]-(m) RETURN count(r)", "MATCH (n)-[r:KNOWS]-(m) RETURN count(r)", "MATCH (n:Source)-[r:KNOWS]->(m) RETURN count(r)" }) |text| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(std.testing.allocator, fixture.backend(), filtered, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
    }
}

test "producer deletion withdraws only its nonincident managed assertions" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    for ([_]@FieldType(v.Relationship, "ownership"){ .document, .artifact }) |ownership| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var seeded = try fixture.run("CREATE (a:Person {name:'a'}),(b:Person {name:'b'}),(c:Person {name:'claim'}),(d:Person {name:'other'})");
        seeded.deinit();
        const a = fixture.arena.allocator();
        const edges = try a.alloc(*const v.Relationship, 2);
        for (edges, 0..) |*edge, index| {
            const relationship = try a.create(v.Relationship);
            relationship.* = .{ .identity = .{ .table = "people", .key = try std.json.Stringify.valueAlloc(a, .{ fixture.nodes[0].identity.key, fixture.nodes[1].identity.key, "knows", "", fixture.nodes[index + 2].identity.key }, .{}) }, .index = "links", .type = "KNOWS", .source = fixture.nodes[0].identity, .target = fixture.nodes[1].identity, .ownership = ownership };
            edge.* = relationship;
        }
        fixture.relationships = edges;
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit } };
        var diagnostic: root.compiler.Diagnostic = .{};
        var query = try root.compiler.compile(std.testing.allocator, "MATCH (n:Person {name:'claim'}) DELETE n WITH 1 AS ignored MATCH ()-[r:KNOWS]->() RETURN count(r)", .{}, &diagnostic);
        defer query.deinit();
        var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &query, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
        try std.testing.expect(overlay.relationships.items[0].deleted);
        try std.testing.expect(!overlay.relationships.items[1].deleted);
    }
}

test "producer postimages preserve facts and artifacts while refreshing fields and authority" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
        fn relationships(_: *anyopaque, _: A, _: *const v.Node) ![]const *const v.Relationship {
            return &.{};
        }
        fn project(_: *anyopaque, _: A, node: *const v.Node) !?*const v.Node {
            return if ((v.Value{ .node = node }).property("hidden").eql(.{ .boolean = true })) null else node;
        }
    };
    for ([_]@FieldType(v.Relationship, "ownership"){ .document, .artifact, .field }) |ownership| {
        for ([_]bool{ false, true }) |hidden| {
            var fixture = Fixture.init();
            defer fixture.arena.deinit();
            var seeded = try fixture.run("CREATE (a:Person {name:'a'}),(b:Person {name:'b'}),(c:Person {name:'claim'})");
            seeded.deinit();
            var relationship: v.Relationship = .{ .identity = .{ .table = "people", .key = "opaque-fact" }, .index = "links", .type = "KNOWS", .source = fixture.nodes[0].identity, .target = fixture.nodes[1].identity, .ownership = ownership, .producer = fixture.nodes[2].identity };
            fixture.relationships = &.{&relationship};
            var callbacks = fixture.backend().vtable.*;
            callbacks.document_relationships = Host.relationships;
            callbacks.project_postimage = Host.project;
            var overlay: root.transaction.Overlay = .{ .base = .{ .ptr = &fixture, .vtable = &callbacks }, .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit } };
            var diagnostic: root.compiler.Diagnostic = .{};
            var query = try root.compiler.compile(std.testing.allocator, if (hidden) "MATCH (n:Person {name:'claim'}) SET n.hidden=true WITH 1 AS ignored MATCH ()-[r:KNOWS]->() RETURN count(r)" else "MATCH (n:Person {name:'claim'}) SET n.note='updated' WITH 1 AS ignored MATCH ()-[r:KNOWS]->() RETURN count(r)", .{}, &diagnostic);
            defer query.deinit();
            var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &query, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, if (hidden or ownership == .field) 0 else 1), result.rows[0][0].integer);
        }
    }
}

test "materialized producer lookup retains nonvertices under the capture fence" {
    const Captured = struct {
        fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            if (after != null) return error.UnexpectedCursor;
            return .{ .items = try a.dupe(*const v.Node, Fixture.self(ptr).nodes[0..2]) };
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seeded = try fixture.run("CREATE (a:Person {name:'a',kind:'vertex'}),(b:Person {name:'b',kind:'vertex'}),(c:Person {name:'claim',kind:'producer'})");
    seeded.deinit();
    const filtered: root.catalog.Graph = .{ .name = "vertices", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Person", .table = "people", .predicate = &.{.{ .name = "kind", .value = .{ .string = "vertex" } }} }}, .relationships = graph.relationships };
    var relationship: v.Relationship = .{ .identity = .{ .table = "people", .key = "opaque-fact" }, .index = "links", .type = "KNOWS", .source = fixture.nodes[0].identity, .target = fixture.nodes[1].identity, .ownership = .document, .producer = fixture.nodes[2].identity };
    fixture.relationships = &.{&relationship};
    var callbacks = fixture.backend().vtable.*;
    callbacks.scan = Captured.scan;
    const snapshot = try root.snapshot.Materialized.create(std.testing.allocator, .{ .ptr = &fixture, .vtable = &callbacks }, filtered, .{});
    defer snapshot.deinit();
    const producer = fixture.nodes[2].identity;
    try fixture.patch(.{ .node = producer }, &.{.{ .name = "name", .value = .{ .string = "newer" } }}, false, &.{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const backend = snapshot.backend();
    const retained = (try backend.vtable.get(backend.ptr, arena.allocator(), producer)).?;
    try std.testing.expectEqualStrings("claim", (v.Value{ .node = retained }).property("name").string);
    try std.testing.expectEqual(@as(usize, 3), snapshot.overlay.nodes.items.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.overlay.relationships.items.len);
}

test "missing producers suppress every assertion with one bounded lookup" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seeded = try fixture.run("CREATE (a:Person {name:'a'}),(b:Person {name:'b'})");
    seeded.deinit();
    const a = fixture.arena.allocator();
    const edges = try a.alloc(*const v.Relationship, 2);
    for (edges, 0..) |*edge, index| {
        const relationship = try a.create(v.Relationship);
        relationship.* = .{ .identity = .{ .table = "people", .key = if (index == 0) "first" else "second" }, .index = "links", .type = "KNOWS", .source = fixture.nodes[0].identity, .target = fixture.nodes[1].identity, .ownership = .document, .producer = .{ .table = "people", .key = "missing" } };
        edge.* = relationship;
    }
    fixture.relationships = edges;
    const snapshot = try root.snapshot.Materialized.create(std.testing.allocator, fixture.backend(), graph, .{});
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 3), snapshot.overlay.captured.scanned_anchors);
    for (snapshot.overlay.relationships.items) |relationship| try std.testing.expect(relationship.deleted);
}

test "computed depth and expanded mutation work fail before commit and roll back writes" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    try std.testing.expectError(error.CypherValueDepthExceeded, fixture.run("CREATE (n:Person) WITH n, reduce(a=[],x IN range(1,10000)|[a]) AS a SET n.nested=a RETURN n"));
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 1), fixture.rollbacks);
    // Mutation property validation must reserve work before walking a DAG.
    try std.testing.expectError(error.CypherStateBudgetExceeded, fixture.run("CREATE (n:Person) WITH n, reduce(a=[1],x IN range(1,40)|[a,a]) AS a SET n.x=a RETURN n"));
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 2), fixture.rollbacks);
}

test "refreshing entity aliases preserves shared entity-free data without expanding it" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE (n:Person) WITH n, reduce(a=[1],x IN range(1,40)|[a,a]) AS a SET n.x=1 RETURN size(a)");
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 2), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
}

test "property writes include entity envelopes in the wire depth before committing" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    try std.testing.expectError(error.CypherValueDepthExceeded, fixture.run("CREATE (n:Person) WITH n,reduce(a=[1],x IN range(1,62)|[a]) AS a SET n.payload=a"));
    try std.testing.expectEqual(@as(usize, 0), fixture.commits);
    try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    var result = try fixture.run("CREATE (n:Person) WITH n,reduce(a=[1],x IN range(1,61)|[a]) AS a SET n.payload=a RETURN n");
    defer result.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, root.wire.Value{ .value = result.rows[0][0] }, .{});
    const decoded = try root.wire.transport(a, try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{}));
    try std.testing.expect(v.Value.eql(.{ .map = result.rows[0][0].node.properties }, .{ .map = decoded.node.properties }));
}

test "bulk write planning is linear, charged before commit, and owns commit-time arena growth" {
    const Host = struct {
        fixture: *Fixture,
        overlay: *root.transaction.Overlay = undefined,
        entered: bool = false,
        committed: bool = false,
        cancel: bool = false,
        fn identity(ptr: *anyopaque, a: A, table: []const u8, relationship: bool) !v.Identity {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return Fixture.identity(self.fixture, a, table, relationship);
        }
        fn commit(ptr: *anyopaque, a: A, _: []const b.Mutation) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered = true;
            if (self.cancel) self.fixture.cancel_after = self.fixture.checkpoints + 4;
            const plans = try root.mutations.build(a, graph, self.overlay);
            try std.testing.expectEqual(@as(usize, 1), plans.len);
            try std.testing.expectEqual(self.overlay.nodes.items.len, plans[0].writes.items.len);
            self.committed = true;
        }
    };
    var previous_work: usize = 0;
    for ([_]usize{ 1000, 2000, 4000 }) |count| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var host: Host = .{ .fixture = &fixture };
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &host, .new_identity = Host.identity, .commit = Host.commit } };
        host.overlay = &overlay;
        const query = try std.fmt.allocPrint(fixture.arena.allocator(), "UNWIND range(1,{d}) AS x CREATE (n:Person {{id:x}}) RETURN count(*)", .{count});
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expect(host.committed);
        try std.testing.expect(overlay.work == null);
        try std.testing.expectEqual(@as(i64, @intCast(count)), result.rows[0][0].integer);
        try std.testing.expect(result.stats.intermediate_states < 50 * count);
        if (previous_work != 0) try std.testing.expect(result.stats.intermediate_states <= 2 * previous_work);
        previous_work = result.stats.intermediate_states;
    }
    for ([_]bool{ false, true }) |cancel| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var host: Host = .{ .fixture = &fixture, .cancel = cancel };
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &host, .new_identity = Host.identity, .commit = Host.commit } };
        host.overlay = &overlay;
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, "UNWIND range(1,1000) AS x CREATE (n:Person {id:x}) RETURN count(*)", .{}, &diagnostic);
        defer compiled.deinit();
        try std.testing.expectError(if (cancel) error.Canceled else error.CypherStateBudgetExceeded, root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{ .intermediate_states = if (cancel) 1_000_000 else 20_000 }));
        try std.testing.expect(host.entered);
        try std.testing.expect(!host.committed);
        try std.testing.expect(!overlay.active);
        try std.testing.expect(overlay.work == null);
        try std.testing.expectEqual(@as(usize, 0), fixture.nodes.len);
    }
}

test "bulk deletion visits incident references once and plans deletions without preimage rescans" {
    const Counter = struct {
        units: usize = 0,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.units += amount;
        }
        fn value(ptr: *anyopaque, _: v.Value) !void {
            try charge(ptr, 1);
        }
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    for ([_]usize{ 1000, 2000, 4000 }) |count| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        const a = fixture.arena.allocator();
        var counter: Counter = .{};
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Counter.commit } };
        const backend = overlay.backend();
        try backend.vtable.bind(backend.ptr, a, graph);
        overlay.active = true;
        overlay.work = .{ .ptr = &counter, .charge = Counter.charge, .value = Counter.value };
        for (0..count) |index| {
            const node = try a.create(v.Node);
            node.* = .{ .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "doc:{d}", .{index}) } };
            try overlay.appendNode(a, .{ .value = node });
        }
        for (0..count) |index| {
            const relationship = try a.create(v.Relationship);
            relationship.* = .{ .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "cypher:{d}", .{index}) }, .index = "links", .type = "KNOWS", .source = overlay.nodes.items[index].value.identity, .target = overlay.nodes.items[(index + 1) % count].value.identity };
            try overlay.appendRelationship(a, .{ .value = relationship });
        }
        for (overlay.nodes.items) |node| try backend.vtable.mutate.?(backend.ptr, a, .{ .delete_node = .{ .identity = node.value.identity, .detach = true } });
        const plans = try root.mutations.build(a, graph, &overlay);
        try std.testing.expectEqual(count, plans[0].deletes.items.len);
        try std.testing.expectEqual(count, plans[0].removed_edges.items.len);
        try std.testing.expectEqual(@as(usize, 0), plans[0].writes.items.len);
        try std.testing.expect(counter.units <= 8 * count);
    }
}

test "reindexed relationship references suppress old endpoints and deduplicate self loops" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit } };
    const backend = overlay.backend();
    try backend.vtable.bind(backend.ptr, a, graph);
    overlay.active = true;
    const first: v.Identity = .{ .table = "people", .key = "first" };
    const second: v.Identity = .{ .table = "people", .key = "second" };
    const relationship = try a.create(v.Relationship);
    relationship.* = .{ .identity = .{ .table = "people", .key = "cypher:one" }, .index = "links", .type = "KNOWS", .source = first, .target = first };
    try overlay.appendRelationship(a, .{ .value = relationship });
    var page = try backend.vtable.expand(backend.ptr, a, graph.relationships[0], first, .both, null, 256);
    try std.testing.expectEqual(@as(usize, 1), page.items.len);
    relationship.source = second;
    relationship.target = second;
    try overlay.indexRelationship(a, 0);
    try overlay.indexRelationship(a, 0);
    page = try backend.vtable.expand(backend.ptr, a, graph.relationships[0], first, .both, null, 256);
    try std.testing.expectEqual(@as(usize, 0), page.items.len);
    page = try backend.vtable.expand(backend.ptr, a, graph.relationships[0], second, .both, null, 256);
    try std.testing.expectEqual(@as(usize, 1), page.items.len);
    try std.testing.expectError(error.CypherInvalidBackendCursor, backend.vtable.expand(backend.ptr, a, graph.relationships[0], second, .both, "1000", 256));
}

test "retained snapshot paging survives transient page allocators and reports its capture work" {
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var seeded = try fixture.run("CREATE (a:Person {name:'a'}),(b:Person {name:'b'}) RETURN count(*)");
    seeded.deinit();
    const snapshot = try root.snapshot.Materialized.create(std.testing.allocator, fixture.backend(), graph, .{});
    defer snapshot.deinit();
    const backend = snapshot.backend();
    const capture_work = backend.vtable.startup_work.?(backend.ptr);
    var after: ?[]const u8 = null;
    {
        var page_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer page_arena.deinit();
        const page = try backend.vtable.scan(backend.ptr, page_arena.allocator(), "people", null, 1);
        try std.testing.expectEqual(@as(usize, 1), page.items.len);
        after = try fixture.arena.allocator().dupe(u8, page.next.?);
    }
    const page = try backend.vtable.scan(backend.ptr, fixture.arena.allocator(), "people", after, 1);
    try std.testing.expectEqual(@as(usize, 1), page.items.len);
    try std.testing.expect(page.next == null);
    const work = backend.vtable.startup_work.?(backend.ptr);
    try std.testing.expect(work.intermediate_states > 0);
    try std.testing.expectEqual(capture_work, work);
}

test "large retained delete keeps linear memory within the statement ceiling" {
    const Host = struct {
        overlay: *root.transaction.Overlay = undefined,
        fn commit(ptr: *anyopaque, a: A, _: []const b.Mutation) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const plans = try root.mutations.build(a, graph, self.overlay);
            try std.testing.expectEqual(@as(usize, 8000), plans[0].deletes.items.len);
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    const nodes = try a.alloc(*const v.Node, 8000);
    for (nodes, 0..) |*out, index| {
        const node = try a.create(v.Node);
        const properties = try a.alloc(v.Entry, 1);
        properties[0] = .{ .name = "id", .value = .{ .string = try std.fmt.allocPrint(a, "{d}", .{index}) } };
        node.* = .{ .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "cypher:0123456789abcdef0123456789abcdef:{d}", .{index}) }, .properties = properties };
        out.* = node;
    }
    fixture.nodes = nodes;
    const snapshot = try root.snapshot.Materialized.create(std.testing.allocator, fixture.backend(), graph, .{});
    defer snapshot.deinit();
    var host: Host = .{};
    var overlay: root.transaction.Overlay = .{ .base = snapshot.backend(), .host = .{ .ptr = &host, .new_identity = Fixture.identity, .commit = Host.commit } };
    host.overlay = &overlay;
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (n:Person) DELETE n RETURN count(n)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, snapshot.remainingLimits(.{}));
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 8000), result.rows[0][0].integer);
    try std.testing.expect(result.stats.memory_bytes + snapshot.budget.live <= 64 << 20);
}

test "large normalized alias updates fit the shared capture and execution memory ceiling" {
    const Host = struct {
        overlay: *root.transaction.Overlay = undefined,
        phase: usize = 0,
        fn normalize(_: *anyopaque, a: A, node: *const v.Node, _: bool) !*const v.Node {
            const properties = (try (v.Value{ .map = node.properties }).clone(a)).map;
            const output = try a.create(v.Node);
            output.* = .{ .identity = node.identity, .properties = try v.entityProperties(a, properties) };
            return output;
        }
        fn relationships(_: *anyopaque, _: A, _: *const v.Node) ![]const *const v.Relationship {
            return &.{};
        }
        fn commit(ptr: *anyopaque, a: A, _: []const b.Mutation) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.phase = 1;
            const plans = try root.mutations.build(a, graph, self.overlay);
            try std.testing.expectEqual(@as(usize, 8000), plans[0].writes.items.len);
            self.phase = 2;
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    const nodes = try a.alloc(*const v.Node, 8000);
    for (nodes, 0..) |*out, index| {
        const node = try a.create(v.Node);
        node.* = .{
            .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "cypher:{x:0>32}", .{index}) },
            .properties = try a.dupe(v.Entry, &.{ .{ .name = "id", .value = .{ .string = try std.fmt.allocPrint(a, "{d}", .{index}) } }, .{ .name = "age", .value = .{ .integer = 1 } } }),
        };
        out.* = node;
    }
    fixture.nodes = nodes;
    var vertices = graph;
    vertices.relationships = &.{};
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    const backing = fixed.allocator();
    for ([_][]const u8{
        "MATCH (n:Person) WITH n,[n,n] AS aliases SET n.age=1 RETURN sum(aliases[0].age)",
        "MATCH (n:Person) WITH n,[n,n] AS aliases REMOVE n.age RETURN count(aliases[0].age)",
    }, 0..) |query, mode| {
        const snapshot = try root.snapshot.Materialized.create(backing, fixture.backend(), vertices, .{});
        defer snapshot.deinit();
        var host: Host = .{};
        snapshot.postimages = .{ .ptr = &host, .normalize = Host.normalize, .relationships = Host.relationships };
        var overlay: root.transaction.Overlay = .{ .base = snapshot.backend(), .host = .{ .ptr = &host, .new_identity = Fixture.identity, .commit = Host.commit } };
        host.overlay = &overlay;
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = root.runtime.execute(backing, overlay.backend(), vertices, &compiled, &.{}, snapshot.remainingLimits(.{})) catch |err| {
            std.debug.print("normalized update mode={} commit phase={}\n", .{ mode, host.phase });
            return err;
        };
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, if (mode == 0) 8000 else 0), result.rows[0][0].integer);
        try std.testing.expect(result.stats.memory_bytes + snapshot.budget.live <= 64 << 20);
    }
}

test "retained capture reclaims decode pages and owns values and cursors" {
    const Pages = struct {
        fn scan(_: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            const position = if (after) |cursor| try std.fmt.parseInt(usize, cursor, 10) else 0;
            // A provider may decode much more than the authorized projection.
            @memset(try a.alloc(u8, 2 << 20), 0xa5);
            const node = try a.create(v.Node);
            node.* = .{ .identity = .{ .table = try a.dupe(u8, "people"), .key = try std.fmt.allocPrint(a, "{d}", .{position}) }, .labels = try a.dupe([]const u8, &.{"Person"}), .properties = try a.dupe(v.Entry, &.{.{ .name = try a.dupe(u8, "name"), .value = .{ .string = try a.dupe(u8, "retained") } }}) };
            return .{ .items = try a.dupe(*const v.Node, &.{node}), .next = if (position < 9) try std.fmt.allocPrint(a, "{d}", .{position + 1}) else null };
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var vtable = fixture.backend().vtable.*;
    vtable.scan = Pages.scan;
    var vertices = graph;
    vertices.relationships = &.{};
    const snapshot = try root.snapshot.Materialized.create(std.testing.allocator, .{ .ptr = &fixture, .vtable = &vtable }, vertices, .{ .memory_bytes = 4 << 20 });
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 10), snapshot.overlay.nodes.items.len);
    for (snapshot.overlay.nodes.items, 0..) |node, index| {
        try std.testing.expectEqual(index, try std.fmt.parseInt(usize, node.value.identity.key, 10));
        try std.testing.expectEqualStrings("retained", (v.Value{ .node = node.value }).property("name").string);
    }
    try std.testing.expect(snapshot.budget.live < 1 << 20);
}

test "anchor deduplication shares identities across labels in exhaustive and goal scans" {
    for ([_]usize{ 1000, 2000 }) |count| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        const a = fixture.arena.allocator();
        const nodes = try a.alloc(*const v.Node, count);
        for (nodes, 0..) |*out, index| {
            const node = try a.create(v.Node);
            node.* = .{ .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "{d}", .{index}) }, .properties = try a.dupe(v.Entry, &.{.{ .name = "id", .value = .{ .integer = @intCast(index) } }}) };
            out.* = node;
        }
        fixture.nodes = nodes;
        var aliases = graph;
        aliases.labels = &.{ .{ .name = "Person", .table = "people" }, .{ .name = "Other", .table = "people" } };
        for ([_][]const u8{ "MATCH (n) RETURN count(n)", "MATCH (n) RETURN n.id LIMIT 4000" }, 0..) |query, mode| {
            var diagnostic: root.compiler.Diagnostic = .{};
            var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
            defer compiled.deinit();
            var result = try root.runtime.execute(std.testing.allocator, fixture.backend(), aliases, &compiled, &.{}, .{ .intermediate_states = count * 50 });
            defer result.deinit();
            if (mode == 0) {
                try std.testing.expectEqual(@as(i64, @intCast(count)), result.rows[0][0].integer);
            } else {
                try std.testing.expectEqual(count, result.rows.len);
                for (result.rows, 0..) |row, index| try std.testing.expectEqual(@as(i64, @intCast(index)), row[0].integer);
            }
        }
    }
}

test "bulk SET and REMOVE resolve nested aliases within linear work" {
    const Host = struct {
        overlay: *root.transaction.Overlay = undefined,
        count: usize,
        fn commit(ptr: *anyopaque, a: A, _: []const b.Mutation) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const plans = try root.mutations.build(a, graph, self.overlay);
            try std.testing.expectEqual(self.count, plans[0].writes.items.len);
        }
    };
    for ([_]usize{ 1000, 2000 }) |count| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        const a = fixture.arena.allocator();
        const nodes = try a.alloc(*const v.Node, count);
        for (nodes, 0..) |*out, index| {
            const node = try a.create(v.Node);
            node.* = .{ .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "{d}", .{index}) }, .properties = try a.dupe(v.Entry, &.{.{ .name = "age", .value = .{ .integer = 0 } }}) };
            out.* = node;
        }
        fixture.nodes = nodes;
        for ([_][]const u8{
            "MATCH (n:Person) WITH n,[n,n] AS aliases SET n.age=1 RETURN sum(aliases[0].age)",
            "MATCH (n:Person) WITH n,[n,n] AS aliases REMOVE n.age RETURN count(aliases[0].age)",
        }, 0..) |query, mode| {
            var host: Host = .{ .count = count };
            var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &host, .new_identity = Fixture.identity, .commit = Host.commit } };
            host.overlay = &overlay;
            var diagnostic: root.compiler.Diagnostic = .{};
            var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
            defer compiled.deinit();
            var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{ .intermediate_states = count * 200 });
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, @intCast(if (mode == 0) count else 0)), result.rows[0][0].integer);
        }
    }
}

test "shared retained entities resolve postimages with linear mutation work" {
    var previous: usize = 0;
    for ([_]usize{ 400, 800, 1600 }) |count| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var seed = try fixture.run("CREATE (:Person {age:0})");
        seed.deinit();
        const query = try std.fmt.allocPrint(fixture.arena.allocator(), "MATCH (n:Person) WITH n,[{{node:n}}] AS aliases,properties(n) AS snapshot UNWIND range(1,{d}) AS i SET n.age=n.age+1 RETURN count(*),sum(aliases[0].node.age),sum(snapshot.age)", .{count});
        var result = try fixture.run(query);
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(count)), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, @intCast(count * count)), result.rows[0][1].integer);
        try std.testing.expectEqual(@as(i64, 0), result.rows[0][2].integer);
        if (previous != 0) try std.testing.expect(result.stats.intermediate_states < previous * 3);
        previous = result.stats.intermediate_states;
    }
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE p=(n:Person {age:0})-[r:KNOWS {age:0}]->(n) WITH p,n,r UNWIND range(1,800) AS i SET r.age=r.age+1,n.age=i REMOVE n.missing RETURN count(*),sum(relationships(p)[0].age),sum(nodes(p)[0].age)");
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 800), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 640000), result.rows[0][1].integer);
    try std.testing.expectEqual(@as(i64, 640000), result.rows[0][2].integer);
}

test "Cypher node properties cannot invoke native ingestion controls" {
    for ([_][]const u8{
        "CREATE (:Person {_edges:{links:{knows:[{target:'b'}]}}})",
        "CREATE (:Person {_embeddings:{dense:[1,0]}})",
        "MATCH (n:Person) SET n._edges={links:{knows:[]}}",
        "MATCH (n:Person) SET n += {_embeddings:{dense:[1,0]}}",
        "MERGE (:Person {name:'other',_edges:{}})",
        "CALL { MATCH (n:Person) SET n={_embeddings:{}} }",
    }) |query| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var seeded = try fixture.run("CREATE (:Person {name:'kept'})");
        seeded.deinit();
        try std.testing.expectError(error.CypherInvalidPropertyValue, fixture.run(query));
        var result = try fixture.run("MATCH (n:Person) RETURN n.name");
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqualStrings("kept", result.rows[0][0].string);
    }
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    var result = try fixture.run("CREATE (a:Person {payload:{_edges:{kept:true}}})-[r:KNOWS {_embeddings:{kept:true}}]->(:Person) RETURN a.payload._edges.kept,r._embeddings.kept");
    defer result.deinit();
    try std.testing.expect(result.rows[0][0].boolean and result.rows[0][1].boolean);
}

test "lazy indexed MERGE uses incremental staged candidates with linear work" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    var previous: usize = 0;
    const filtered_graph: root.catalog.Graph = .{ .name = "filtered", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Person", .table = "people", .predicate = &.{.{ .name = "kind", .value = .{ .integer = 1 } }} }}, .relationships = &.{} };
    for ([_]usize{ 200, 400, 800 }) |count| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = true };
        const text = try std.fmt.allocPrint(fixture.arena.allocator(), "UNWIND range(1,{d}) AS id MERGE (n:Person {{age:id,kind:1}}) RETURN count(*)", .{count});
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), filtered_graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, @intCast(count)), result.rows[0][0].integer);
        try std.testing.expectEqual(count, overlay.nodes.items.len);
        try std.testing.expectEqual(@as(usize, 0), result.stats.scanned_anchors);
        if (previous != 0) try std.testing.expect(result.stats.intermediate_states < previous * 3);
        previous = result.stats.intermediate_states;
    }
}

test "staged equality and membership indexes follow replacement removal deletion and numeric equality" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = true };
    const backend = overlay.backend();
    try backend.vtable.bind(backend.ptr, a, graph);
    try backend.vtable.begin.?(backend.ptr);
    defer backend.vtable.rollback.?(backend.ptr);
    const node: v.Node = .{ .identity = .{ .table = "people", .key = "p" }, .properties = &.{.{ .name = "age", .value = .{ .integer = 1 } }} };
    try backend.vtable.mutate.?(backend.ptr, a, .{ .create_node = &node });
    for ([_]u8{ 0, 1, 2, 3, 4 }) |stage| {
        if (stage == 1) try backend.vtable.mutate.?(backend.ptr, a, .{ .set_properties = .{ .target = .{ .node = node.identity }, .properties = &.{.{ .name = "age", .value = .{ .integer = 2 } }}, .replace = true } });
        if (stage == 2) try backend.vtable.mutate.?(backend.ptr, a, .{ .remove_properties = .{ .target = .{ .node = node.identity }, .names = &.{"age"} } });
        if (stage == 3) try overlay.stageReadDocument(a, node.identity, &node);
        if (stage == 4) try backend.vtable.mutate.?(backend.ptr, a, .{ .delete_node = .{ .identity = node.identity, .detach = false } });
        for ([_]b.PropertyFilter{
            .{ .property = "age", .value = .{ .number = 1.0 } },
            .{ .property = "age", .op = .in, .value = .{ .list = &.{ .{ .integer = 1 }, .{ .number = 1.0 }, .null } } },
        }) |filter| {
            var after: ?[]const u8 = null;
            var found: usize = 0;
            while (true) {
                const page = try backend.vtable.scan_where.?(backend.ptr, a, "people", &.{filter}, after, 1);
                found += page.items.len;
                after = page.next;
                if (after == null) break;
            }
            try std.testing.expectEqual(@as(usize, if (stage == 0 or stage == 3) 1 else 0), found);
        }
    }
}

test "staged index candidates use authorized projected property values" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
        fn project(_: *anyopaque, a: A, node: *const v.Node) !?*const v.Node {
            if ((v.Value{ .node = node }).property("age").integer < 0) return null;
            const projected = try a.create(v.Node);
            projected.* = node.*;
            projected.properties = &.{.{ .name = "age", .value = .{ .integer = 1 } }};
            return projected;
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    var callbacks = fixture.backend().vtable.*;
    callbacks.project_postimage = Host.project;
    var overlay: root.transaction.Overlay = .{ .base = .{ .ptr = &fixture, .vtable = &callbacks }, .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = true };
    const backend = overlay.backend();
    try backend.vtable.bind(backend.ptr, a, graph);
    try backend.vtable.begin.?(backend.ptr);
    defer backend.vtable.rollback.?(backend.ptr);
    const node: v.Node = .{ .identity = .{ .table = "people", .key = "p" }, .properties = &.{.{ .name = "age", .value = .{ .integer = 5 } }} };
    try backend.vtable.mutate.?(backend.ptr, a, .{ .create_node = &node });
    for ([_]bool{ false, true }) |hidden| {
        if (hidden) try backend.vtable.mutate.?(backend.ptr, a, .{ .set_properties = .{ .target = .{ .node = node.identity }, .properties = &.{.{ .name = "age", .value = .{ .integer = -1 } }}, .replace = false } });
        var after: ?[]const u8 = null;
        var found: usize = 0;
        while (true) {
            const page = try backend.vtable.scan_where.?(backend.ptr, a, "people", &.{.{ .property = "age", .value = .{ .integer = 1 } }}, after, 1);
            found += page.items.len;
            after = page.next;
            if (after == null) break;
        }
        try std.testing.expectEqual(@as(usize, if (hidden) 0 else 1), found);
    }
}

test "bound relationship MERGE indexes pairs and scalar properties with linear work" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    inline for ([_]bool{ false, true }) |parallel| {
        var previous: usize = 0;
        for ([_]usize{ 200, 400, 800 }) |count| {
            var fixture = Fixture.init();
            defer fixture.arena.deinit();
            var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = true };
            const text = try std.fmt.allocPrint(fixture.arena.allocator(), if (parallel) "CREATE (a:Person),(b:Person) WITH a,b UNWIND range(1,{d}) AS i MERGE (a)-[:KNOWS {{age:i}}]->(b) RETURN count(*)" else "CREATE (a:Person) WITH a UNWIND range(1,{d}) AS i MERGE (b:Person {{age:i}}) MERGE (a)-[:KNOWS]->(b) RETURN count(*)", .{count});
            var diagnostic: root.compiler.Diagnostic = .{};
            var compiled = try root.compiler.compile(std.testing.allocator, text, .{}, &diagnostic);
            defer compiled.deinit();
            var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(i64, @intCast(count)), result.rows[0][0].integer);
            try std.testing.expectEqual(count, overlay.relationships.items.len);
            try std.testing.expectEqual(@as(usize, 0), result.stats.explored_edges);
            if (previous != 0) try std.testing.expect(result.stats.intermediate_states < previous * 3);
            previous = result.stats.intermediate_states;
        }
    }
}

test "pair indexes track postimage updates deletion parallel bags incoming edges and self loops" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
    };
    for ([_]bool{ false, true }) |lazy| {
        var fixture = Fixture.init();
        defer fixture.arena.deinit();
        var overlay: root.transaction.Overlay = .{ .base = fixture.backend(), .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = lazy };
        const text = "CREATE (a:Person),(b:Person),(a)-[r:KNOWS {age:1}]->(b),(a)-[s:KNOWS {age:1}]->(b) WITH a,b,r,s MATCH (a)-[m:KNOWS {age:1}]->(b) WITH a,b,r,s,count(m) AS before SET r.age=2 DELETE s WITH a,b,before MATCH (b)<-[m:KNOWS {age:2}]-(a) WITH a,before,count(m) AS after CREATE (a)-[:KNOWS {age:2}]->(a) WITH a,before,after MATCH (a)-[loop:KNOWS]-(a) RETURN before,after,count(loop)";
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, text, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(i64, 2), result.rows[0][0].integer);
        try std.testing.expectEqual(@as(i64, 1), result.rows[0][1].integer);
        try std.testing.expectEqual(@as(i64, 1), result.rows[0][2].integer);
    }
}

test "write read phases pull bounded native pair pages without capturing parallel bags" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
        fn between(ptr: *anyopaque, a: A, _: root.catalog.Relationship, _: v.Identity, _: v.Identity, _: root.ast.Direction, _: []const b.PropertyFilter, after: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
            const all = Fixture.self(ptr).relationships;
            const offset = if (after) |encoded| try std.fmt.parseUnsigned(usize, encoded, 10) else 0;
            const end = offset + @min(limit, all.len - offset);
            if (end - offset > max_examined) return error.CypherEdgeBudgetExceeded;
            return .{ .items = try a.dupe(*const v.Relationship, all[offset..end]), .examined = end - offset, .next = if (end < all.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    const first: v.Node = .{ .identity = .{ .table = "people", .key = "a" }, .labels = &.{"Person"}, .properties = &.{.{ .name = "age", .value = .{ .integer = 1 } }} };
    const second: v.Node = .{ .identity = .{ .table = "people", .key = "b" }, .labels = &.{"Person"}, .properties = &.{.{ .name = "age", .value = .{ .integer = 2 } }} };
    fixture.nodes = &.{ &first, &second };
    const edges = try a.alloc(*const v.Relationship, 1000);
    for (edges, 0..) |*edge, index| {
        const value = try a.create(v.Relationship);
        value.* = .{ .identity = .{ .table = "people", .key = try std.fmt.allocPrint(a, "e{d}", .{index}) }, .index = "links", .type = "KNOWS", .source = first.identity, .target = second.identity };
        edge.* = value;
    }
    fixture.relationships = edges;
    var callbacks = fixture.backend().vtable.*;
    callbacks.expand_between = Host.between;
    var overlay: root.transaction.Overlay = .{ .base = .{ .ptr = &fixture, .vtable = &callbacks }, .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = true };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (a:Person {age:1}),(b:Person {age:2}) CREATE (:Person) WITH a,b MATCH (a)-[:KNOWS]->(b) RETURN a.age LIMIT 1", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(std.testing.allocator, overlay.backend(), graph, &compiled, &.{}, .{ .explored_edges = 1, .intermediate_states = 500 });
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 1), result.stats.explored_edges);
    try std.testing.expectEqual(@as(usize, 1), overlay.relationships.items.len);
}

test "native pair pages merge clean document replacements across producer retirement and restoration" {
    const Host = struct {
        fn commit(_: *anyopaque, _: A, _: []const b.Mutation) !void {}
        fn between(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, _: v.Identity, direction: root.ast.Direction, _: []const b.PropertyFilter, after: ?[]const u8, limit: usize, _: usize) !b.Page(*const v.Relationship) {
            return Fixture.expand(ptr, a, mapping, identity, direction, after, limit);
        }
        fn project(_: *anyopaque, a: A, node: *const v.Node) ![]const *const v.Relationship {
            if ((v.Value{ .node = node }).property("age").integer == 0) return &.{};
            const edge = try a.create(v.Relationship);
            edge.* = .{ .identity = .{ .table = "people", .key = "field" }, .index = "links", .type = "KNOWS", .source = node.identity, .target = .{ .table = "people", .key = "b" }, .ownership = .field, .producer = node.identity };
            return a.dupe(*const v.Relationship, &.{edge});
        }
    };
    var fixture = Fixture.init();
    defer fixture.arena.deinit();
    const a = fixture.arena.allocator();
    var producer: v.Node = .{ .identity = .{ .table = "people", .key = "a" }, .properties = &.{.{ .name = "age", .value = .{ .integer = 1 } }} };
    const target: v.Node = .{ .identity = .{ .table = "people", .key = "b" } };
    fixture.nodes = &.{ &producer, &target };
    fixture.relationships = try Host.project(&fixture, a, &producer);
    var callbacks = fixture.backend().vtable.*;
    callbacks.expand_between = Host.between;
    callbacks.document_relationships = Host.project;
    var overlay: root.transaction.Overlay = .{ .base = .{ .ptr = &fixture, .vtable = &callbacks }, .host = .{ .ptr = &fixture, .new_identity = Fixture.identity, .commit = Host.commit }, .lazy = true };
    const backend = overlay.backend();
    try backend.vtable.bind(backend.ptr, a, graph);
    try backend.vtable.begin.?(backend.ptr);
    defer backend.vtable.rollback.?(backend.ptr);
    producer.properties = &.{.{ .name = "age", .value = .{ .integer = 0 } }};
    try overlay.stageReadDocument(a, producer.identity, &producer);
    for ([_]i64{ 1, 0, 1 }) |age| {
        producer.properties = try a.dupe(v.Entry, &.{.{ .name = "age", .value = .{ .integer = age } }});
        try overlay.stageReadDocument(a, producer.identity, &producer);
        var after: ?[]const u8 = null;
        var count: usize = 0;
        while (true) {
            const page = try backend.vtable.expand_between.?(backend.ptr, a, graph.relationships[0], producer.identity, target.identity, .out, &.{}, after, 1, 10);
            count += page.items.len;
            after = page.next;
            if (after == null) break;
        }
        try std.testing.expectEqual(@as(usize, if (age == 0) 0 else 1), count);
    }
}
