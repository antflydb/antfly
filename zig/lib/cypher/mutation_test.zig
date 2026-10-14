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
    fn init() Fixture {
        return .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    }
    fn self(ptr: *anyopaque) *Fixture {
        return @ptrCast(@alignCast(ptr));
    }
    fn bind(_: *anyopaque, _: A, _: root.catalog.Graph) !void {}
    fn checkpoint(_: *anyopaque) !void {}
    fn scan(ptr: *anyopaque, a: A, _: []const u8, cursor: ?[]const u8, _: usize) !b.Page(*const v.Node) {
        if (cursor != null) return error.UnexpectedCursor;
        return .{ .items = try a.dupe(*const v.Node, self(ptr).nodes) };
    }
    fn get(ptr: *anyopaque, _: A, id: v.Identity) !?*const v.Node {
        for (self(ptr).nodes) |node| if (node.identity.eql(id)) return node;
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
        return .{ .ptr = fixture, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .begin = begin, .commit = commit, .rollback = rollback, .new_identity = identity, .mutate = mutate } };
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
        fn run(a: A, source: *Fixture) !void {
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
    try std.testing.expectError(error.CypherTypeMismatch, fixture.run("CREATE (n:Person {name:'Ada'}) SET n.age=1 RETURN 1 + 'bad'"));
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
