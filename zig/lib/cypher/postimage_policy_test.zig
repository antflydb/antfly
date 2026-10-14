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
const A = std.mem.Allocator;
const graph: root.catalog.Graph = .{ .name = "documents", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs" }}, .relationships = &.{} };
const Fixture = struct {
    arena: std.heap.ArenaAllocator,
    overlay: root.transaction.Overlay = undefined,
    committed: []const *const v.Node = &.{},
    projections: usize = 0,
    commits: usize = 0,
    nonce: usize = 0,
    const original: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"}, .properties = &.{ .{ .name = "id", .value = .{ .string = "a" } }, .{ .name = "tenant", .value = .{ .string = "visible" } } } };
    fn cast(ptr: *anyopaque) *Fixture {
        return @ptrCast(@alignCast(ptr));
    }
    fn bind(_: *anyopaque, _: A, _: root.catalog.Graph) !void {}
    fn checkpoint(_: *anyopaque) !void {}
    fn scan(_: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !root.backend.Page(*const v.Node) {
        return .{ .items = try a.dupe(*const v.Node, if (after == null) &.{&original} else &.{}) };
    }
    fn get(_: *anyopaque, _: A, id: v.Identity) !?*const v.Node {
        return if (id.eql(original.identity)) &original else null;
    }
    fn expand(_: *anyopaque, _: A, _: root.catalog.Relationship, _: v.Identity, _: root.ast.Direction, _: ?[]const u8, _: usize) !root.backend.Page(*const v.Relationship) {
        return .{ .items = &.{} };
    }
    fn project(ptr: *anyopaque, _: A, node: *const v.Node) !?*const v.Node {
        cast(ptr).projections += 1;
        const tenant = (v.Value{ .node = node }).property("tenant");
        return if (tenant == .string and std.mem.eql(u8, tenant.string, "visible")) node else null;
    }
    fn identity(ptr: *anyopaque, a: A, table: []const u8, _: bool) !v.Identity {
        const self = cast(ptr);
        self.nonce += 1;
        return .{ .table = table, .key = try std.fmt.allocPrint(a, "created:{}", .{self.nonce}) };
    }
    fn commit(ptr: *anyopaque, _: A, _: []const root.backend.Mutation) !void {
        const self = cast(ptr);
        var records: std.ArrayList(*const v.Node) = .empty;
        for (self.overlay.nodes.items) |node| {
            if (node.deleted) continue;
            const copy = try (v.Value{ .node = node.value }).clone(self.arena.allocator());
            try records.append(self.arena.allocator(), copy.node);
        }
        self.committed = records.items;
        self.commits += 1;
    }
    fn run(self: *Fixture, a: A, statement: []const u8) !root.runtime.Result {
        const base: root.backend.Backend = .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .project_postimage = project } };
        self.overlay = .{ .base = base, .host = .{ .ptr = self, .new_identity = identity, .commit = commit } };
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(a, statement, .{}, &diagnostic);
        defer compiled.deinit();
        return root.runtime.execute(a, self.overlay.backend(), graph, &compiled, &.{}, .{});
    }
};

test "dirty postimages are projected before reads while full values remain available for commit" {
    var fixture: Fixture = .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer fixture.arena.deinit();
    var result = try fixture.run(std.testing.allocator, "CREATE (n:Document {id:'hidden',tenant:'hidden'}) RETURN n AS node,n.tenant AS tenant,[n] AS nodes,{copy:n} AS wrapped");
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), fixture.commits);
    try std.testing.expectEqual(@as(usize, 1), fixture.projections);
    const row = result.rows[0];
    try std.testing.expect(row[0] == .null and row[1] == .null);
    try std.testing.expect(row[2].list[0] == .null and row[3].map[0].value == .null);
    try std.testing.expectEqualStrings("hidden", (v.Value{ .node = fixture.committed[1] }).property("tenant").string);
}

test "projection caches refresh after another write and hidden postimages do not match" {
    var fixture: Fixture = .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer fixture.arena.deinit();
    var restored = try fixture.run(std.testing.allocator, "MATCH (n:Document) SET n.tenant='hidden',n.body=n.tenant,n.tenant='visible' RETURN n.tenant,n.body");
    defer restored.deinit();
    try std.testing.expectEqualStrings("visible", restored.rows[0][0].string);
    try std.testing.expect(restored.rows[0][1] == .null);
    try std.testing.expectEqual(@as(usize, 2), fixture.projections);
    fixture.projections = 0;
    var hidden = try fixture.run(std.testing.allocator, "MATCH (n:Document) SET n.tenant='hidden' WITH n MATCH (m:Document) RETURN m.id");
    defer hidden.deinit();
    try std.testing.expectEqual(@as(usize, 0), hidden.rows.len);
    try std.testing.expectEqual(@as(usize, 1), fixture.projections);
    try std.testing.expectEqualStrings("hidden", (v.Value{ .node = fixture.committed[0] }).property("tenant").string);
}

test "opaque newly created write bindings can be deleted without granting read visibility" {
    var fixture: Fixture = .{ .arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer fixture.arena.deinit();
    var result = try fixture.run(std.testing.allocator, "CREATE (n:Document {tenant:'hidden'}) SET n.id='written' DELETE n RETURN count(*) AS count");
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(usize, 1), fixture.committed.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.projections);
}

test "postimage projection cleans up every allocation failure before returning hidden values" {
    const Campaign = struct {
        fn run(a: A) !void {
            var fixture: Fixture = .{ .arena = std.heap.ArenaAllocator.init(a) };
            defer fixture.arena.deinit();
            var result = try fixture.run(a, "CREATE (n:Document {id:'hidden',tenant:'hidden'}) RETURN n AS node,[n] AS wrapped");
            defer result.deinit();
            try std.testing.expect(result.rows[0][0] == .null);
            try std.testing.expect(result.rows[0][1].list[0] == .null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Campaign.run, .{});
}
