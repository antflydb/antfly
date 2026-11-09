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
const cypher = @import("cypher");
const overlay = @import("sql_graph_overlay.zig");
const A = std.mem.Allocator;
const definition: cypher.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{.{ .name = "R", .index = "links", .source_label = "D", .target_label = "D", .stored_type = "link" }} };
const Fixture = struct {
    expansions: usize = 0,
    bulk_gets: usize = 0,
    scalar_gets: usize = 0,
    fn bind(_: *anyopaque, _: A, _: cypher.catalog.Graph) !void {}
    fn checkpoint(_: *anyopaque) !void {}
    fn scan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Node) {
        return .{ .items = &.{} };
    }
    fn get(_: *anyopaque, _: A, _: cypher.value.Identity) !?*const cypher.value.Node {
        return null;
    }
    fn expand(ptr: *anyopaque, _: A, _: cypher.catalog.Relationship, _: cypher.value.Identity, _: cypher.ast.Direction, _: ?[]const u8, _: usize) !cypher.backend.Page(*const cypher.value.Relationship) {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.expansions += 1;
        return error.UnexpectedAdjacencyRead;
    }
    fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const request = try std.json.parseFromSliceLeaky(cypher.rpc.Request, scratch.allocator(), body, .{});
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (request.operation == .get) self.scalar_gets += 1;
        if (request.operation == .get_many) {
            self.bulk_gets += 1;
            const values = try scratch.allocator().alloc(cypher.wire.Value, request.identities.len);
            for (request.identities, values) |identity, *out| {
                const node = try scratch.allocator().create(cypher.value.Node);
                node.* = .{ .identity = identity, .labels = &.{"D"}, .properties = &.{} };
                out.* = .{ .value = if (std.mem.eql(u8, identity.key, "missing")) .null else .{ .node = node } };
            }
            return std.json.Stringify.valueAlloc(a, .{ .values = values }, .{});
        }
        if (request.operation == .project) {
            const value = try cypher.value.Value.fromJson(scratch.allocator(), request.document.?);
            const node: cypher.value.Node = .{ .identity = request.identity.?, .labels = &.{"D"}, .properties = value.map };
            return std.json.Stringify.valueAlloc(a, .{ .values = &.{cypher.wire.Value{ .value = .{ .node = &node } }} }, .{});
        }
        return (cypher.rpc.Owner{ .graph = definition, .table = "docs", .backend = .{ .ptr = ptr, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand } } }).execute(a, body);
    }
};
test "sql graph overlay node-only reads never discover staged source adjacency" {
    var fixture: Fixture = .{};
    var client: cypher.rpc.Client = .{ .incarnation = 1, .revision = 1, .channel = .{ .ptr = &fixture, .call = Fixture.call } };
    var view: overlay.View = .{ .client = &client, .limits = .{ .explored_edges = 0 }, .rows = &.{.{ .identity = .{ .table = "docs", .key = "hub" }, .document_json = "{\"score\":7}" }} };
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var compiled = try cypher.compiler.compile(std.testing.allocator, "MATCH (n:D) RETURN count(*),sum(n.score)", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try cypher.runtime.execute(std.testing.allocator, view.backend(), definition, &compiled, &.{}, view.limits);
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.rows[0][0].integer);
    try std.testing.expectEqual(@as(i64, 7), result.rows[0][1].integer);
    try std.testing.expectEqual(@as(usize, 0), fixture.expansions);
    try std.testing.expectEqual(@as(usize, 0), result.stats.explored_edges);
}

test "sql graph overlay batches untouched reads with staged deleted and duplicate identities" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    var client: cypher.rpc.Client = .{ .incarnation = 1, .revision = 1, .channel = .{ .ptr = &fixture, .call = Fixture.call } };
    var view: overlay.View = .{ .client = &client, .limits = .{}, .rows = &.{
        .{ .identity = .{ .table = "docs", .key = "staged" }, .document_json = "{\"score\":7}" },
        .{ .identity = .{ .table = "docs", .key = "deleted" }, .document_json = null },
    } };
    const backend = view.backend();
    try backend.vtable.bind(backend.ptr, a, definition);
    fixture.scalar_gets = 0;
    fixture.bulk_gets = 0;
    const nodes = try backend.vtable.get_many.?(backend.ptr, a, &.{
        .{ .table = "docs", .key = "native" },  .{ .table = "docs", .key = "staged" },
        .{ .table = "docs", .key = "deleted" }, .{ .table = "docs", .key = "missing" },
        .{ .table = "docs", .key = "native" },
    });
    try std.testing.expectEqual(@as(usize, 1), fixture.bulk_gets);
    try std.testing.expectEqual(@as(usize, 0), fixture.scalar_gets);
    try std.testing.expectEqualStrings("native", nodes[0].?.identity.key);
    try std.testing.expectEqual(@as(i64, 7), (cypher.value.Value{ .node = nodes[1].? }).property("score").integer);
    try std.testing.expect(nodes[2] == null and nodes[3] == null);
    try std.testing.expectEqualStrings("native", nodes[4].?.identity.key);
}
