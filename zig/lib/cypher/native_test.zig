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
const Cursor = struct { offset: usize };
const Edge = struct { source: []const u8, target: []const u8, edge_type: []const u8, edge_id: []const u8 = "", owner_document: []const u8 = "", metadata: []const u8 = "", weight: f64 = 1 };
const Database = struct {
    open_mode: enum { writer, query_readonly } = .query_readonly,
    name: []const u8,
    document_json: ?[]const u8 = null,
    core: Core = .{},
    pub fn hasRetainedQueryCut(db: *const @This()) bool {
        return db.open_mode == .query_readonly;
    }
    pub fn cypherGraphIndexIdentity(_: *const @This(), _: A, _: []const u8) !?root.catalog.IndexIdentity {
        return .{ .incarnation = 41, .config_hash = 99 };
    }
    const Core = struct {
        identity_namespace: struct { table_id: u64 = 1 } = .{},
        pub fn graphIndex(_: *@This(), index: []const u8) ?u8 {
            return if (std.mem.eql(u8, index, "links")) 1 else null;
        }
    };
    const documents = [_]struct { table: []const u8, key: []const u8, json: []const u8 }{
        .{ .table = "docs", .key = "a", .json = "{\"title\":\"Alpha\",\"secret\":\"hidden\"}" },
        .{ .table = "docs", .key = "b", .json = "{\"title\":\"Beta\",\"absent\":null,\"nested\":{\"value\":null}}" },
        .{ .table = "people", .key = "a", .json = "{\"name\":\"Ada\"}" },
    };
    const edges = [_]Edge{
        .{ .source = "a", .target = "a", .edge_type = "mentions", .edge_id = "mentions-1", .metadata = "{\"target_table\":\"people\",\"secret\":\"hidden\",\"weight\":999}" },
        .{ .source = "a", .target = "a", .edge_type = "mentions", .edge_id = "mentions-2", .metadata = "{\"target_table\":\"people\"}" },
        .{ .source = "a", .target = "b", .edge_type = "cites", .edge_id = "ab" },
        .{ .source = "a", .target = "a", .edge_type = "cites", .edge_id = "loop" },
    };
    const Document = struct { id: []u8, json: []u8 };
    const Scan = struct {
        documents: []Document,
        pub fn deinit(found: *@This(), a: A) void {
            for (found.documents) |doc| {
                a.free(doc.id);
                a.free(doc.json);
            }
            a.free(found.documents);
        }
    };
    pub fn scan(db: *@This(), a: A, from: []const u8, _: []const u8, options: anytype) !Scan {
        var output: std.ArrayList(Document) = .empty;
        for (documents) |document| {
            if (!std.mem.eql(u8, document.table, db.name)) continue;
            const order = std.mem.order(u8, document.key, from);
            if (order == .lt or (order == .eq and !options.inclusive_from)) continue;
            if (output.items.len >= options.limit) break;
            try output.append(a, .{ .id = try a.dupe(u8, document.key), .json = try a.dupe(u8, db.document_json orelse document.json) });
        }
        return .{ .documents = try output.toOwnedSlice(a) };
    }
    const Lookup = struct {
        json: []u8,
        pub fn deinit(found: *@This(), a: A) void {
            a.free(found.json);
        }
    };
    pub fn getDocument(db: *@This(), a: A, key: []const u8, _: anytype) !?Lookup {
        for (documents) |document| if (std.mem.eql(u8, document.table, db.name) and std.mem.eql(u8, document.key, key)) return .{ .json = try a.dupe(u8, db.document_json orelse document.json) };
        return null;
    }
    const Page = struct {
        edges: []Edge,
        next_cursor: ?Cursor = null,
        scanned_rows: usize,
        pub fn deinit(page: *@This(), a: A) void {
            a.free(page.edges);
        }
    };
    const Limits = struct { peer_key: ?[]const u8 = null, max_edges: usize, max_owned_bytes: usize, max_scanned_rows: usize };
    pub fn graphEdgesForCypherPageAt(db: *@This(), a: A, _: []const u8, key: []const u8, kinds: []const []const u8, direction: root.ast.Direction, cursor: ?Cursor, limits: Limits, _: u64) !Page {
        if (!std.mem.eql(u8, db.name, "docs")) return error.WrongPhysicalSource;
        var output: std.ArrayList(Edge) = .empty;
        var offset = if (cursor) |position| position.offset else 0;
        // Deliberately tiny physical pages exercise both direction phases and
        // continuations even when logical pages request 128 rows.
        while (offset < edges.len and output.items.len < @min(1, limits.max_edges)) : (offset += 1) {
            const edge = edges[offset];
            if (!std.mem.eql(u8, kinds[0], edge.edge_type)) continue;
            if (!std.mem.eql(u8, if (direction == .in) edge.target else edge.source, key)) continue;
            if (limits.peer_key) |peer| if (!std.mem.eql(u8, if (direction == .in) edge.source else edge.target, peer)) continue;
            try output.append(a, edge);
        }
        const examined = output.items.len;
        return .{ .edges = try output.toOwnedSlice(a), .next_cursor = if (offset < edges.len) .{ .offset = offset } else null, .scanned_rows = examined };
    }
};
const Native = root.native.Adapter(Database, void, Cursor);
const Authority = struct {
    deny_b: bool = false,
    saw_secret: bool = false,
    revocations: usize = 0,
    hide_relationships: bool = false,
    fn bind(_: *anyopaque, _: root.catalog.Graph) !void {}
    fn projectRelationship(ptr: *anyopaque, a: A, relationship: *const v.Relationship) !?*const v.Relationship {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.hide_relationships) return null;
        const output = try a.create(v.Relationship);
        output.* = relationship.*;
        var properties: std.ArrayList(v.Entry) = .empty;
        for (relationship.properties) |entry| if (!std.mem.eql(u8, entry.name, "secret")) try properties.append(a, entry);
        output.properties = try properties.toOwnedSlice(a);
        return output;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.revocations > 0) return error.PermissionDenied;
    }
    fn project(ptr: *anyopaque, a: A, node: *const v.Node) !?*const v.Node {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if ((v.Value{ .node = node }).property("secret") == .string) self.saw_secret = true;
        if (self.deny_b and std.mem.eql(u8, node.identity.key, "b")) return null;
        const filtered = try a.create(v.Node);
        filtered.* = node.*;
        var properties: std.ArrayList(v.Entry) = .empty;
        for (node.properties) |entry| if (!std.mem.eql(u8, entry.name, "secret")) try properties.append(a, entry);
        filtered.properties = try properties.toOwnedSlice(a);
        return filtered;
    }
    fn callbacks(self: *@This()) Native.Authority {
        return .{ .ptr = self, .bind = bind, .checkpoint = checkpoint, .project = project, .project_relationship = projectRelationship };
    }
};
const graph: root.catalog.Graph = .{ .name = "knowledge", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "Document", .table = "docs" }, .{ .name = "Person", .table = "people" } }, .relationships = &.{ .{ .name = "MENTIONS", .index = "links", .source_label = "Document", .target_label = "Person", .stored_type = "mentions" }, .{ .name = "CITES", .index = "links", .source_label = "Document", .target_label = "Document", .stored_type = "cites" } } };
test "native entity null properties are absent while nested user map nulls survive" {
    var docs: Database = .{ .name = "docs" };
    var authority: Authority = .{};
    var native: Native = .{ .tables = &.{.{ .name = "docs", .db = &docs }}, .authority = authority.callbacks(), .ttl_now_ns = 1 };
    const selected: root.catalog.Graph = .{ .name = "documents", .incarnation = 2, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs" }}, .relationships = &.{} };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (n:Document {title:'Beta'}) RETURN keys(n), properties(n), n.absent, n.nested", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(std.testing.allocator, native.backend(), selected, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    const row = result.rows[0];
    try std.testing.expectEqual(@as(usize, 2), row[0].list.len);
    for (row[0].list) |key| try std.testing.expect(!std.mem.eql(u8, key.string, "absent"));
    try std.testing.expectEqual(@as(usize, 2), row[1].map.len);
    try std.testing.expect(row[2] == .null);
    try std.testing.expectEqual(@as(usize, 1), row[3].map.len);
    try std.testing.expect(row[3].map[0].value == .null);
}

test "native virtual labels exclude nonmembers from anchors while retaining authorized producer lookup" {
    var docs: Database = .{ .name = "docs" };
    var authority: Authority = .{};
    var native: Native = .{ .tables = &.{.{ .name = "docs", .db = &docs }}, .authority = authority.callbacks(), .ttl_now_ns = 1 };
    const selected: root.catalog.Graph = .{ .name = "alpha", .incarnation = 2, .revision = 1, .labels = &.{.{ .name = "Alpha", .table = "docs", .predicate = &.{.{ .name = "title", .value = .{ .string = "Alpha" } }} }}, .relationships = &.{} };
    const backend = native.backend();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    try backend.vtable.bind(backend.ptr, arena.allocator(), selected);
    const producer = (try backend.vtable.get(backend.ptr, arena.allocator(), .{ .table = "docs", .key = "b" })).?;
    try std.testing.expectEqual(@as(usize, 0), producer.labels.len);
    const queries = [_][]const u8{
        "MATCH (n) RETURN n.title",
        "MATCH (n) WHERE antfly.documentKey(n)='b' RETURN n.title",
        "MATCH (n) WHERE antfly.documentKey(n)='a' RETURN n.title",
    };
    for (queries, [_]usize{ 1, 0, 1 }) |query, expected| {
        var diagnostic: root.compiler.Diagnostic = .{};
        var compiled = try root.compiler.compile(std.testing.allocator, query, .{}, &diagnostic);
        defer compiled.deinit();
        var result = try root.runtime.execute(std.testing.allocator, backend, selected, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(expected, result.rows.len);
        if (expected != 0) try std.testing.expectEqualStrings("Alpha", result.rows[0][0].string);
    }
}
test "native graph paging preserves parallel and cross-table relationships and applies authority" {
    var docs: Database = .{ .name = "docs" };
    var people: Database = .{ .name = "people" };
    var authority: Authority = .{ .deny_b = true };
    var native: Native = .{ .tables = &.{ .{ .name = "docs", .db = &docs }, .{ .name = "people", .db = &people } }, .authority = authority.callbacks(), .ttl_now_ns = 1 };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (p:Person)<-[r:MENTIONS]-(d:Document) RETURN p.name, d.title, d.secret, p=d, r.secret, r.weight", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(std.testing.allocator, native.backend(), graph, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("Ada", result.rows[0][0].string);
    try std.testing.expectEqualStrings("Alpha", result.rows[0][1].string);
    try std.testing.expect(result.rows[0][2] == .null);
    try std.testing.expect(!result.rows[0][3].boolean);
    try std.testing.expect(result.rows[0][4] == .null);
    try std.testing.expectEqual(@as(f64, 1), result.rows[0][5].number);
    authority.hide_relationships = true;
    var hidden = try root.runtime.execute(std.testing.allocator, native.backend(), graph, &compiled, &.{}, .{});
    defer hidden.deinit();
    try std.testing.expectEqual(@as(usize, 0), hidden.rows.len);
    authority.hide_relationships = false;
    var bound_graph = graph;
    var bound_labels = [_]root.catalog.Label{ graph.labels[0], graph.labels[1] };
    bound_labels[0].table_id = 7;
    bound_graph.labels = &bound_labels;
    var bound_relationships = [_]root.catalog.Relationship{ graph.relationships[0], graph.relationships[1] };
    bound_relationships[0].identity = .{ .incarnation = 42, .config_hash = 99 };
    bound_graph.relationships = &bound_relationships;
    try std.testing.expectError(error.CatalogGenerationChanged, root.runtime.execute(std.testing.allocator, native.backend(), bound_graph, &compiled, &.{}, .{}));
    docs.core.identity_namespace.table_id = 7;
    try std.testing.expectError(error.CatalogGenerationChanged, root.runtime.execute(std.testing.allocator, native.backend(), bound_graph, &compiled, &.{}, .{}));
    bound_relationships[0].identity = .{ .incarnation = 41, .config_hash = 99 };
    var bound_result = try root.runtime.execute(std.testing.allocator, native.backend(), bound_graph, &compiled, &.{}, .{});
    defer bound_result.deinit();
    try std.testing.expectEqual(@as(usize, 2), bound_result.rows.len);
    docs.open_mode = .writer;
    try std.testing.expectError(error.CypherSnapshotRequired, root.runtime.execute(std.testing.allocator, native.backend(), graph, &compiled, &.{}, .{}));
    docs.open_mode = .query_readonly;
    authority.revocations = 1;
    try std.testing.expectError(error.PermissionDenied, root.runtime.execute(std.testing.allocator, native.backend(), graph, &compiled, &.{}, .{}));
}

test "native capture and routed owner predicates share the statement work ceiling" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const entries = try a.alloc(v.Entry, 4000);
    for (entries, 0..) |*entry, i| entry.* = .{ .name = try std.fmt.allocPrint(a, "field:{d}", .{i}), .value = .{ .integer = @intCast(i) } };
    const selected: root.catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .relationships = &.{}, .labels = &.{.{ .name = "D", .table = "docs", .predicate = &.{.{ .name = "meta", .value = .{ .map = entries } }} }} };
    var docs: Database = .{ .name = "docs", .document_json = try std.json.Stringify.valueAlloc(a, v.Value{ .map = &.{.{ .name = "meta", .value = .{ .map = entries } }} }, .{}) };
    var authority: Authority = .{};
    var native: Native = .{ .tables = &.{.{ .name = "docs", .db = &docs }}, .authority = authority.callbacks(), .ttl_now_ns = 1 };
    try std.testing.expectError(error.CypherStateBudgetExceeded, root.snapshot.Materialized.create(std.testing.allocator, native.backend(), selected, .{ .intermediate_states = 100 }));
    const snapshot = try root.snapshot.Materialized.create(std.testing.allocator, native.backend(), selected, .{});
    defer snapshot.deinit();
    try std.testing.expect(snapshot.overlay.captured.intermediate_states > 4000);
    try std.testing.expect(snapshot.overlay.captured.intermediate_states < 200_000);
    const Channel = struct {
        native: *Native,
        graph: root.catalog.Graph,
        fn call(ptr: *anyopaque, allocator: A, request: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return (root.rpc.Owner{ .backend = self.native.backend(), .graph = self.graph, .table = "docs" }).execute(allocator, request);
        }
    };
    var channel: Channel = .{ .native = &native, .graph = selected };
    var client: root.rpc.Client = .{ .channel = .{ .ptr = &channel, .call = Channel.call }, .incarnation = 1, .revision = 1 };
    try std.testing.expectError(error.CypherStateBudgetExceeded, root.snapshot.Materialized.create(std.testing.allocator, client.backend(), selected, .{ .intermediate_states = 100 }));
    const remote = try root.snapshot.Materialized.create(std.testing.allocator, client.backend(), selected, .{});
    defer remote.deinit();
    try std.testing.expect(remote.overlay.captured.intermediate_states > 4000);
    try std.testing.expect(remote.overlay.captured.intermediate_states < 200_000);
    // An owner page can legitimately exceed the default ceiling when the
    // coordinator grants a larger operator budget; RPC must not reclamp it.
    const labels = try a.alloc(root.catalog.Label, 64);
    for (labels, 0..) |*label, i| {
        label.* = selected.labels[0];
        label.name = try std.fmt.allocPrint(a, "D{d}", .{i});
    }
    var expanded = selected;
    expanded.labels = labels;
    channel.graph = expanded;
    const backend = native.backend();
    try backend.vtable.bind(backend.ptr, a, expanded);
    try std.testing.expectError(error.CypherStateBudgetExceeded, root.snapshot.Materialized.create(std.testing.allocator, client.backend(), expanded, .{}));
    const large = try root.snapshot.Materialized.create(std.testing.allocator, client.backend(), expanded, .{ .intermediate_states = 8_000_000 });
    defer large.deinit();
    try std.testing.expect(large.overlay.captured.intermediate_states > 1_000_000);
    try std.testing.expect(large.overlay.captured.intermediate_states < 8_000_000);
}

test "native required fields prune after full policy checks and own projected values" {
    var docs: Database = .{ .name = "docs" };
    var authority: Authority = .{};
    var native: Native = .{ .tables = &.{.{ .name = "docs", .db = &docs }}, .authority = authority.callbacks(), .ttl_now_ns = 1 };
    const selected: root.catalog.Graph = .{ .name = "alpha", .incarnation = 2, .revision = 1, .labels = &.{.{ .name = "Alpha", .table = "docs", .predicate = &.{.{ .name = "title", .value = .{ .string = "Alpha" } }} }}, .relationships = &.{} };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const backend = native.backend();
    try backend.vtable.bind(backend.ptr, a, selected);
    backend.vtable.set_read_fields.?(backend.ptr, &.{});
    const node = (try backend.vtable.get(backend.ptr, a, .{ .table = "docs", .key = "a" })).?;
    try std.testing.expect(authority.saw_secret);
    try std.testing.expectEqual(@as(usize, 0), node.properties.len);
    try std.testing.expectEqualStrings("Alpha", node.labels[0]);
    try std.testing.expectEqualStrings("a", node.identity.key);
    backend.vtable.set_read_fields.?(backend.ptr, &.{ "title", "secret" });
    const projected = (try backend.vtable.get(backend.ptr, a, .{ .table = "docs", .key = "a" })).?;
    try std.testing.expectEqual(@as(usize, 1), projected.properties.len);
    try std.testing.expectEqualStrings("Alpha", (v.Value{ .node = projected }).property("title").string);
    authority.deny_b = true;
    try std.testing.expect((try backend.vtable.get(backend.ptr, a, .{ .table = "docs", .key = "b" })) == null);
}

test "native pair seeks preserve parallel cross-table identities and self-loop direction phases" {
    var docs: Database = .{ .name = "docs" };
    var people: Database = .{ .name = "people" };
    var authority: Authority = .{};
    var native: Native = .{ .tables = &.{ .{ .name = "docs", .db = &docs }, .{ .name = "people", .db = &people } }, .authority = authority.callbacks(), .ttl_now_ns = 1 };
    const backend = native.backend();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try backend.vtable.bind(backend.ptr, a, graph);
    for ([_]bool{ false, true }) |self_loop| {
        const mapping = graph.relationships[if (self_loop) @as(usize, 1) else 0];
        const peer: v.Identity = .{ .table = if (self_loop) "docs" else "people", .key = "a" };
        var after: ?[]const u8 = null;
        var total: usize = 0;
        while (true) {
            const page = try backend.vtable.expand_between.?(backend.ptr, a, mapping, .{ .table = "docs", .key = "a" }, peer, .both, &.{}, after, 1, 10);
            for (page.items) |edge| {
                try std.testing.expect(edge.target.eql(peer));
                try std.testing.expectEqualStrings("docs", edge.source.table);
                total += 1;
            }
            after = page.next orelse break;
        }
        try std.testing.expectEqual(@as(usize, if (self_loop) 1 else 2), total);
    }
}
