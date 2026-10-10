// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Visibility-aware traversal of an immutable composed source graph.
//! Adjacency stays in the leaf indexes. Only bounded frontier/path state and
//! requested documents cross the coordinator; no archive graph is copied.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const V = std.json.Value;
const Response = @import("contextual_operations.zig").OwnedResponse;
const query = local.graph_query;
const keys_mod = local.serverless_external_source_mod.lake_catalog.row_commit.stable_key;
pub const Leaf = struct { table: []const u8, physical: []const u8, token: []const u8, identity: []const u8 };
const Node = struct { leaf: usize, key: []const u8, row: V };
const Entry = struct { node: Node, depth: u32, path: []const V, edges: []const V };
const View = struct {
    a: A,
    ptr: *anyopaque,
    run: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!Response,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    leaves: []const Leaf,
    keys: []const []const u8,
    kinds: []const keys_mod.Kind,
    tombstone: []const u8,
    visibility: ?V,
    calls: usize = 0,
    fn call(self: *View, leaf: usize, input: V) !V {
        try self.checkpoint(self.ptr);
        if (self.calls == 100_000) return error.GraphWorkLimitExceeded;
        self.calls += 1;
        var request = input;
        try request.object.put(self.a, "remote_snapshot", .{ .string = self.leaves[leaf].token });
        if (self.visibility) |selected| try request.object.put(self.a, "lake_visibility", selected);
        var response = try self.run(self.ptr, self.a, self.leaves[leaf].table, try std.json.Stringify.valueAlloc(self.a, request, .{}));
        defer response.deinit(self.a);
        if (response.status != 200) return switch (response.status) {
            401, 403 => error.Forbidden,
            409, 410 => error.CatalogGenerationChanged,
            422 => error.UnsupportedQueryRequest,
            408, 504 => error.QueryTimedOut,
            else => error.ComposedGraphLeafFailed,
        };
        const decoded = try std.json.parseFromSliceLeaky(V, self.a, response.body, .{ .allocate = .alloc_always });
        const results = decoded.object.get("responses") orelse return error.InvalidQueryRequest;
        if (results != .array or results.array.items.len != 1) return error.InvalidQueryRequest;
        const result = results.array.items[0];
        const token = result.object.get("remote_snapshot") orelse return error.CatalogGenerationChanged;
        if (token != .string or !std.mem.eql(u8, token.string, self.leaves[leaf].token)) return error.CatalogGenerationChanged;
        const serving_identity = try std.json.Stringify.valueAlloc(self.a, result.object.get("_composed_identity") orelse .null, .{});
        if (!std.mem.eql(u8, serving_identity, self.leaves[leaf].identity)) return error.CatalogGenerationChanged;
        return result;
    }
    fn select(self: *View, leaf: usize, filter: V) !?Node {
        const request = try value(self.a, .{ .full_text_search = .{ .match_all = V{ .object = .empty } }, .filter_query = filter, .order_by = .{.{ .field = "_id" }}, .limit = 2 });
        const result = try self.call(leaf, request);
        const hits = result.object.get("hits") orelse return error.InvalidQueryRequest;
        const total = hits.object.get("total").?;
        if (total == .object) if (total.object.get("relation")) |relation| if (relation != .string or (!std.mem.eql(u8, relation.string, "exact") and !std.mem.eql(u8, relation.string, "eq"))) return error.QueryCandidateBudgetExceeded;
        const count = if (total == .integer) total.integer else total.object.get("value").?.integer;
        const rows = hits.object.get("hits").?.array.items;
        if (count != rows.len or rows.len > 1) return error.InvalidComposedGraphIdentity;
        if (rows.len == 0) return null;
        return .{ .leaf = leaf, .key = rows[0].object.get("_id").?.string, .row = rows[0].object.get("_source") orelse return error.UnsupportedQueryRequest };
    }
    fn visible(self: *View, leaf: usize, key: []const u8) !?Node {
        // External graph entities need not have materialized documents. They
        // are admitted only as explicit seeds or endpoints from pinned edges;
        // the edge's owning fact still has to pass document visibility below.
        const raw = (try self.select(leaf, try value(self.a, .{ .doc_id = .{ .ids = .{key} } }))) orelse return .{ .leaf = leaf, .key = key, .row = .null };
        var node = raw;
        if (self.keys.len != 0 and leaf == 0) {
            var terms: std.ArrayList(V) = .empty;
            for (self.keys, 0..) |field, ordinal| {
                const normalized = try keys_mod.normalize(self.a, raw.row.object.get(field) orelse return error.InvalidLakeKey, if (self.kinds.len == 0) .scalar else self.kinds[ordinal]);
                try terms.append(self.a, try value(self.a, .{ .term = .{ .path = try std.fmt.allocPrint(self.a, "/{s}", .{field}), .value = normalized } }));
            }
            if (try self.select(1, try value(self.a, .{ .conjuncts = terms.items }))) |replacement| node = replacement;
        }
        if (self.keys.len != 0 and node.leaf == 1) if (node.row.object.get(self.tombstone)) |deleted| {
            if (deleted != .null and deleted != .bool) return error.InvalidQueryRequest;
            if (deleted == .bool and deleted.bool) return null;
        };
        return node;
    }
    fn leafFor(self: *View, table: []const u8) ?usize {
        for (self.leaves, 0..) |leaf, i| if (std.mem.eql(u8, leaf.table, table) or std.mem.eql(u8, leaf.physical, table)) return i;
        return null;
    }
    fn identity(self: *View, node: Node) ![]const u8 {
        return std.json.Stringify.valueAlloc(self.a, .{ self.leaves[node.leaf].table, node.key }, .{});
    }
    fn endpoint(self: *View, node: Node) !V {
        return value(self.a, .{ .table = self.leaves[node.leaf].table, .key = node.key });
    }
};
fn value(a: A, input: anytype) !V {
    return std.json.parseFromSliceLeaky(V, a, try std.json.Stringify.valueAlloc(a, input, .{}), .{ .allocate = .alloc_always });
}
fn appendNode(view: *View, nodes: *std.ArrayList(Node), leaf: usize, key: []const u8) !void {
    if (try view.visible(leaf, key)) |node| try nodes.append(view.a, node);
}
fn starts(view: *View, selector: query.NodeSelector, hits: []const V, complete_hits: bool, prior: V) ![]Node {
    var nodes: std.ArrayList(Node) = .empty;
    switch (selector) {
        .keys => |keys| for (keys) |key| {
            for (view.leaves, 0..) |_, i| try appendNode(view, &nodes, i, key);
        },
        .identities => |identities| for (identities) |node| {
            if (node.table) |table| {
                const leaf = view.leafFor(table) orelse return error.InvalidComposedGraphIdentity;
                try appendNode(view, &nodes, leaf, node.key);
            } else for (view.leaves, 0..) |_, i| try appendNode(view, &nodes, i, node.key);
        },
        .result_ref => |ref| {
            if (ref.binding != null) return error.UnsupportedQueryRequest;
            const retrieval = std.mem.eql(u8, ref.ref, "$query_results");
            const items = if (retrieval) hits else graph: {
                const prefix = "$graph_results.";
                if (!std.mem.startsWith(u8, ref.ref, prefix)) return error.InvalidQueryRequest;
                const result = prior.object.get(ref.ref[prefix.len..]) orelse return error.InvalidQueryRequest;
                if (ref.limit == 0 and result.object.get("stats").?.object.get("truncated").?.bool) return error.GraphResultRefRequiresLimit;
                break :graph result.object.get("nodes").?.array.items;
            };
            if (retrieval and ref.limit == 0 and !complete_hits) return error.GraphResultRefRequiresLimit;
            const count = if (ref.limit == 0) items.len else @min(items.len, ref.limit);
            for (items[0..count]) |item| {
                const table = item.object.get(if (retrieval) "_table" else "table").?.string;
                const key = item.object.get(if (retrieval) "_id" else "key").?.string;
                try appendNode(view, &nodes, view.leafFor(table) orelse return error.InvalidComposedGraphIdentity, key);
            }
        },
    }
    return nodes.items;
}
pub fn execute(a: A, specification: V, leaves: []const Leaf, overlay_keys: []const []const u8, kinds: []const keys_mod.Kind, tombstone: []const u8, visibility: ?V, hits: []const V, complete_hits: bool, executor: anytype) !V {
    var view: View = .{ .a = a, .ptr = executor.ptr, .run = executor.execute, .checkpoint = executor.checkpoint, .leaves = leaves, .keys = overlay_keys, .kinds = kinds, .tombstone = tombstone, .visibility = visibility };
    var parsed = try @import("antfly-json").parseFromSlice(@import("antfly_metadata_openapi").QueryRequest, a, try std.json.Stringify.valueAlloc(a, .{ .graph_queries = specification }, .{}), .{});
    defer parsed.deinit();
    const named = try @import("public_graph_query.zig").parseCanonicalGraphQueriesAlloc(a, parsed.value);
    defer @import("public_graph_query.zig").freeNamedGraphQueries(a, named);
    const order = try query.executionOrderAlloc(a, named);
    var results: V = .{ .object = .empty };
    for (order) |ordinal| {
        const operation = named[ordinal];
        const q = operation.query;
        if (q.query_type != .traverse and q.query_type != .neighbors) return error.UnsupportedQueryRequest;
        if (q.match_pattern != null or q.target_nodes != null or q.metrics.len != 0 or q.order_by.len != 0 or q.where_metric.len != 0 or query.nodeFilterActive(q.params.node_filter)) return error.UnsupportedQueryRequest;
        var queue: std.ArrayList(Entry) = .empty;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        for (try starts(&view, q.start_nodes, hits, complete_hits, results)) |node| {
            const key = try view.identity(node);
            if ((try seen.getOrPut(a, key)).found_existing) continue;
            try queue.append(a, .{ .node = node, .depth = 0, .path = try a.dupe(V, &.{try view.endpoint(node)}), .edges = &.{} });
        }
        var output: std.ArrayList(V) = .empty;
        var head: usize = 0;
        var truncated = false;
        while (head < queue.items.len) {
            try executor.checkpoint(executor.ptr);
            const entry = queue.items[head];
            head += 1;
            if (entry.depth != 0) {
                if (output.items.len == q.params.max_results) {
                    truncated = true;
                    break;
                }
                var node = try value(a, .{ .table = leaves[entry.node.leaf].table, .key = entry.node.key, .depth = entry.depth });
                if (q.include_documents) {
                    var row = entry.node.row;
                    if (row != .null and !q.include_all_fields) {
                        row = .{ .object = .empty };
                        for (q.fields) |field| try row.object.put(a, field, entry.node.row.object.get(field) orelse .null);
                    }
                    try node.object.put(a, "document", row);
                }
                if (q.params.include_paths) {
                    try node.object.put(a, "path", try value(a, entry.path));
                    try node.object.put(a, "path_edges", try value(a, entry.edges));
                }
                try output.append(a, node);
            }
            if (entry.depth == q.params.max_depth) continue;
            // Each leaf evaluates its relationship predicates against its own
            // retained edge generation. Never apply retrieval filters here.
            const operation_body = try value(a, .{ .index = q.index_name, .traverse = .{ .start = .{ .keys = .{entry.node.key} }, .direction = @tagName(q.params.direction), .edge_types = q.params.edge_types, .max_depth = 1, .limit = 10000, .include_paths = true } });
            var graph: V = .{ .object = .empty };
            try graph.object.put(a, "step", operation_body);
            // Copy canonical relationship predicates without changing their
            // public spelling or losing decimal/timestamp values.
            const original = specification.object.get(operation.name).?.object.get("traverse").?;
            const traverse = graph.object.getPtr("step").?.object.getPtr("traverse").?;
            for ([_][]const u8{ "edge_filter", "edge_weight" }) |field| if (original.object.get(field)) |filter| try traverse.object.put(a, field, filter);
            const response = try view.call(entry.node.leaf, try value(a, .{ .full_text_search = .{ .match_none = V{ .object = .empty } }, .graph_queries = graph, .order_by = .{.{ .field = "_id" }}, .limit = 1 }));
            const step = (response.object.get("graph_results") orelse return error.InvalidQueryRequest).object.get("step") orelse return error.InvalidQueryRequest;
            if (step.object.get("stats").?.object.get("truncated").?.bool) return error.GraphWorkLimitExceeded;
            for (step.object.get("nodes").?.array.items) |candidate| {
                const target_leaf = if (candidate.object.get("table")) |table| if (table == .string) view.leafFor(table.string) orelse continue else entry.node.leaf else entry.node.leaf;
                const target = (try view.visible(target_leaf, candidate.object.get("key").?.string)) orelse continue;
                const edges = candidate.object.get("path_edges") orelse return error.InvalidQueryRequest;
                if (edges != .array or edges.array.items.len != 1) return error.InvalidQueryRequest;
                var edge = edges.array.items[0];
                if (edge.object.get("owner_document")) |owner| if (owner == .string) {
                    const visible_owner = (try view.visible(entry.node.leaf, owner.string)) orelse continue;
                    if (visible_owner.row == .null or visible_owner.leaf != entry.node.leaf or !std.mem.eql(u8, visible_owner.key, owner.string)) continue;
                };
                const identity = try view.identity(target);
                if ((try seen.getOrPut(a, identity)).found_existing) continue;
                if (queue.items.len == 100_000) return error.GraphWorkLimitExceeded;
                const path = try a.alloc(V, entry.path.len + 1);
                @memcpy(path[0..entry.path.len], entry.path);
                path[entry.path.len] = try view.endpoint(target);
                const path_edges = try a.alloc(V, entry.edges.len + 1);
                @memcpy(path_edges[0..entry.edges.len], entry.edges);
                // Keep traversal provenance in the composed identity domain.
                try edge.object.put(a, "from", try view.endpoint(entry.node));
                try edge.object.put(a, "to", try view.endpoint(target));
                path_edges[entry.edges.len] = edge;
                try queue.append(a, .{ .node = target, .depth = entry.depth + 1, .path = path, .edges = path_edges });
            }
        }
        try results.object.put(a, try a.dupe(u8, operation.name), try value(a, .{ .kind = "nodes", .nodes = output.items, .stats = .{ .returned_items = output.items.len, .truncated = truncated } }));
    }
    return results;
}

const Fixture = struct {
    hidden: bool = false,
    replacement: bool = false,
    external: bool = false,
    changed: bool = false,
    calls: usize = 0,
    fn checkpoint(_: *anyopaque) !void {}
    fn run(raw: *anyopaque, a: A, table: []const u8, body: []const u8) !Response {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.calls += 1;
        const request = try std.json.parseFromSliceLeaky(V, a, body, .{});
        try std.testing.expectEqualStrings(table, request.object.get("remote_snapshot").?.string);
        // Validate real public DSL spelling; mocks must not accept malformed
        // empty predicates that production admission rejects.
        var admission = try value(a, request);
        _ = admission.object.orderedRemove("remote_snapshot");
        var checked = try local.api_query.parsePublicQueryRequest(a, null, table, try std.json.Stringify.valueAlloc(a, admission, .{}));
        defer checked.deinit(a);
        const first = std.mem.eql(u8, table, "history");
        var rows: std.ArrayList(V) = .empty;
        var graphs: V = .{ .object = .empty };
        if (request.object.get("graph_queries")) |queries| {
            const key = queries.object.get("step").?.object.get("traverse").?.object.get("start").?.object.get("keys").?.array.items[0].string;
            var neighbors: std.ArrayList(V) = .empty;
            if ((first and std.mem.eql(u8, key, "a")) or (!first and std.mem.eql(u8, key, "b"))) {
                const target = if (first) "b" else "c";
                try neighbors.append(a, try value(a, .{ .table = "physical-current", .key = target, .path_edges = .{.{ .owner_document = if (self.hidden and first) "removed" else if (self.external and !first) "factb" else key, .direction = "forward", .relationship_type = "link" }} }));
            }
            try graphs.object.put(a, "step", try value(a, .{ .nodes = neighbors.items, .stats = .{ .truncated = false } }));
        } else {
            const filter = request.object.get("filter_query").?;
            const requested = if (filter.object.get("doc_id")) |ids| ids.object.get("ids").?.array.items[0].string else if (filter.object.get("conjuncts").?.array.items[0].object.get("term").?.object.get("value").?.integer == 1) "a" else "absent";
            if ((!first and self.replacement and std.mem.eql(u8, requested, "a")) or (first and std.mem.eql(u8, requested, "a")) or (!first and self.external and std.mem.eql(u8, requested, "factb")) or (!first and !self.external and (std.mem.eql(u8, requested, "b") or std.mem.eql(u8, requested, "c")))) {
                try rows.append(a, try value(a, .{ ._id = requested, ._source = .{ .name = requested, .id = if (std.mem.eql(u8, requested, "a")) @as(i64, 1) else if (std.mem.eql(u8, requested, "b")) @as(i64, 2) else @as(i64, 3) } }));
            }
        }
        return @import("contextual_operations.zig").json(try std.json.Stringify.valueAlloc(a, .{ .responses = .{.{ .remote_snapshot = table, ._composed_identity = if (self.changed) "recreated" else "stable", .hits = .{ .total = .{ .value = rows.items.len, .relation = "exact" }, .hits = rows.items }, .graph_results = graphs }} }, .{}), false);
    }
};
test "external lake composed graph crosses pinned sources and rejects hidden fact owners" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    const executor = @import("composed_query.zig").Executor{ .ptr = &fixture, .execute = Fixture.run, .checkpoint = Fixture.checkpoint };
    const leaves: []const Leaf = &.{ .{ .table = "history", .physical = "physical-history", .token = "history", .identity = "\"stable\"" }, .{ .table = "current", .physical = "physical-current", .token = "current", .identity = "\"stable\"" } };
    const spec = try std.json.parseFromSliceLeaky(V, a, "{\"walk\":{\"index\":\"relations\",\"traverse\":{\"start\":{\"keys\":[\"a\"]},\"max_depth\":2,\"limit\":10,\"include_paths\":true,\"include_documents\":true}}}", .{});
    const result = try execute(a, spec, leaves, &.{}, &.{}, "_deleted", null, &.{}, true, executor);
    const nodes = result.object.get("walk").?.object.get("nodes").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), nodes.len);
    try std.testing.expectEqualStrings("current", nodes[1].object.get("table").?.string);
    try std.testing.expectEqualStrings("c", nodes[1].object.get("key").?.string);
    try std.testing.expectEqual(@as(usize, 3), nodes[1].object.get("path").?.array.items.len);
    fixture.external = true;
    const external = try execute(a, spec, leaves, &.{}, &.{}, "_deleted", null, &.{}, true, executor);
    const external_nodes = external.object.get("walk").?.object.get("nodes").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), external_nodes.len);
    try std.testing.expect(external_nodes[1].object.get("document").? == .null);
    fixture.external = false;
    fixture.hidden = true;
    const hidden = try execute(a, spec, leaves, &.{}, &.{}, "_deleted", null, &.{}, true, executor);
    try std.testing.expectEqual(@as(usize, 0), hidden.object.get("walk").?.object.get("nodes").?.array.items.len);
    fixture.hidden = false;
    fixture.replacement = true;
    const overridden = try execute(a, spec, leaves, &.{"id"}, &.{}, "_deleted", null, &.{}, true, executor);
    try std.testing.expectEqual(@as(usize, 0), overridden.object.get("walk").?.object.get("nodes").?.array.items.len);
    fixture.changed = true;
    try std.testing.expectError(error.CatalogGenerationChanged, execute(a, spec, leaves, &.{}, &.{}, "_deleted", null, &.{}, true, executor));
}
