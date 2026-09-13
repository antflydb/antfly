// Copyright 2026 Antfly, Inc.
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

//! Query-local ordinal adapter over stable page identities. Only visited node,
//! type and qualified-table strings are interned. Cursors retain bounded pages,
//! not complete adjacency, and share admitted memory and transport allowances.
const std = @import("std");
const Allocator = std.mem.Allocator;
const graph = @import("page_graph.zig");
const keys = @import("page_keys.zig");
const tree = @import("page_tree.zig");
const page_store = @import("page_store.zig");
const artifacts = @import("../artifacts/store.zig");
const refs = @import("../manifest/artifact_ref.zig");
const wire = @import("packed.zig");
const types = @import("types.zig");
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const Dictionary = std.StringArrayHashMapUnmanaged(void);

fn clear(alloc: Allocator, dict: *Dictionary) void {
    for (dict.keys()) |key| alloc.free(key);
    dict.deinit(alloc);
}

fn intern(alloc: Allocator, dict: *Dictionary, name: []const u8) !u32 {
    if (dict.getIndex(name)) |index| return @intCast(index);
    if (dict.count() >= std.math.maxInt(u32)) return error.QueryCandidateBudgetExceeded;
    const owned = try alloc.dupe(u8, name);
    errdefer alloc.free(owned);
    const index: u32 = @intCast(dict.count());
    try dict.put(alloc, owned, {});
    return index;
}

pub const Reader = struct {
    alloc: Allocator,
    root: graph.Root,
    pages: page_store.PageStore,
    cache: tree.Cache,
    no_writes: u64 = 0,
    nodes: Dictionary = .empty,
    kinds: Dictionary = .empty,
    tables: Dictionary = .empty,
    table_metadata: std.ArrayListUnmanaged([]u8) = .empty,

    pub fn create(alloc: Allocator, store: *artifacts.ArtifactStore, source: refs.ArtifactRef, cancellation: CancellationToken, remaining: *u64, shared: ?page_store.ReadCache) !*Reader {
        const self = try alloc.create(Reader);
        errdefer alloc.destroy(self);
        self.* = .{
            .alloc = alloc,
            .root = .{},
            .pages = .{ .artifacts = store, .cancellation = cancellation, .remaining_read_bytes = remaining, .remaining_write_bytes = &self.no_writes, .read_cache = shared },
            .cache = undefined,
        };
        self.root = try self.pages.loadRoot(alloc, source);
        self.cache = .{ .alloc = alloc, .underlying = self.pages.store() };
        return self;
    }

    pub fn destroy(self: *Reader) void {
        clear(self.alloc, &self.nodes);
        clear(self.alloc, &self.kinds);
        clear(self.alloc, &self.tables);
        for (self.table_metadata.items) |metadata| self.alloc.free(metadata);
        self.table_metadata.deinit(self.alloc);
        self.cache.deinit();
        self.alloc.destroy(self);
    }

    pub fn containsNode(self: *Reader, node: []const u8) !bool {
        return graph.containsNode(self.alloc, self.cache.store(), self.root, node);
    }

    pub fn ordinal(self: *Reader, node: []const u8) !?u32 {
        if (self.nodes.getIndex(node)) |index| return @intCast(index);
        if (!try self.containsNode(node)) return null;
        return try intern(self.alloc, &self.nodes, node);
    }

    pub fn nodeNameAlloc(self: *Reader, node: u32) ![]u8 {
        if (node >= self.nodes.count()) return error.InvalidGraphSegment;
        return self.alloc.dupe(u8, self.nodes.keys()[node]);
    }

    pub fn tableMetadata(self: *Reader, id: u32) ?[]const u8 {
        if (id >= self.table_metadata.items.len) return null;
        return self.table_metadata.items[id];
    }

    fn wireEdge(self: *Reader, edge: keys.Edge, incoming: bool) !wire.Edge {
        const node = try intern(self.alloc, &self.nodes, if (incoming) edge.source else edge.target);
        const kind = try intern(self.alloc, &self.kinds, edge.kind);
        const table = if (edge.table) |name| table: {
            if (self.tables.getIndex(name)) |id| break :table @as(u32, @intCast(id));
            const metadata = try std.json.Stringify.valueAlloc(self.alloc, .{ .target_table = name }, .{});
            errdefer self.alloc.free(metadata);
            try self.table_metadata.ensureUnusedCapacity(self.alloc, 1);
            const id = try intern(self.alloc, &self.tables, name);
            self.table_metadata.appendAssumeCapacity(metadata);
            break :table id;
        } else null;
        return .{ .node = node, .edge_type = kind, .table = table, .weight = edge.weight };
    }

    pub fn copyEdge(self: *Reader, edge: wire.Edge) !types.Edge {
        if (edge.edge_type >= self.kinds.count()) return error.InvalidGraphSegment;
        const name = try self.nodeNameAlloc(edge.node);
        errdefer self.alloc.free(name);
        return .{ .neighbor_id = name, .edge_type = try self.alloc.dupe(u8, self.kinds.keys()[edge.edge_type]), .weight = edge.weight, .neighbor_table_id = edge.table };
    }

    pub fn resolveTypes(self: *Reader, requested: []const []const u8) !?[]u32 {
        if (requested.len == 0) return null;
        var ids: std.ArrayListUnmanaged(u32) = .empty;
        errdefer ids.deinit(self.alloc);
        for (requested) |name| {
            const id = try intern(self.alloc, &self.kinds, name);
            if (std.mem.indexOfScalar(u32, ids.items, id) == null) try ids.append(self.alloc, id);
        }
        std.mem.sort(u32, ids.items, self.kinds.keys(), struct {
            fn less(names: []const []const u8, a: u32, b: u32) bool {
                return std.mem.order(u8, names[a], names[b]) == .lt;
            }
        }.less);
        return try ids.toOwnedSlice(self.alloc);
    }

    pub const Cursor = struct {
        reader: *Reader,
        node: u32,
        filters: ?[]u32,
        filter_count: usize = 0,
        incoming: bool,
        work: *usize,
        range: usize = 0,
        active: ?graph.Cursor = null,

        pub fn deinit(self: *Cursor) void {
            if (self.active) |*active| active.deinit();
            if (self.filters) |ids| self.reader.alloc.free(ids);
            self.* = undefined;
        }

        pub fn nextWire(self: *Cursor) !?wire.Edge {
            const count: usize = if (self.filters != null) self.filter_count else 1;
            while (self.range < count) {
                if (self.active == null) {
                    const kind = if (self.filters) |ids| self.reader.kinds.keys()[ids[self.range]] else null;
                    self.active = try graph.Cursor.adjacency(self.reader.alloc, self.reader.cache.store(), self.reader.root, self.reader.nodes.keys()[self.node], if (self.incoming) .incoming else .outgoing, kind);
                }
                const edge = try self.active.?.next() orelse {
                    self.active.?.deinit();
                    self.active = null;
                    self.range += 1;
                    continue;
                };
                if (self.work.* == 0) return error.GraphTraversalQueryBudgetExceeded;
                self.work.* -= 1;
                return try self.reader.wireEdge(edge, self.incoming);
            }
            return null;
        }
    };

    pub fn cursorOrdinal(self: *Reader, node: u32, filter: ?[]const u32, incoming: bool, work: *usize) !Cursor {
        if (node >= self.nodes.count()) return error.InvalidGraphSegment;
        if (filter) |ids| for (ids) |id| if (id >= self.kinds.count()) return error.InvalidGraphSegment;
        const owned = if (filter) |ids| try self.alloc.dupe(u32, ids) else null;
        var count: usize = 0;
        if (owned) |ids| {
            std.mem.sort(u32, ids, self.kinds.keys(), struct {
                fn less(names: []const []const u8, a: u32, b: u32) bool {
                    return std.mem.order(u8, names[a], names[b]) == .lt;
                }
            }.less);
            for (ids) |id| {
                if (count != 0 and ids[count - 1] == id) continue;
                ids[count] = id;
                count += 1;
            }
        }
        return .{ .reader = self, .node = node, .filters = owned, .filter_count = count, .incoming = incoming, .work = work };
    }

    pub fn cursor(self: *Reader, node: []const u8, requested: []const []const u8, incoming: bool, work: *usize) !Cursor {
        const id = try intern(self.alloc, &self.nodes, node);
        const ids = try self.resolveTypes(requested);
        defer if (ids) |values| self.alloc.free(values);
        return self.cursorOrdinal(id, ids, incoming, work);
    }

    pub fn probe(self: *Reader, source: []const u8, kind: []const u8, target: []const u8, work: *usize) !?types.Edge {
        var prefix = try keys.Key.neighborPrefix(self.alloc, source, kind, target);
        defer prefix.deinit(self.alloc);
        const upper = try prefix.successorAlloc(self.alloc);
        defer self.alloc.free(upper);
        var raw = try tree.Cursor.init(self.alloc, self.cache.store(), self.root.page, prefix.bytes.items, upper);
        defer raw.deinit();
        const record = try raw.next() orelse return null;
        if (work.* == 0) return error.GraphTraversalQueryBudgetExceeded;
        work.* -= 1;
        const scratch = try self.alloc.alloc(u8, record.key.len);
        defer self.alloc.free(scratch);
        const decoded = try keys.decode(record.key, scratch);
        if (record.value.len != 0 or decoded.direction != .outgoing or decoded.topology) return error.InvalidGraphPageKey;
        return try self.copyEdge(try self.wireEdge(decoded.edge.?, false));
    }

    pub fn adjacencyFiltered(self: *Reader, node: []const u8, requested: []const []const u8, direction: anytype, limit: usize, work: *usize, qualified: bool, deduplicate_self: bool) !?types.Adjacency {
        if (!try self.containsNode(node)) return null;
        var outgoing: std.ArrayListUnmanaged(types.Edge) = .empty;
        defer outgoing.deinit(self.alloc);
        errdefer for (outgoing.items) |*edge| edge.deinit(self.alloc);
        var incoming: std.ArrayListUnmanaged(types.Edge) = .empty;
        defer incoming.deinit(self.alloc);
        errdefer for (incoming.items) |*edge| edge.deinit(self.alloc);
        for ([_]bool{ false, true }) |reverse| {
            if ((!reverse and direction == .in) or (reverse and direction == .out)) continue;
            var selected = try self.cursor(node, requested, reverse, work);
            defer selected.deinit();
            while (try selected.nextWire()) |wire_edge| {
                if (!reverse and !qualified and wire_edge.table != null) continue;
                if (reverse and direction == .both and deduplicate_self and std.mem.eql(u8, self.nodes.keys()[wire_edge.node], node)) continue;
                if (outgoing.items.len + incoming.items.len >= limit) return error.QueryCandidateBudgetExceeded;
                const list = if (reverse) &incoming else &outgoing;
                try list.ensureUnusedCapacity(self.alloc, 1);
                list.appendAssumeCapacity(try self.copyEdge(wire_edge));
            }
        }
        const id = try self.alloc.dupe(u8, node);
        errdefer self.alloc.free(id);
        const out = try outgoing.toOwnedSlice(self.alloc);
        errdefer {
            for (out) |*edge| edge.deinit(self.alloc);
            self.alloc.free(out);
        }
        return .{ .node_id = id, .out_edges = out, .in_edges = try incoming.toOwnedSlice(self.alloc) };
    }
};
