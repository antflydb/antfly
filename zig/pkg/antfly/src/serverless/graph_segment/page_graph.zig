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

//! Source-fenced graph change plans over stable adjacency identities. Planning
//! reads only replaced source rows and affected node membership. A plan owns
//! its normalized mutations and is reusable across graph aliases; publication
//! never reparses documents or discovers a different before-image.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tree = @import("page_tree.zig");
const keys = @import("page_keys.zig");

pub const Root = struct {
    domain: [32]u8 = @splat(0),
    page: ?tree.Ref = null,
    nodes: u64 = 0,
    edges: u64 = 0,

    pub fn eql(a: Root, b: Root) bool {
        if (!std.mem.eql(u8, &a.domain, &b.domain)) return false;
        if (a.nodes != b.nodes or a.edges != b.edges) return false;
        if (a.page) |page| return if (b.page) |other| page.eql(other) else false;
        return b.page == null;
    }

    pub const encoded_bytes = 128;
    pub const metadata_version = 1;

    pub fn encode(self: Root) [encoded_bytes]u8 {
        var bytes = [_]u8{0} ** encoded_bytes;
        @memcpy(bytes[0..8], "AFGROOT3");
        @memcpy(bytes[96..128], &self.domain);
        std.mem.writeInt(u64, bytes[80..88], self.nodes, .little);
        std.mem.writeInt(u64, bytes[88..96], self.edges, .little);
        if (self.page) |page| {
            bytes[8] = 1;
            @memcpy(bytes[16..48], &page.digest);
            std.mem.writeInt(u32, bytes[48..52], page.bytes, .little);
            bytes[52] = page.height;
            std.mem.writeInt(u64, bytes[56..64], page.records, .little);
            @memcpy(bytes[64..80], &page.attempt);
        }
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Root {
        if (bytes.len != encoded_bytes or !std.mem.eql(u8, bytes[0..8], "AFGROOT3") or
            !std.mem.allEqual(u8, bytes[9..16], 0) or !std.mem.allEqual(u8, bytes[53..56], 0))
            return error.InvalidGraphRoot;
        const nodes = std.mem.readInt(u64, bytes[80..88], .little);
        const edges = std.mem.readInt(u64, bytes[88..96], .little);
        if (bytes[8] == 0) {
            if (!std.mem.allEqual(u8, bytes[16..96], 0)) return error.InvalidGraphRoot;
            return .{ .domain = bytes[96..128].* };
        }
        if (bytes[8] != 1) return error.InvalidGraphRoot;
        const page: tree.Ref = .{
            .digest = bytes[16..48].*,
            .bytes = std.mem.readInt(u32, bytes[48..52], .little),
            .height = bytes[52],
            .records = std.mem.readInt(u64, bytes[56..64], .little),
            .attempt = bytes[64..80].*,
        };
        try page.validate();
        if (nodes == 0 or nodes > page.records or edges > page.records) return error.InvalidGraphRoot;
        return .{ .page = page, .nodes = nodes, .edges = edges, .domain = bytes[96..128].* };
    }
};

pub const Replacement = struct {
    id: []const u8,
    /// null removes the explicit document and all of its outgoing edges.
    /// Local incoming edges can still keep the node in the graph.
    edges: ?[]const keys.Edge,
};

const KeyMap = std.StringHashMapUnmanaged(?[]const u8);
const NodeMap = std.StringHashMapUnmanaged(bool);

pub const Plan = struct {
    alloc: Allocator,
    source: Root,
    domain: [32]u8,
    nodes: u64,
    edges: u64,
    changes: []tree.Mutation,

    pub fn deinit(self: *Plan) void {
        for (self.changes) |change| self.alloc.free(change.key);
        self.alloc.free(self.changes);
        self.* = undefined;
    }

    pub fn publish(self: *const Plan, store: tree.Store, current: Root) !Root {
        if (!current.eql(self.source)) return error.GraphChangePlanSourceChanged;
        if (!std.mem.eql(u8, &self.domain, &store.domain)) return error.GraphPageDomainMismatch;
        const page = try tree.apply(self.alloc, store, self.source.page, self.changes);
        return .{ .page = page, .nodes = self.nodes, .edges = self.edges, .domain = self.domain };
    }
};

const Planner = struct {
    alloc: Allocator,
    store: tree.Store,
    source: Root,
    changes: KeyMap = .empty,
    // The boolean records definite after-membership supplied by a new edge or
    // explicit document. Other touched nodes need only one surviving old key.
    nodes: NodeMap = .empty,
    removed_edges: u64 = 0,
    added_edges: u64 = 0,

    fn deinit(self: *Planner) void {
        var changes = self.changes.keyIterator();
        while (changes.next()) |key| self.alloc.free(key.*);
        self.changes.deinit(self.alloc);
        var nodes = self.nodes.keyIterator();
        while (nodes.next()) |key| self.alloc.free(key.*);
        self.nodes.deinit(self.alloc);
    }

    fn touch(self: *Planner, node: []const u8, definitely_present: bool) !void {
        if (self.nodes.getPtr(node)) |value| {
            value.* = value.* or definitely_present;
            return;
        }
        const owned = try self.alloc.dupe(u8, node);
        errdefer self.alloc.free(owned);
        try self.nodes.put(self.alloc, owned, definitely_present);
    }

    fn change(self: *Planner, key: keys.Key, present: bool) !void {
        if (self.changes.getPtr(key.bytes.items)) |value| {
            value.* = if (present) "" else null;
            return;
        }
        const owned = try self.alloc.dupe(u8, key.bytes.items);
        errdefer self.alloc.free(owned);
        try self.changes.put(self.alloc, owned, if (present) "" else null);
    }

    fn edge(self: *Planner, value: keys.Edge, present: bool) !void {
        try self.store.check(self.store.ptr);
        var outgoing = try keys.Key.edge(self.alloc, value, .outgoing);
        defer outgoing.deinit(self.alloc);
        try self.change(outgoing, present);
        if (value.table == null) {
            var incoming = try keys.Key.edge(self.alloc, value, .incoming);
            defer incoming.deinit(self.alloc);
            try self.change(incoming, present);
            var topology = try keys.Key.topologyEdge(self.alloc, value);
            defer topology.deinit(self.alloc);
            try self.change(topology, present);
        }
    }

    fn replace(self: *Planner, replacement: Replacement) !void {
        var member = try keys.Key.adjacency(self.alloc, replacement.id, .member, null);
        defer member.deinit(self.alloc);
        {
            var membership = try tree.Cursor.init(self.alloc, self.store, self.source.page, member.bytes.items, null);
            defer membership.deinit();
            const record = try membership.next();
            const before = record != null and std.mem.eql(u8, record.?.key, member.bytes.items);
            if (before and record.?.value.len != 0) return error.InvalidGraphPageKey;
            if (before != (replacement.edges != null)) try self.change(member, replacement.edges != null);
        }
        const next = replacement.edges orelse &.{};
        // Sort stable keys, then number only exact duplicates. Reordering a JSON
        // array changes neither identities nor multiplicity in any index.
        var canonical: std.ArrayListUnmanaged(keys.Key) = .empty;
        defer {
            for (canonical.items) |*key| key.deinit(self.alloc);
            canonical.deinit(self.alloc);
        }
        try canonical.ensureTotalCapacity(self.alloc, next.len);
        for (next) |value| {
            if (!std.mem.eql(u8, value.source, replacement.id)) return error.InvalidGraphReplacementSource;
            var copy = value;
            copy.occurrence = 0;
            canonical.appendAssumeCapacity(try keys.Key.edge(self.alloc, copy, .outgoing));
        }
        std.mem.sort(keys.Key, canonical.items, {}, struct {
            fn less(_: void, a: keys.Key, b: keys.Key) bool {
                return std.mem.order(u8, a.bytes.items, b.bytes.items) == .lt;
            }
        }.less);
        var occurrence: u32 = 0;
        for (canonical.items, 0..) |*key, i| {
            if (i != 0 and std.mem.eql(u8, canonical.items[i - 1].bytes.items[0 .. canonical.items[i - 1].bytes.items.len - 4], key.bytes.items[0 .. key.bytes.items.len - 4])) {
                occurrence = std.math.add(u32, occurrence, 1) catch return error.GraphSegmentTooLarge;
            } else occurrence = 0;
            std.mem.writeInt(u32, key.bytes.items[key.bytes.items.len - 4 ..][0..4], occurrence, .big);
        }
        // Compute the semantic delta before generating any reverse/topology
        // mutations. In particular, a one-edge edit on a supernode must not
        // enqueue its unchanged degree or reopen every unchanged endpoint.
        var prefix = try keys.Key.adjacency(self.alloc, replacement.id, .outgoing, null);
        defer prefix.deinit(self.alloc);
        const upper = try prefix.successorAlloc(self.alloc);
        defer self.alloc.free(upper);
        var cursor = try tree.Cursor.init(self.alloc, self.store, self.source.page, prefix.bytes.items, upper);
        defer cursor.deinit();
        var scratch: std.ArrayListUnmanaged(u8) = .empty;
        defer scratch.deinit(self.alloc);
        var old = try cursor.next();
        var index: usize = 0;
        while (old != null or index < canonical.items.len) {
            try self.store.check(self.store.ptr);
            const order: std.math.Order = if (old == null) .gt else if (index == canonical.items.len) .lt else std.mem.order(u8, old.?.key, canonical.items[index].bytes.items);
            if (order == .eq) {
                if (old.?.value.len != 0) return error.InvalidGraphPageKey;
                old = try cursor.next();
                index += 1;
                continue;
            }
            const key = if (order == .lt) old.?.key else canonical.items[index].bytes.items;
            try scratch.ensureTotalCapacity(self.alloc, key.len);
            const decoded = try keys.decode(key, scratch.allocatedSlice());
            if (decoded.topology or decoded.direction != .outgoing or
                !std.mem.eql(u8, decoded.node, replacement.id)) return error.InvalidGraphPageKey;
            if (order == .lt) {
                if (old.?.value.len != 0) return error.InvalidGraphPageKey;
                try self.edge(decoded.edge.?, false);
                self.removed_edges = try std.math.add(u64, self.removed_edges, 1);
                old = try cursor.next();
            } else {
                try self.edge(decoded.edge.?, true);
                self.added_edges = try std.math.add(u64, self.added_edges, 1);
                index += 1;
            }
        }
    }

    fn finish(self: *Planner) !Plan {
        var scratch: std.ArrayListUnmanaged(u8) = .empty;
        defer scratch.deinit(self.alloc);
        var mutations = self.changes.iterator();
        while (mutations.next()) |entry| {
            try self.store.check(self.store.ptr);
            try scratch.ensureTotalCapacity(self.alloc, entry.key_ptr.len);
            const decoded = try keys.decode(entry.key_ptr.*, scratch.allocatedSlice());
            if (!decoded.topology) try self.touch(decoded.node, entry.value_ptr.* != null);
        }
        var node_count = self.source.nodes;
        var nodes = self.nodes.iterator();
        while (nodes.next()) |item| {
            try self.store.check(self.store.ptr);
            var prefix = try keys.Key.node(self.alloc, item.key_ptr.*);
            defer prefix.deinit(self.alloc);
            const upper = try prefix.successorAlloc(self.alloc);
            defer self.alloc.free(upper);
            var cursor = try tree.Cursor.init(self.alloc, self.store, self.source.page, prefix.bytes.items, upper);
            defer cursor.deinit();
            var before = false;
            var after = item.value_ptr.*;
            while (try cursor.next()) |record| {
                before = true;
                if (after) break;
                // getPtr distinguishes a deletion from an absent mutation.
                if (self.changes.getPtr(record.key)) |value| {
                    if (value.* != null) after = true;
                } else after = true;
                if (after) break;
            }
            if (before and !after) node_count = std.math.sub(u64, node_count, 1) catch return error.InvalidGraphRoot;
            if (!before and after) node_count = try std.math.add(u64, node_count, 1);
        }
        const edges = try std.math.add(u64, std.math.sub(u64, self.source.edges, self.removed_edges) catch return error.InvalidGraphRoot, self.added_edges);
        const changes = try self.alloc.alloc(tree.Mutation, self.changes.count());
        var iterator = self.changes.iterator();
        var i: usize = 0;
        while (iterator.next()) |entry| : (i += 1) changes[i] = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
        self.changes.clearRetainingCapacity(); // keys now owned by Plan
        std.mem.sort(tree.Mutation, changes, {}, struct {
            fn less(_: void, a: tree.Mutation, b: tree.Mutation) bool {
                return std.mem.order(u8, a.key, b.key) == .lt;
            }
        }.less);
        return .{ .alloc = self.alloc, .source = self.source, .domain = self.store.domain, .nodes = node_count, .edges = edges, .changes = changes };
    }
};

pub fn plan(alloc: Allocator, store: tree.Store, source: Root, replacements: []const Replacement) !Plan {
    try store.check(store.ptr);
    if ((source.page != null or !std.mem.allEqual(u8, &source.domain, 0)) and
        !std.mem.eql(u8, &source.domain, &store.domain)) return error.GraphPageDomainMismatch;
    var planner: Planner = .{ .alloc = alloc, .store = store, .source = source };
    defer planner.deinit();
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    defer seen.deinit(alloc);
    // Validate coalescing before planning. Two versions of one source cannot
    // safely share an old-image scan or independently remove reverse entries.
    for (replacements, 0..) |replacement, i| {
        if (i % 1024 == 0) try store.check(store.ptr);
        const entry = try seen.getOrPut(alloc, replacement.id);
        if (entry.found_existing) return error.UncoalescedGraphReplacements;
    }
    for (replacements) |replacement| try planner.replace(replacement);
    return planner.finish();
}

/// Streaming ingestion owns each normalized record before advancing the source.
/// The input may reuse its document/edge buffers. Duplicate source identities
/// are rejected; callers must coalesce document changes before planning.
pub fn planFromSource(alloc: Allocator, store: tree.Store, root: Root, source: anytype) !Plan {
    try store.check(store.ptr);
    if ((root.page != null or !std.mem.allEqual(u8, &root.domain, 0)) and
        !std.mem.eql(u8, &root.domain, &store.domain)) return error.GraphPageDomainMismatch;
    var planner: Planner = .{ .alloc = alloc, .store = store, .source = root };
    defer planner.deinit();
    var seen: std.StringHashMapUnmanaged(void) = .empty;
    defer {
        var iterator = seen.keyIterator();
        while (iterator.next()) |id| alloc.free(id.*);
        seen.deinit(alloc);
    }
    while (try source.next()) |replacement| {
        try store.check(store.ptr);
        if (seen.contains(replacement.id)) return error.UncoalescedGraphReplacements;
        const id = try alloc.dupe(u8, replacement.id);
        seen.put(alloc, id, {}) catch |err| {
            alloc.free(id);
            return err;
        };
        try planner.replace(replacement);
    }
    return planner.finish();
}

test "serverless graph streaming plans own borrowed replacements and unwind every allocation" {
    const Source = struct {
        index: u8 = 0,
        id: [1]u8 = undefined,
        edges: [1]keys.Edge = undefined,
        duplicate: bool = false,
        pub fn next(self: *@This()) !?Replacement {
            if (self.index == 3) return null;
            self.id[0] = if (self.duplicate) 'a' else 'a' + self.index;
            self.index += 1;
            self.edges[0] = .{ .source = &self.id, .target = "target", .kind = "link" };
            return .{ .id = &self.id, .edges = &self.edges };
        }
    };
    const Exercise = struct {
        fn run(alloc: Allocator) !void {
            var memory = tree.testing.MemoryStore{ .alloc = std.testing.allocator };
            defer memory.deinit();
            var input: Source = .{};
            var planned = try planFromSource(alloc, memory.store(), .{}, &input);
            defer planned.deinit();
            input.id[0] = 'z';
            const root = try planned.publish(memory.store(), .{});
            try std.testing.expectEqual(@as(u64, 3), root.edges);
            try std.testing.expectEqual(@as(u64, 4), root.nodes);
            try std.testing.expect(try containsNode(alloc, memory.store(), root, "a"));
            try std.testing.expect(!try containsNode(alloc, memory.store(), root, "z"));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{});
    var memory = tree.testing.MemoryStore{ .alloc = std.testing.allocator };
    defer memory.deinit();
    var duplicate: Source = .{ .duplicate = true };
    try std.testing.expectError(error.UncoalescedGraphReplacements, planFromSource(std.testing.allocator, memory.store(), .{}, &duplicate));
    try std.testing.expectEqual(@as(usize, 0), memory.writes);
}

/// Prefix-routed graph scan. Each returned edge borrows bounded scratch until
/// next/deinit. A supernode spans ordinary pages; no degree-sized allocation is
/// needed before the first result can be consumed.
pub const Cursor = struct {
    alloc: Allocator,
    prefix: keys.Key,
    upper: []u8,
    cursor: tree.Cursor,
    scratch: std.ArrayListUnmanaged(u8) = .empty,

    pub fn adjacency(alloc: Allocator, store: tree.Store, root: Root, node: []const u8, direction: keys.Direction, kind: ?[]const u8) !Cursor {
        if (direction == .member) return error.InvalidGraphPageKey;
        const prefix = try keys.Key.adjacency(alloc, node, direction, kind);
        return init(alloc, store, root, prefix);
    }

    pub fn topology(alloc: Allocator, store: tree.Store, root: Root, kind: ?[]const u8) !Cursor {
        const prefix = try keys.Key.topology(alloc, kind);
        return init(alloc, store, root, prefix);
    }

    fn init(alloc: Allocator, store: tree.Store, root: Root, owned_prefix: keys.Key) !Cursor {
        var prefix = owned_prefix;
        errdefer prefix.deinit(alloc);
        const upper = try prefix.successorAlloc(alloc);
        errdefer alloc.free(upper);
        return .{
            .alloc = alloc,
            .prefix = prefix,
            .upper = upper,
            .cursor = try tree.Cursor.init(alloc, store, root.page, prefix.bytes.items, upper),
        };
    }

    pub fn deinit(self: *Cursor) void {
        self.cursor.deinit();
        self.scratch.deinit(self.alloc);
        self.prefix.deinit(self.alloc);
        self.alloc.free(self.upper);
        self.* = undefined;
    }

    pub fn next(self: *Cursor) !?keys.Edge {
        const record = try self.cursor.next() orelse return null;
        if (record.value.len != 0 or !std.mem.startsWith(u8, record.key, self.prefix.bytes.items)) return error.InvalidGraphPageKey;
        try self.scratch.ensureTotalCapacity(self.alloc, record.key.len);
        return (try keys.decode(record.key, self.scratch.allocatedSlice())).edge orelse error.InvalidGraphPageKey;
    }
};

pub fn containsNode(alloc: Allocator, store: tree.Store, root: Root, node: []const u8) !bool {
    var prefix = try keys.Key.node(alloc, node);
    defer prefix.deinit(alloc);
    const upper = try prefix.successorAlloc(alloc);
    defer alloc.free(upper);
    var cursor = try tree.Cursor.init(alloc, store, root.page, prefix.bytes.items, upper);
    defer cursor.deinit();
    return try cursor.next() != null;
}

pub fn topologyEdgeCount(alloc: Allocator, store: tree.Store, root: Root, kind: ?[]const u8) !u64 {
    var prefix = try keys.Key.topology(alloc, kind);
    defer prefix.deinit(alloc);
    const upper = try prefix.successorAlloc(alloc);
    defer alloc.free(upper);
    return tree.countRange(alloc, store, root.page, prefix.bytes.items, upper);
}

test "serverless paged graph supernode updates retain only semantic deltas" {
    const a = std.testing.allocator;
    const degree = 2048;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const ids = try a.alloc([8]u8, degree);
    defer a.free(ids);
    const edges = try a.alloc(keys.Edge, degree);
    defer a.free(edges);
    for (ids, edges, 0..) |*id, *edge, i| {
        std.mem.writeInt(u64, id, i, .big);
        edge.* = .{ .source = "hub", .target = id, .kind = "link" };
    }
    var seed = try plan(a, memory.store(), .{}, &.{.{ .id = "hub", .edges = edges }});
    defer seed.deinit();
    const original = try seed.publish(memory.store(), .{});
    var cache = tree.Cache{ .alloc = a, .underlying = memory.store() };
    defer cache.deinit();
    var noop = try plan(a, cache.store(), original, &.{.{ .id = "hub", .edges = edges }});
    defer noop.deinit();
    try std.testing.expectEqual(@as(usize, 0), noop.changes.len);
    try std.testing.expect(original.eql(try noop.publish(cache.store(), original)));
    edges[1024].weight = 2;
    var changed = try plan(a, cache.store(), original, &.{.{ .id = "hub", .edges = edges }});
    defer changed.deinit();
    // One old/new edge pair in each of outgoing, incoming and topology.
    // No unchanged member/edge or unchanged endpoint becomes write work.
    try std.testing.expectEqual(@as(usize, 6), changed.changes.len);
    const before_writes = memory.writes;
    const updated = try changed.publish(cache.store(), original);
    try std.testing.expect(memory.writes - before_writes <= 6 * (@as(usize, original.page.?.height) + 1));
    try std.testing.expectEqual(original.nodes, updated.nodes);
    try std.testing.expectEqual(original.edges, updated.edges);
    // Rebuilding from the complete after-image is the independent semantic
    // oracle; identical complete topology streams imply identical metric input.
    var rebuilt_plan = try plan(a, memory.store(), .{}, &.{.{ .id = "hub", .edges = edges }});
    defer rebuilt_plan.deinit();
    const rebuilt = try rebuilt_plan.publish(memory.store(), .{});
    var expected = try tree.Cursor.init(a, memory.store(), rebuilt.page, "", null);
    defer expected.deinit();
    var actual = try tree.Cursor.init(a, memory.store(), updated.page, "", null);
    defer actual.deinit();
    while (try expected.next()) |record| try std.testing.expectEqualStrings(record.key, (try actual.next()).?.key);
    try std.testing.expect((try actual.next()) == null);
}

test "serverless paged graph plans preserve implicit nodes reverse edges qualification and duplicates" {
    const alloc = std.testing.allocator;
    var backing: tree.testing.MemoryStore = .{ .alloc = alloc };
    defer backing.deinit();
    const initial = [_]keys.Edge{
        .{ .source = "a", .target = "b", .kind = "link" },
        .{ .source = "a", .target = "b", .kind = "link" },
        .{ .source = "a", .target = "foreign", .kind = "link", .table = "remote" },
        .{ .source = "a", .target = "a", .kind = "self" },
    };
    var first = try plan(alloc, backing.store(), .{}, &.{
        .{ .id = "a", .edges = &initial }, .{ .id = "b", .edges = &.{} }, .{ .id = "isolated", .edges = &.{} },
    });
    defer first.deinit();
    const root = try first.publish(backing.store(), .{});
    try std.testing.expectEqual(3, root.nodes);
    try std.testing.expectEqual(4, root.edges);
    try std.testing.expectEqual(3, try topologyEdgeCount(alloc, backing.store(), root, null));
    try std.testing.expectEqual(2, try topologyEdgeCount(alloc, backing.store(), root, "link"));
    try std.testing.expect(root.eql(try Root.decode(&root.encode())));
    try std.testing.expect(try containsNode(alloc, backing.store(), root, "b"));
    try std.testing.expect(!try containsNode(alloc, backing.store(), root, "foreign"));
    var incoming = try Cursor.adjacency(alloc, backing.store(), root, "b", .incoming, "link");
    defer incoming.deinit();
    for (0..2) |i| {
        const edge = (try incoming.next()).?;
        try std.testing.expectEqualStrings("a", edge.source);
        try std.testing.expectEqualStrings("b", edge.target);
        try std.testing.expectEqual(i, edge.occurrence);
    }
    try std.testing.expectEqual(null, try incoming.next());
    var topology = try Cursor.topology(alloc, backing.store(), root, "link");
    defer topology.deinit();
    try std.testing.expectEqual(0, (try topology.next()).?.occurrence);
    try std.testing.expectEqual(1, (try topology.next()).?.occurrence);
    try std.testing.expectEqual(null, try topology.next()); // qualified edge is not local topology
    var remove_doc = try plan(alloc, backing.store(), root, &.{.{ .id = "b", .edges = null }});
    defer remove_doc.deinit();
    const implicit = try remove_doc.publish(backing.store(), root);
    try std.testing.expectEqual(3, implicit.nodes); // a -> b keeps b alive
    var remove_source = try plan(alloc, backing.store(), implicit, &.{.{ .id = "a", .edges = null }});
    defer remove_source.deinit();
    const removed = try remove_source.publish(backing.store(), implicit);
    try std.testing.expectEqual(1, removed.nodes);
    try std.testing.expectEqual(0, removed.edges);
    try std.testing.expect(!try containsNode(alloc, backing.store(), removed, "b"));
    try std.testing.expect(try containsNode(alloc, backing.store(), removed, "isolated"));
    try std.testing.expectError(error.GraphChangePlanSourceChanged, remove_source.publish(backing.store(), root));
    try std.testing.expect(try containsNode(alloc, backing.store(), root, "a")); // old root remains pinned
}

test "serverless paged graph plans are permutation invariant and reject uncoalesced sources" {
    const alloc = std.testing.allocator;
    var backing: tree.testing.MemoryStore = .{ .alloc = alloc };
    defer backing.deinit();
    const edges = [_]keys.Edge{
        .{ .source = "a", .target = "b", .kind = "link", .weight = 2 },
        .{ .source = "a", .target = "b", .kind = "link", .weight = 1 },
        .{ .source = "a", .target = "b", .kind = "link", .weight = 2 },
    };
    var first = try plan(alloc, backing.store(), .{}, &.{.{ .id = "a", .edges = &edges }});
    defer first.deinit();
    const root = try first.publish(backing.store(), .{});
    var reordered = [_]keys.Edge{ edges[2], edges[0], edges[1] };
    var second = try plan(alloc, backing.store(), root, &.{.{ .id = "a", .edges = &reordered }});
    defer second.deinit();
    const writes = backing.writes;
    try std.testing.expect(root.eql(try second.publish(backing.store(), root)));
    try std.testing.expectEqual(writes, backing.writes);
    try std.testing.expectError(error.UncoalescedGraphReplacements, plan(alloc, backing.store(), root, &.{
        .{ .id = "a", .edges = &edges }, .{ .id = "a", .edges = null },
    }));
}

fn allocationExercise(alloc: Allocator) !void {
    var backing: tree.testing.MemoryStore = .{ .alloc = std.testing.allocator };
    defer backing.deinit();
    const edges = [_]keys.Edge{.{ .source = "a", .target = "b", .kind = "link" }};
    var first = try plan(alloc, backing.store(), .{}, &.{.{ .id = "a", .edges = &edges }});
    defer first.deinit();
    const root = try first.publish(backing.store(), .{});
    var second = try plan(alloc, backing.store(), root, &.{.{ .id = "a", .edges = null }});
    defer second.deinit();
    const empty = try second.publish(backing.store(), root);
    try std.testing.expectEqual(null, empty.page);
    try std.testing.expectEqual(0, empty.nodes);
    try std.testing.expectEqual(0, empty.edges);
    var cursor = try Cursor.topology(alloc, backing.store(), root, "link");
    defer cursor.deinit();
    try std.testing.expect(try cursor.next() != null);
}

test "serverless paged graph planning allocation failure preserves all ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
}
