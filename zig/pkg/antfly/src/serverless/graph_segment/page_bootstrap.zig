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

//! Admitted external sorting for initial graph publication. Scratch runs use
//! the same fenced upload scope as final pages, so abandoned/intermediate runs
//! are discoverable by ordinary upload inventory GC. No graph-wide key or node
//! hash map is retained; source documents may arrive in any order.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tree = @import("page_tree.zig");
const graph = @import("page_graph.zig");
const keys = @import("page_keys.zig");

pub const Options = struct { max_run_bytes: usize = 4 * 1024 * 1024 };
const fan_in = 8;
const max_levels = 32;
const source_tag: u8 = 8;

pub fn buildFromSource(alloc: Allocator, store: tree.Store, source: anytype, options: Options) !graph.Root {
    if (options.max_run_bytes == 0) return error.InvalidGraphBootstrapOptions;
    try store.check(store.ptr);
    var sorter = Sorter{ .alloc = alloc, .store = store, .options = options };
    defer sorter.deinit();
    while (try source.next()) |replacement| {
        try store.check(store.ptr);
        // Auxiliary source keys catch duplicate replacements after sorting,
        // including deleted sources, without a namespace-sized seen-ID map.
        if (replacement.id.len == 0 or replacement.id.len >= tree.max_key_bytes) return error.InvalidGraphPageKey;
        var marker: keys.Key = .{};
        errdefer marker.deinit(alloc);
        try marker.bytes.append(alloc, source_tag);
        try marker.bytes.appendSlice(alloc, replacement.id);
        try sorter.add(&marker);
        const edges = replacement.edges orelse continue;
        var member = try keys.Key.adjacency(alloc, replacement.id, .member, null);
        defer member.deinit(alloc);
        try sorter.add(&member);

        // At most one decoded document's degree is retained. Number exact
        // duplicates after canonical sorting, matching the incremental plan.
        var canonical = std.ArrayListUnmanaged(keys.Key).empty;
        defer {
            for (canonical.items) |*key| key.deinit(alloc);
            canonical.deinit(alloc);
        }
        try canonical.ensureTotalCapacity(alloc, edges.len);
        for (edges) |edge| {
            if (!std.mem.eql(u8, edge.source, replacement.id)) return error.InvalidGraphReplacementSource;
            var normalized = edge;
            normalized.occurrence = 0;
            canonical.appendAssumeCapacity(try keys.Key.edge(alloc, normalized, .outgoing));
        }
        std.mem.sort(keys.Key, canonical.items, {}, lessKey);
        var scratch = std.ArrayListUnmanaged(u8).empty;
        defer scratch.deinit(alloc);
        var occurrence: u32 = 0;
        for (canonical.items, 0..) |key, i| {
            if (i != 0 and std.mem.eql(u8, canonical.items[i - 1].bytes.items[0 .. canonical.items[i - 1].bytes.items.len - 4], key.bytes.items[0 .. key.bytes.items.len - 4])) {
                occurrence = std.math.add(u32, occurrence, 1) catch return error.GraphSegmentTooLarge;
            } else occurrence = 0;
            try scratch.ensureTotalCapacity(alloc, key.bytes.items.len);
            var edge = (try keys.decode(key.bytes.items, scratch.allocatedSlice())).edge.?;
            edge.occurrence = occurrence;
            var outgoing = try keys.Key.edge(alloc, edge, .outgoing);
            defer outgoing.deinit(alloc);
            try sorter.add(&outgoing);
            if (edge.table == null) {
                var incoming = try keys.Key.edge(alloc, edge, .incoming);
                defer incoming.deinit(alloc);
                try sorter.add(&incoming);
                var topology = try keys.Key.topologyEdge(alloc, edge);
                defer topology.deinit(alloc);
                try sorter.add(&topology);
            }
        }
    }
    try sorter.flush();
    const run = try sorter.finish();
    if (run == null) return .{ .domain = store.domain };
    var cursor = try tree.Cursor.init(alloc, store, run, "", null);
    defer cursor.deinit();
    var final = FinalSource{ .alloc = alloc, .cursor = &cursor };
    defer final.deinit();
    const page = try tree.buildSorted(alloc, store, &final);
    return .{ .domain = store.domain, .page = page, .nodes = final.nodes, .edges = final.edges };
}

fn lessKey(_: void, a: keys.Key, b: keys.Key) bool {
    return std.mem.order(u8, a.bytes.items, b.bytes.items) == .lt;
}

const Sorter = struct {
    alloc: Allocator,
    store: tree.Store,
    options: Options,
    buffered: std.ArrayListUnmanaged(keys.Key) = .empty,
    buffered_bytes: usize = 0,
    runs: [max_levels][fan_in]tree.Ref = undefined,
    counts: [max_levels]usize = @splat(0),

    fn deinit(self: *@This()) void {
        for (self.buffered.items) |*key| key.deinit(self.alloc);
        self.buffered.deinit(self.alloc);
    }
    fn add(self: *@This(), key: *keys.Key) !void {
        try self.store.check(self.store.ptr);
        const retained = try std.math.add(usize, key.bytes.capacity, @sizeOf(keys.Key));
        if (self.buffered.items.len != 0 and retained > self.options.max_run_bytes -| self.buffered_bytes) try self.flush();
        try self.buffered.append(self.alloc, key.*);
        key.* = .{};
        self.buffered_bytes = try std.math.add(usize, self.buffered_bytes, retained);
    }
    fn flush(self: *@This()) !void {
        if (self.buffered.items.len == 0) return;
        std.mem.sort(keys.Key, self.buffered.items, {}, lessKey);
        for (self.buffered.items[1..], 1..) |key, i| {
            if (std.mem.eql(u8, key.bytes.items, self.buffered.items[i - 1].bytes.items)) return duplicateError(key.bytes.items);
        }
        const Source = struct {
            items: []const keys.Key,
            index: usize = 0,
            pub fn next(s: *@This()) !?tree.Cursor.Record {
                if (s.index == s.items.len) return null;
                defer s.index += 1;
                return .{ .key = s.items[s.index].bytes.items, .value = "" };
            }
        };
        var source = Source{ .items = self.buffered.items };
        const root = (try tree.buildSorted(self.alloc, self.store, &source)).?;
        for (self.buffered.items) |*key| key.deinit(self.alloc);
        self.buffered.clearRetainingCapacity();
        self.buffered_bytes = 0;
        try self.addRun(root, 0);
    }
    fn addRun(self: *@This(), root: tree.Ref, level: usize) !void {
        if (level == max_levels) return error.GraphSegmentTooLarge;
        self.runs[level][self.counts[level]] = root;
        self.counts[level] += 1;
        if (self.counts[level] == fan_in) {
            const merged = try merge(self.alloc, self.store, &self.runs[level]);
            self.counts[level] = 0;
            try self.addRun(merged, level + 1);
        }
    }
    fn finish(self: *@This()) !?tree.Ref {
        var merged: ?tree.Ref = null;
        for (0..max_levels) |level| {
            var inputs: [fan_in]tree.Ref = undefined;
            var count: usize = 0;
            if (merged) |prior| {
                inputs[0] = prior;
                count = 1;
            }
            @memcpy(inputs[count .. count + self.counts[level]], self.runs[level][0..self.counts[level]]);
            count += self.counts[level];
            merged = if (count == 0) null else if (count == 1) inputs[0] else try merge(self.alloc, self.store, inputs[0..count]);
        }
        return merged;
    }
};

fn duplicateError(key: []const u8) anyerror {
    return if (key.len > 0 and key[0] == source_tag) error.UncoalescedGraphReplacements else error.InvalidGraphPageKey;
}

fn merge(alloc: Allocator, store: tree.Store, runs: []const tree.Ref) !tree.Ref {
    var source = MergeSource{ .alloc = alloc, .len = runs.len };
    defer source.deinit();
    for (runs, 0..) |run, i| {
        source.cursors[i] = try tree.Cursor.init(alloc, store, run, "", null);
        source.initialized += 1;
        source.heads[i] = try source.cursors[i].next();
    }
    return (try tree.buildSorted(alloc, store, &source)).?;
}

const MergeSource = struct {
    alloc: Allocator,
    len: usize,
    initialized: usize = 0,
    cursors: [fan_in]tree.Cursor = undefined,
    heads: [fan_in]?tree.Cursor.Record = @splat(null),
    previous: ?usize = null,
    fn deinit(self: *@This()) void {
        for (self.cursors[0..self.initialized]) |*cursor| cursor.deinit();
    }
    pub fn next(self: *@This()) !?tree.Cursor.Record {
        if (self.previous) |index| self.heads[index] = try self.cursors[index].next();
        var selected: ?usize = null;
        for (self.heads[0..self.len], 0..) |head, i| if (head) |candidate| {
            if (selected) |prior| {
                switch (std.mem.order(u8, candidate.key, self.heads[prior].?.key)) {
                    .lt => selected = i,
                    .eq => return duplicateError(candidate.key),
                    .gt => {},
                }
            } else selected = i;
        };
        self.previous = selected;
        return if (selected) |index| self.heads[index] else null;
    }
};

const FinalSource = struct {
    alloc: Allocator,
    cursor: *tree.Cursor,
    nodes: u64 = 0,
    edges: u64 = 0,
    prior_node: std.ArrayListUnmanaged(u8) = .empty,
    scratch: std.ArrayListUnmanaged(u8) = .empty,
    fn deinit(self: *@This()) void {
        self.prior_node.deinit(self.alloc);
        self.scratch.deinit(self.alloc);
    }
    pub fn next(self: *@This()) !?tree.Cursor.Record {
        while (try self.cursor.next()) |record| {
            if (record.key[0] == source_tag) continue;
            try self.scratch.ensureTotalCapacity(self.alloc, record.key.len);
            const decoded = try keys.decode(record.key, self.scratch.allocatedSlice());
            if (!decoded.topology) {
                if (!std.mem.eql(u8, self.prior_node.items, decoded.node)) {
                    self.nodes = try std.math.add(u64, self.nodes, 1);
                    self.prior_node.clearRetainingCapacity();
                    try self.prior_node.appendSlice(self.alloc, decoded.node);
                }
                if (decoded.direction == .outgoing) self.edges = try std.math.add(u64, self.edges, 1);
            }
            return record;
        }
        return null;
    }
};

test "serverless external graph bootstrap agrees with incremental oracle across bounded spills" {
    const a = std.testing.allocator;
    var memory = tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const edges = [_]keys.Edge{
        .{ .source = "a", .target = "implicit", .kind = "link" },
        .{ .source = "a", .target = "implicit", .kind = "link" },
        .{ .source = "a", .target = "outside", .kind = "link", .table = "remote" },
        .{ .source = "a", .target = "a", .kind = "self" },
    };
    const replacements = [_]graph.Replacement{ .{ .id = "z", .edges = &.{} }, .{ .id = "a", .edges = &edges }, .{ .id = "deleted", .edges = null } };
    const Source = struct {
        items: []const graph.Replacement,
        index: usize = 0,
        pub fn next(self: *@This()) !?graph.Replacement {
            if (self.index == self.items.len) return null;
            defer self.index += 1;
            return self.items[self.index];
        }
    };
    var oracle_source = Source{ .items = &replacements };
    var plan = try graph.planFromSource(a, memory.store(), .{}, &oracle_source);
    defer plan.deinit();
    const oracle = try plan.publish(memory.store(), .{});
    for ([_]usize{ 1, 128, 4096 }) |limit| {
        var source = Source{ .items = &replacements };
        const root = try buildFromSource(a, memory.store(), &source, .{ .max_run_bytes = limit });
        try std.testing.expectEqual(oracle.nodes, root.nodes);
        try std.testing.expectEqual(oracle.edges, root.edges);
        var expected = try tree.Cursor.init(a, memory.store(), oracle.page, "", null);
        defer expected.deinit();
        var actual = try tree.Cursor.init(a, memory.store(), root.page, "", null);
        defer actual.deinit();
        while (try expected.next()) |record| {
            try std.testing.expectEqualStrings(record.key, (try actual.next()).?.key);
        }
        try std.testing.expect((try actual.next()) == null);
    }
    var duplicate = Source{ .items = &.{ replacements[0], replacements[0] } };
    try std.testing.expectError(error.UncoalescedGraphReplacements, buildFromSource(a, memory.store(), &duplicate, .{ .max_run_bytes = 1 }));
}

test "serverless external graph bootstrap unwinds every scratch allocation and checks cancellation" {
    const a = std.testing.allocator;
    const Exercise = struct {
        fn run(alloc: Allocator) !void {
            var memory = tree.testing.MemoryStore{ .alloc = std.testing.allocator };
            defer memory.deinit();
            const Source = struct {
                index: usize = 0,
                id: [1]u8 = undefined,
                edges: [1]keys.Edge = undefined,
                pub fn next(self: *@This()) !?graph.Replacement {
                    if (self.index == 3) return null;
                    self.id[0] = @intCast('a' + self.index);
                    self.index += 1;
                    self.edges[0] = .{ .source = &self.id, .target = "target", .kind = "link" };
                    return .{ .id = &self.id, .edges = &self.edges };
                }
            };
            var source = Source{};
            const root = try buildFromSource(alloc, memory.store(), &source, .{ .max_run_bytes = 128 });
            try std.testing.expectEqual(@as(u64, 4), root.nodes);
            try std.testing.expectEqual(@as(u64, 3), root.edges);
            memory.canceled = true;
            var canceled = Source{};
            try std.testing.expectError(error.Canceled, buildFromSource(alloc, memory.store(), &canceled, .{}));
            try std.testing.expectEqual(@as(usize, 0), canceled.index);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{});
}
