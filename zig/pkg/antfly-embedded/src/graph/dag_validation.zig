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

//! Bounded validation of a final visible-edge delta over an accepted DAG.
//! This kernel is not a commit authority. Its caller must serialize the immutable
//! (index incarnation, edge type) domain and fence its accepted revision through
//! commit. An asynchronous graph query projection cannot supply this proof.
const std = @import("std");
const A = std.mem.Allocator;

pub const Change = struct {
    source: []const u8,
    target: []const u8,
    /// Final membership AFTER complete contribution replacement and precedence.
    /// Removing one owner while another survives must not produce false.
    present: bool,
};
pub const Row = struct {
    /// Strictly increasing opaque storage key within this source's adjacency.
    key: []const u8,
    target: []const u8,
};
pub const Limits = struct {
    nodes: usize = 100_000,
    edges: usize = 1_000_000,
    bytes: usize = 64 * 1024 * 1024,
    requests: usize = 100_000,
    memory: usize = 64 * 1024 * 1024,
    deadline_ns: u64 = std.math.maxInt(u64),
};
pub const Dimension = enum { nodes, edges, bytes, requests, memory, time };
pub const Stats = struct {
    nodes: usize = 0,
    edges: usize = 0,
    bytes: usize = 0,
    requests: usize = 0,
    peak_memory: usize = 0,
};
pub const Clock = struct {
    context: *anyopaque,
    now: *const fn (*anyopaque) u64,
};
pub const Budget = struct {
    limits: Limits = .{},
    stats: Stats = .{},
    exhausted: ?Dimension = null,
    clock: Clock,

    pub fn checkpoint(self: *Budget) !void {
        if (self.clock.now(self.clock.context) >= self.limits.deadline_ns) return self.fail(.time);
    }
    pub fn consume(self: *Budget, comptime dimension: Dimension, count: usize) !void {
        try self.checkpoint();
        const used = &@field(self.stats, @tagName(dimension));
        if (count > @field(self.limits, @tagName(dimension)) -| used.*) return self.fail(dimension);
        used.* += count;
    }
    fn fail(self: *Budget, dimension: Dimension) error{GraphConstraintCapacity} {
        self.exhausted = dimension;
        return error.GraphConstraintCapacity;
    }
};

/// Fixed storage outlives the validation allocator. Overlong witnesses are
/// omitted in full. Call disclose before rendering identifiers to a caller.
pub const Witness = struct {
    const max_nodes = 64;
    const max_bytes = 4096;
    data: [max_bytes]u8 = @splat(0),
    ends: [max_nodes]u16 = @splat(0),
    count: usize = 0,

    pub fn clear(self: *Witness) void {
        @memset(&self.data, 0);
        @memset(&self.ends, 0);
        self.count = 0;
    }
    pub fn node(self: *const Witness, index: usize) []const u8 {
        std.debug.assert(index < self.count);
        const start: usize = if (index == 0) 0 else self.ends[index - 1];
        return self.data[start..self.ends[index]];
    }
    pub fn disclose(self: *Witness, context: anytype, can_read: anytype) void {
        for (0..self.count) |i| if (!can_read(context, self.node(i))) {
            self.clear();
            return;
        };
    }
    fn append(self: *Witness, value: []const u8) bool {
        const start: usize = if (self.count == 0) 0 else self.ends[self.count - 1];
        if (self.count == max_nodes or value.len > max_bytes - start) {
            self.clear();
            return false;
        }
        @memcpy(self.data[start..][0..value.len], value);
        self.ends[self.count] = @intCast(start + value.len);
        self.count += 1;
        return true;
    }
};

/// Bounds actual allocations, including map capacity, reader scratch overhead,
/// buffers and realloc overlap. Parent allocation failure remains OutOfMemory.
pub const Memory = struct {
    parent: A,
    budget: *Budget,
    used: usize = 0,
    denied: bool = false,
    pub fn allocator(self: *Memory) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
    }
    fn reserve(self: *Memory, count: usize) bool {
        self.denied = count > self.budget.limits.memory -| self.used;
        return !self.denied;
    }
    fn account(self: *Memory, count: usize) void {
        self.used += count;
        self.budget.stats.peak_memory = @max(self.budget.stats.peak_memory, self.used);
    }
    fn allocate(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Memory = @ptrCast(@alignCast(ctx));
        if (!self.reserve(len)) return null;
        const result = self.parent.rawAlloc(len, alignment, ra) orelse return null;
        self.account(len);
        return result;
    }
    fn resize(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *Memory = @ptrCast(@alignCast(ctx));
        if (!self.reserve(len -| buf.len)) return false;
        if (!self.parent.rawResize(buf, alignment, len, ra)) return false;
        self.used -= buf.len;
        self.account(len);
        return true;
    }
    fn remap(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *Memory = @ptrCast(@alignCast(ctx));
        if (!self.reserve(len -| buf.len)) return null;
        const result = self.parent.rawRemap(buf, alignment, len, ra) orelse return null;
        self.used -= buf.len;
        self.account(len);
        return result;
    }
    fn free(ctx: *anyopaque, buf: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Memory = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(buf, alignment, ra);
        self.used -= buf.len;
    }
};
const Overlay = struct {
    targets: std.StringHashMapUnmanaged(bool) = .empty,
    additions: std.ArrayList([]const u8) = .empty,
};
const Node = struct { key: []const u8, color: enum { visiting, done } = .visiting };
const Frame = struct {
    node: usize,
    after: ?[]const u8 = null,
    base_done: bool = false,
    overlay_offset: usize = 0,
};

/// Reader.next(alloc, source, after, budget) returns ?Row at one pinned accepted
/// revision. Buffers belong to alloc or remain borrowed until the next call.
/// The reader must bound response bytes before retaining responses, respect the
/// deadline during blocking I/O, and charge actual storage/network requests to
/// budget. Cached adjacency advances do not consume the physical request budget.
/// The supplied allocator is capped and reset per read; readers need not free it.
///
/// Iterative three-color DFS shares completed nodes across additions. Every
/// reachable adjacency is scanned once, regardless of batch width. Previously
/// accepted state must have an acyclicity certificate. Activation must use
/// validateExisting with a COMPLETE enumeration of existing source nodes.
pub fn validate(alloc: A, reader: anytype, changes: []const Change, budget: *Budget, witness: *Witness) !void {
    return run(alloc, reader, changes, &.{}, budget, witness);
}
pub fn validateExisting(alloc: A, reader: anytype, roots: []const []const u8, changes: []const Change, budget: *Budget, witness: *Witness) !void {
    return run(alloc, reader, changes, roots, budget, witness);
}
fn run(alloc: A, reader: anytype, changes: []const Change, roots: []const []const u8, budget: *Budget, witness: *Witness) !void {
    witness.clear();
    var memory: Memory = .{ .parent = alloc, .budget = budget };
    defer std.debug.assert(memory.used == 0);
    validateInner(memory.allocator(), reader, changes, roots, budget, witness) catch |err| {
        if (err == error.GraphCycle) return err;
        witness.clear();
        if (err == error.OutOfMemory and memory.denied) return budget.fail(.memory);
        return err;
    };
}
fn validateInner(alloc: A, reader: anytype, changes: []const Change, roots: []const []const u8, budget: *Budget, witness: *Witness) !void {
    try budget.checkpoint();
    var sources: std.StringHashMapUnmanaged(usize) = .empty;
    defer sources.deinit(alloc);
    var overlays: std.ArrayList(Overlay) = .empty;
    defer {
        for (overlays.items) |*overlay| {
            overlay.targets.deinit(alloc);
            overlay.additions.deinit(alloc);
        }
        overlays.deinit(alloc);
    }
    for (changes) |change| {
        try budget.consume(.edges, 1);
        try budget.consume(.bytes, change.source.len);
        try budget.consume(.bytes, change.target.len);
        const result = try sources.getOrPut(alloc, change.source);
        if (!result.found_existing) {
            result.value_ptr.* = overlays.items.len;
            try overlays.append(alloc, .{});
        }
        const overlay = &overlays.items[result.value_ptr.*];
        const target = try overlay.targets.getOrPut(alloc, change.target);
        if (target.found_existing) {
            if (target.value_ptr.* != change.present) return error.InvalidDagMutation;
            continue;
        }
        target.value_ptr.* = change.present;
        if (change.present) try overlay.additions.append(alloc, change.target);
    }
    var nodes: std.ArrayList(Node) = .empty;
    defer {
        for (nodes.items) |node_value| alloc.free(node_value.key);
        nodes.deinit(alloc);
    }
    var visited: std.StringHashMapUnmanaged(usize) = .empty;
    defer visited.deinit(alloc);
    var stack: std.ArrayList(Frame) = .empty;
    defer {
        for (stack.items) |frame| if (frame.after) |after| alloc.free(after);
        stack.deinit(alloc);
    }
    for (0..roots.len + changes.len) |root_index| {
        try budget.checkpoint();
        const root = if (root_index < roots.len) roots[root_index] else blk: {
            const change = changes[root_index - roots.len];
            if (!change.present) continue;
            break :blk change.source;
        };
        if (visited.contains(root)) continue;
        try push(alloc, root, &nodes, &visited, &stack, budget);
        while (stack.items.len != 0) {
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            try budget.checkpoint();
            const frame = &stack.items[stack.items.len - 1];
            const source = nodes.items[frame.node].key;
            const overlay: ?*Overlay = if (sources.get(source)) |index| &overlays.items[index] else null;
            const next: ?[]const u8 = blk: {
                while (!frame.base_done) {
                    _ = scratch.reset(.free_all);
                    const result = try reader.next(scratch.allocator(), source, frame.after, budget);
                    try budget.checkpoint();
                    const row = result orelse {
                        frame.base_done = true;
                        break;
                    };
                    try budget.consume(.edges, 1);
                    try budget.consume(.bytes, row.key.len);
                    try budget.consume(.bytes, row.target.len);
                    if (frame.after) |after| if (std.mem.order(u8, after, row.key) != .lt) return error.GraphConstraintNotReady;
                    const overridden = if (overlay) |o| o.targets.contains(row.target) else false;
                    // Copy before releasing the old cursor: a reader may borrow
                    // that input buffer for its returned target.
                    const aliases_cursor = if (frame.after) |after| @intFromPtr(row.target.ptr) >= @intFromPtr(after.ptr) and @intFromPtr(row.target.ptr) - @intFromPtr(after.ptr) < after.len else false;
                    const target = if (overridden) null else if (aliases_cursor) try scratch.allocator().dupe(u8, row.target) else row.target;
                    const next_cursor = try alloc.dupe(u8, row.key);
                    if (frame.after) |after| alloc.free(after);
                    frame.after = next_cursor;
                    if (overridden) continue;
                    break :blk target;
                }
                if (overlay) |o| {
                    if (frame.overlay_offset < o.additions.items.len) {
                        const target = o.additions.items[frame.overlay_offset];
                        frame.overlay_offset += 1;
                        break :blk target;
                    }
                }
                break :blk null;
            };
            if (next) |target| {
                if (visited.get(target)) |id| {
                    if (nodes.items[id].color == .visiting) {
                        var start: usize = 0;
                        while (stack.items[start].node != id) : (start += 1) {}
                        for (stack.items[start..]) |item| if (!witness.append(nodes.items[item.node].key)) return error.GraphCycle;
                        _ = witness.append(nodes.items[id].key);
                        return error.GraphCycle;
                    }
                } else try push(alloc, target, &nodes, &visited, &stack, budget);
            } else {
                nodes.items[frame.node].color = .done;
                if (frame.after) |after| alloc.free(after);
                _ = stack.pop();
            }
        }
    }
}
fn push(alloc: A, key: []const u8, nodes: *std.ArrayList(Node), visited: *std.StringHashMapUnmanaged(usize), stack: *std.ArrayList(Frame), budget: *Budget) !void {
    try budget.consume(.nodes, 1);
    try nodes.ensureUnusedCapacity(alloc, 1);
    try stack.ensureUnusedCapacity(alloc, 1);
    try visited.ensureUnusedCapacity(alloc, 1);
    const owned = try alloc.dupe(u8, key);
    const id = nodes.items.len;
    visited.putAssumeCapacity(owned, id);
    nodes.appendAssumeCapacity(.{ .key = owned });
    stack.appendAssumeCapacity(.{ .node = id });
}

const TestClock = struct {
    time: u64 = 0,
    fn now(raw: *anyopaque) u64 {
        const self: *TestClock = @ptrCast(@alignCast(raw));
        return self.time;
    }
    fn budget(self: *TestClock) Budget {
        return .{ .clock = .{ .context = self, .now = now } };
    }
};
const TestEdge = struct { source: []const u8, target: []const u8 };
const TestReader = struct {
    edges: []const TestEdge = &.{},
    fn next(self: @This(), _: A, source: []const u8, after: ?[]const u8, budget: *Budget) !?Row {
        try budget.consume(.requests, 1);
        var best: ?[]const u8 = null;
        for (self.edges) |edge| {
            if (!std.mem.eql(u8, source, edge.source)) continue;
            if (after) |cursor| if (std.mem.order(u8, cursor, edge.target) != .lt) continue;
            if (best) |previous| if (std.mem.order(u8, previous, edge.target) != .gt) continue;
            best = edge.target;
        }
        return if (best) |target| .{ .key = target, .target = target } else null;
    }
};

test "DAG validation allows multiple parents and rejects self loops and indirect cycles" {
    var clock: TestClock = .{};
    var witness: Witness = .{};
    var budget = clock.budget();
    const reader: TestReader = .{ .edges = &.{ .{ .source = "a", .target = "b" }, .{ .source = "b", .target = "c" } } };
    try validate(std.testing.allocator, reader, &.{.{ .source = "a", .target = "c", .present = true }}, &budget, &witness);
    budget = clock.budget();
    try std.testing.expectError(error.GraphCycle, validate(std.testing.allocator, reader, &.{.{ .source = "a", .target = "a", .present = true }}, &budget, &witness));
    try std.testing.expectEqual(@as(usize, 2), witness.count);
    try std.testing.expectEqualStrings("a", witness.node(0));
    try std.testing.expectEqualStrings("a", witness.node(1));
    budget = clock.budget();
    try std.testing.expectError(error.GraphCycle, validate(std.testing.allocator, reader, &.{.{ .source = "c", .target = "a", .present = true }}, &budget, &witness));
    try std.testing.expectEqual(@as(usize, 4), witness.count);
    try std.testing.expectEqualStrings(witness.node(0), witness.node(witness.count - 1));
}

test "DAG validation checks atomic replacement final state regardless of mutation order" {
    var clock: TestClock = .{};
    var witness: Witness = .{};
    const reader: TestReader = .{ .edges = &.{.{ .source = "a", .target = "b" }} };
    const changes = [_]Change{ .{ .source = "b", .target = "a", .present = true }, .{ .source = "a", .target = "b", .present = false } };
    var budget = clock.budget();
    try validate(std.testing.allocator, reader, &changes, &budget, &witness);
    budget = clock.budget();
    try validate(std.testing.allocator, reader, &.{ changes[1], changes[0] }, &budget, &witness);
    // A surviving contribution means a->b is STILL present in the visible set.
    budget = clock.budget();
    try std.testing.expectError(error.GraphCycle, validate(std.testing.allocator, reader, changes[0..1], &budget, &witness));
    budget = clock.budget();
    try std.testing.expectError(error.InvalidDagMutation, validate(std.testing.allocator, reader, &.{ changes[0], .{ .source = "b", .target = "a", .present = false } }, &budget, &witness));
}

test "DAG validation finds cycles made entirely of candidates and activation finds disconnected existing cycles" {
    var clock: TestClock = .{};
    var budget = clock.budget();
    var witness: Witness = .{};
    const additions = [_]Change{ .{ .source = "a", .target = "b", .present = true }, .{ .source = "b", .target = "a", .present = true } };
    try std.testing.expectError(error.GraphCycle, validate(std.testing.allocator, TestReader{}, &additions, &budget, &witness));
    const reader: TestReader = .{ .edges = &.{ .{ .source = "hidden-a", .target = "hidden-b" }, .{ .source = "hidden-b", .target = "hidden-a" } } };
    budget = clock.budget();
    try std.testing.expectError(error.GraphCycle, validateExisting(std.testing.allocator, reader, &.{ "unconnected", "hidden-a" }, &.{}, &budget, &witness));
}

test "DAG validation every budget fails closed with a typed dimension and no witness" {
    var clock: TestClock = .{};
    const additions = [_]Change{.{ .source = "a", .target = "b", .present = true }};
    inline for (.{ Dimension.nodes, .edges, .bytes, .requests, .memory, .time }) |dimension| {
        var budget = clock.budget();
        if (dimension == .time) budget.limits.deadline_ns = 0 else @field(budget.limits, @tagName(dimension)) = 0;
        var witness: Witness = .{};
        try std.testing.expect(witness.append("old diagnostic"));
        try std.testing.expectError(error.GraphConstraintCapacity, validate(std.testing.allocator, TestReader{}, &additions, &budget, &witness));
        try std.testing.expectEqual(dimension, budget.exhausted.?);
        try std.testing.expectEqual(@as(usize, 0), witness.count);
    }
}

test "DAG validation all-or-nothing witness authorization and bounded diagnostics" {
    var clock: TestClock = .{};
    var budget = clock.budget();
    var witness: Witness = .{};
    const reader: TestReader = .{ .edges = &.{.{ .source = "private", .target = "public" }} };
    try std.testing.expectError(error.GraphCycle, validate(std.testing.allocator, reader, &.{.{ .source = "public", .target = "private", .present = true }}, &budget, &witness));
    const Auth = struct {
        fn read(_: void, node_key: []const u8) bool {
            return std.mem.eql(u8, node_key, "public");
        }
    };
    witness.disclose({}, Auth.read);
    try std.testing.expectEqual(@as(usize, 0), witness.count);
    for (0..Witness.max_nodes) |_| try std.testing.expect(witness.append("a"));
    try std.testing.expect(!witness.append("a"));
    try std.testing.expectEqual(@as(usize, 0), witness.count);
    const huge: [Witness.max_bytes + 1]u8 = @splat('a');
    try std.testing.expect(!witness.append(&huge));
    try std.testing.expectEqual(@as(usize, 0), witness.count);
}

test "DAG validation shared descendants scanned once across wide replacements" {
    var clock: TestClock = .{};
    var budget = clock.budget();
    var witness: Witness = .{};
    const reader: TestReader = .{ .edges = &.{ .{ .source = "b", .target = "d" }, .{ .source = "c", .target = "d" }, .{ .source = "d", .target = "e" } } };
    try validate(std.testing.allocator, reader, &.{ .{ .source = "a", .target = "b", .present = true }, .{ .source = "a", .target = "c", .present = true } }, &budget, &witness);
    try std.testing.expectEqual(@as(usize, 5), budget.stats.nodes);
    try std.testing.expectEqual(@as(usize, 8), budget.stats.requests); // 3 rows + 5 exhausted adjacencies
    try std.testing.expectEqual(@as(usize, 5), budget.stats.edges); // 2 candidate + 3 accepted
}

test "DAG validation allocation failure cleanup preserves OutOfMemory" {
    const Case = struct {
        fn run(alloc: A) !void {
            var clock: TestClock = .{};
            var budget = clock.budget();
            var witness: Witness = .{};
            const reader: TestReader = .{ .edges = &.{.{ .source = "b", .target = "c" }} };
            try validate(alloc, reader, &.{.{ .source = "a", .target = "b", .present = true }}, &budget, &witness);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "DAG validation malformed cursor and reader failures cannot certify validity" {
    const Reader = struct {
        fn next(_: @This(), _: A, source: []const u8, _: ?[]const u8, _: *Budget) !?Row {
            if (std.mem.eql(u8, source, "leaf")) return null;
            return .{ .key = "same", .target = "leaf" };
        }
    };
    var clock: TestClock = .{};
    var budget = clock.budget();
    var witness: Witness = .{};
    try std.testing.expectError(error.GraphConstraintNotReady, validate(std.testing.allocator, Reader{}, &.{.{ .source = "a", .target = "b", .present = true }}, &budget, &witness));
    const Unavailable = struct {
        fn next(_: @This(), _: A, _: []const u8, _: ?[]const u8, _: *Budget) !?Row {
            return error.GraphConstraintNotReady;
        }
    };
    budget = clock.budget();
    try std.testing.expectError(error.GraphConstraintNotReady, validate(std.testing.allocator, Unavailable{}, &.{.{ .source = "a", .target = "b", .present = true }}, &budget, &witness));
    try std.testing.expectEqual(@as(usize, 0), witness.count);
}

// Independent Kahn oracle: no reuse of DFS, reader, or overlay implementation.
fn oracleCyclic(comptime n: usize, matrix: [n][n]bool) bool {
    var degree: [n]usize = @splat(0);
    for (matrix) |row| for (row, 0..) |present, target| {
        if (present) degree[target] += 1;
    };
    var removed: [n]bool = @splat(false);
    var count: usize = 0;
    while (true) {
        var source: ?usize = null;
        for (0..n) |i| if (!removed[i] and degree[i] == 0) {
            source = i;
            break;
        };
        const node_id = source orelse break;
        removed[node_id] = true;
        count += 1;
        for (matrix[node_id], 0..) |present, target| {
            if (present) degree[target] -= 1;
        }
    }
    return count != n;
}
fn matrixFromMask(comptime n: usize, mask: usize) [n][n]bool {
    var matrix: [n][n]bool = @splat(@splat(false));
    var bit: std.math.Log2Int(usize) = 0;
    for (0..n) |source| for (0..n) |target| {
        if (source == target) continue;
        matrix[source][target] = mask & (@as(usize, 1) << bit) != 0;
        bit += 1;
    };
    return matrix;
}
test "DAG validation exhaustive final-state oracle for every three-node replacement" {
    const names = [_][]const u8{ "a", "b", "c" };
    for (0..64) |before_mask| {
        const before = matrixFromMask(3, before_mask);
        if (oracleCyclic(3, before)) continue;
        var edges: std.ArrayList(TestEdge) = .empty;
        defer edges.deinit(std.testing.allocator);
        for (0..3) |source| for (0..3) |target| {
            if (before[source][target]) try edges.append(std.testing.allocator, .{ .source = names[source], .target = names[target] });
        };
        for (0..64) |after_mask| {
            const after = matrixFromMask(3, after_mask);
            var changes: std.ArrayList(Change) = .empty;
            defer changes.deinit(std.testing.allocator);
            for (0..3) |source| for (0..3) |target| {
                if (before[source][target] != after[source][target]) try changes.append(std.testing.allocator, .{ .source = names[source], .target = names[target], .present = after[source][target] });
            };
            var clock: TestClock = .{};
            var budget = clock.budget();
            var witness: Witness = .{};
            const reader: TestReader = .{ .edges = edges.items };
            if (oracleCyclic(3, after)) {
                try std.testing.expectError(error.GraphCycle, validate(std.testing.allocator, reader, changes.items, &budget, &witness));
                try std.testing.expect(witness.count >= 3);
                // Every returned witness hop exists in the final graph.
                for (0..witness.count - 1) |i| {
                    const source = witness.node(i)[0] - 'a';
                    const target = witness.node(i + 1)[0] - 'a';
                    try std.testing.expect(after[source][target]);
                }
            } else try validate(std.testing.allocator, reader, changes.items, &budget, &witness);
        }
    }
}

test "DAG validation exhaustive existing-state oracle for all four-node graphs" {
    const names = [_][]const u8{ "a", "b", "c", "d" };
    for (0..4096) |mask| {
        const matrix = matrixFromMask(4, mask);
        var edges: std.ArrayList(TestEdge) = .empty;
        defer edges.deinit(std.testing.allocator);
        for (0..4) |source| for (0..4) |target| {
            if (matrix[source][target]) try edges.append(std.testing.allocator, .{ .source = names[source], .target = names[target] });
        };
        var clock: TestClock = .{};
        var budget = clock.budget();
        var witness: Witness = .{};
        const reader: TestReader = .{ .edges = edges.items };
        if (oracleCyclic(4, matrix)) {
            try std.testing.expectError(error.GraphCycle, validateExisting(std.testing.allocator, reader, &names, &.{}, &budget, &witness));
        } else try validateExisting(std.testing.allocator, reader, &names, &.{}, &budget, &witness);
    }
}

test "DAG validation deep graphs use bounded iterative state without recursion" {
    const depth = 10_000;
    const Reader = struct {
        fn next(_: @This(), alloc: A, source: []const u8, after: ?[]const u8, budget: *Budget) !?Row {
            try budget.consume(.requests, 1);
            if (after != null) return null;
            const id = std.mem.readInt(u64, source[0..8], .big);
            if (id == depth) return null;
            const target = try alloc.alloc(u8, 8);
            std.mem.writeInt(u64, target[0..8], id + 1, .big);
            return .{ .key = target, .target = target };
        }
    };
    var clock: TestClock = .{};
    var budget = clock.budget();
    var witness: Witness = .{};
    const root: [8]u8 = @splat(0);
    try validateExisting(std.testing.allocator, Reader{}, &.{&root}, &.{}, &budget, &witness);
    try std.testing.expectEqual(@as(usize, depth + 1), budget.stats.nodes);
    try std.testing.expectEqual(@as(usize, depth), budget.stats.edges);
    try std.testing.expectEqual(@as(usize, 2 * depth + 1), budget.stats.requests);
}

test "DAG validation deadline applies after final empty adjacency read" {
    const Reader = struct {
        clock: *TestClock,
        fn next(self: @This(), _: A, _: []const u8, _: ?[]const u8, _: *Budget) !?Row {
            self.clock.time = 100;
            return null;
        }
    };
    var clock: TestClock = .{};
    var budget = clock.budget();
    budget.limits.deadline_ns = 100;
    var witness: Witness = .{};
    try std.testing.expectError(error.GraphConstraintCapacity, validate(std.testing.allocator, Reader{ .clock = &clock }, &.{.{ .source = "a", .target = "b", .present = true }}, &budget, &witness));
    try std.testing.expectEqual(Dimension.time, budget.exhausted.?);
}

test "DAG validation preserves borrowed cursor targets until they are consumed" {
    const Reader = struct {
        fn next(_: @This(), _: A, source: []const u8, after: ?[]const u8, _: *Budget) !?Row {
            if (!std.mem.eql(u8, source, "a")) return null;
            if (after) |cursor| {
                if (std.mem.eql(u8, cursor, "b")) return .{ .key = "c", .target = cursor };
                return null;
            }
            return .{ .key = "b", .target = "leaf" };
        }
    };
    var clock: TestClock = .{};
    var budget = clock.budget();
    var witness: Witness = .{};
    try validateExisting(std.testing.allocator, Reader{}, &.{"a"}, &.{}, &budget, &witness);
    try std.testing.expectEqual(@as(usize, 3), budget.stats.nodes);
}
