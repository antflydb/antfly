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

//! Immutable ordered indexes over the native transactional row store. Roots
//! fork in constant space; updates copy only a logarithmic search path. Values
//! are record references, never source text. The caller atomically publishes a
//! root with its authority revision; staging nodes alone conveys no authority.
const std = @import("std");
const A = std.mem.Allocator;
pub const Root = ?[]const u8;
pub const Store = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, []const u8) anyerror!?[]const u8,
    put: *const fn (*anyopaque, A, []const u8, []const u8) anyerror!void,
};
const Node = struct {
    version: u8 = 1,
    key: []const u8,
    value: []const u8,
    left: Root = null,
    right: Root = null,
    height: u16 = 1,
    size: u64 = 1,
};
pub const Item = struct { key: []const u8, value: []const u8 };
pub const Page = struct { items: []const Item, next: ?[]const u8 = null };
pub const Index = struct {
    a: A,
    store: Store,
    prefix: []const u8,
    nodes: std.StringHashMapUnmanaged(Node) = .empty,
    pending: std.StringHashMapUnmanaged([]const u8) = .empty,
    steps: usize = 0,
    max_steps: usize = 65536,
    const max_height = 128;

    fn step(self: *Index) !void {
        self.steps += 1;
        if (self.steps > self.max_steps) return error.MemoryWorkLimit;
    }
    fn digest(a: A, value: []const u8) ![]const u8 {
        var hash: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(value, &hash, .{});
        return a.dupe(u8, &std.fmt.bytesToHex(hash, .lower));
    }
    fn validRoot(root: []const u8) bool {
        if (root.len != 64) return false;
        for (root) |byte| if (!std.ascii.isHex(byte) or std.ascii.isUpper(byte)) return false;
        return true;
    }
    fn load(self: *Index, root: []const u8) !Node {
        try self.step();
        if (!validRoot(root)) return error.MemoryCorruptState;
        if (self.nodes.get(root)) |node| return node;
        const raw = (try self.store.get(self.store.ptr, self.a, try std.mem.concat(self.a, u8, &.{ self.prefix, root }))) orelse return error.MemoryCorruptState;
        if (raw.len > 16384 or !std.mem.eql(u8, root, try digest(self.a, raw))) return error.MemoryCorruptState;
        const node = try std.json.parseFromSliceLeaky(Node, self.a, raw, .{ .allocate = .alloc_always });
        if (node.version != 1 or node.height == 0 or node.height > max_height or node.size == 0 or node.key.len == 0 or node.key.len > 1024 or node.value.len > 4096) return error.MemoryCorruptState;
        if (node.left) |child| if (!validRoot(child)) return error.MemoryCorruptState;
        if (node.right) |child| if (!validRoot(child)) return error.MemoryCorruptState;
        try self.nodes.put(self.a, try self.a.dupe(u8, root), node);
        return node;
    }
    fn save(self: *Index, input: Node) ![]const u8 {
        var node = input;
        const left = if (node.left) |root| try self.load(root) else null;
        const right = if (node.right) |root| try self.load(root) else null;
        node.height = 1 + @max(if (left) |v| v.height else 0, if (right) |v| v.height else 0);
        if (node.height > max_height) return error.MemoryWorkLimit;
        node.size = try std.math.add(u64, 1, try std.math.add(u64, if (left) |v| v.size else 0, if (right) |v| v.size else 0));
        const raw = try std.json.Stringify.valueAlloc(self.a, node, .{});
        const root = try digest(self.a, raw);
        if (!self.nodes.contains(root)) {
            try self.pending.put(self.a, root, raw);
            try self.nodes.put(self.a, root, node);
        }
        return root;
    }
    /// Publish only transaction-final reachable nodes, children before parents.
    /// Intermediate search paths remain arena-local and create no durable GC debt.
    pub fn publish(self: *Index, root: Root) !void {
        try self.publishDepth(root, 0);
    }
    fn publishDepth(self: *Index, root: Root, depth: usize) anyerror!void {
        const id = root orelse return;
        if (depth >= max_height) return error.MemoryWorkLimit;
        const raw = self.pending.get(id) orelse return;
        const node = self.nodes.get(id) orelse return error.MemoryCorruptState;
        try self.publishDepth(node.left, depth + 1);
        try self.publishDepth(node.right, depth + 1);
        try self.store.put(self.store.ptr, self.a, try std.mem.concat(self.a, u8, &.{ self.prefix, id }), raw);
        _ = self.pending.remove(id);
    }
    fn priorityBefore(left: []const u8, right: []const u8) bool {
        var l: [32]u8 = undefined;
        var r: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(left, &l, .{});
        std.crypto.hash.sha2.Sha256.hash(right, &r, .{});
        const order = std.mem.order(u8, &l, &r);
        return order == .lt or (order == .eq and std.mem.lessThan(u8, left, right));
    }
    const Split = struct { left: Root, right: Root };
    fn split(self: *Index, root: Root, key: []const u8, depth: usize) anyerror!Split {
        if (depth >= max_height) return error.MemoryWorkLimit;
        const id = root orelse return .{ .left = null, .right = null };
        var node = try self.load(id);
        if (std.mem.lessThan(u8, node.key, key)) {
            const parts = try self.split(node.right, key, depth + 1);
            node.right = parts.left;
            return .{ .left = try self.save(node), .right = parts.right };
        }
        const parts = try self.split(node.left, key, depth + 1);
        node.left = parts.right;
        return .{ .left = parts.left, .right = try self.save(node) };
    }
    pub fn get(self: *Index, root: Root, key: []const u8) !?[]const u8 {
        var at = root;
        var depth: usize = 0;
        while (at) |id| {
            depth += 1;
            if (depth > max_height) return error.MemoryCorruptState;
            const node = try self.load(id);
            switch (std.mem.order(u8, key, node.key)) {
                .eq => return node.value,
                .lt => at = node.left,
                .gt => at = node.right,
            }
        }
        return null;
    }
    pub fn put(self: *Index, root: Root, key: []const u8, value: []const u8) !Root {
        if (key.len == 0 or key.len > 1024 or value.len > 4096 or !std.unicode.utf8ValidateSlice(key) or !std.unicode.utf8ValidateSlice(value)) return error.InvalidMemoryRequest;
        // An existing key keeps its deterministic priority and position.
        return self.insert(root, try self.a.dupe(u8, key), try self.a.dupe(u8, value), 0);
    }
    fn insert(self: *Index, root: Root, key: []const u8, value: []const u8, depth: usize) anyerror!Root {
        if (depth >= max_height) return error.MemoryWorkLimit;
        const id = root orelse return try self.save(.{ .key = key, .value = value });
        var node = try self.load(id);
        const order = std.mem.order(u8, key, node.key);
        if (order == .eq) {
            if (std.mem.eql(u8, value, node.value)) return root;
            node.value = value;
        } else if (priorityBefore(key, node.key)) {
            const parts = try self.split(root, key, depth);
            return try self.save(.{ .key = key, .value = value, .left = parts.left, .right = parts.right });
        } else if (order == .lt) node.left = try self.insert(node.left, key, value, depth + 1) else node.right = try self.insert(node.right, key, value, depth + 1);
        return try self.save(node);
    }
    pub fn remove(self: *Index, root: Root, key: []const u8) anyerror!Root {
        return self.erase(root, key, 0);
    }
    fn erase(self: *Index, root: Root, key: []const u8, depth: usize) anyerror!Root {
        if (depth >= max_height) return error.MemoryWorkLimit;
        const id = root orelse return null;
        var node = try self.load(id);
        switch (std.mem.order(u8, key, node.key)) {
            .eq => return self.merge(node.left, node.right, depth + 1),
            .lt => node.left = try self.erase(node.left, key, depth + 1),
            .gt => node.right = try self.erase(node.right, key, depth + 1),
        }
        return try self.save(node);
    }
    fn merge(self: *Index, left: Root, right: Root, depth: usize) anyerror!Root {
        if (depth >= max_height) return error.MemoryWorkLimit;
        if (left == null) return right;
        if (right == null) return left;
        var l = try self.load(left.?);
        var r = try self.load(right.?);
        if (priorityBefore(l.key, r.key)) {
            l.right = try self.merge(l.right, right, depth + 1);
            return try self.save(l);
        }
        r.left = try self.merge(left, r.left, depth + 1);
        return try self.save(r);
    }
    pub fn page(self: *Index, root: Root, lower: []const u8, upper: ?[]const u8, after: ?[]const u8, limit: usize) !Page {
        if (limit == 0 or limit > 256) return error.InvalidMemoryRequest;
        var items: std.ArrayList(Item) = .empty;
        try self.collect(root, lower, upper, after, limit + 1, &items, 0);
        const more = items.items.len > limit;
        return .{ .items = items.items[0..@min(limit, items.items.len)], .next = if (more) items.items[limit - 1].key else null };
    }
    fn collect(self: *Index, root: Root, lower: []const u8, upper: ?[]const u8, after: ?[]const u8, limit: usize, items: *std.ArrayList(Item), depth: usize) anyerror!void {
        const id = root orelse return;
        if (items.items.len == limit) return;
        if (depth > max_height) return error.MemoryCorruptState;
        const node = try self.load(id);
        const below = std.mem.lessThan(u8, node.key, lower) or (if (after) |v| !std.mem.lessThan(u8, v, node.key) else false);
        const above = if (upper) |v| !std.mem.lessThan(u8, node.key, v) else false;
        if (!below) try self.collect(node.left, lower, upper, after, limit, items, depth + 1);
        if (!below and !above and items.items.len < limit) try items.append(self.a, .{ .key = node.key, .value = node.value });
        if (!above) try self.collect(node.right, lower, upper, after, limit, items, depth + 1);
    }
};

test "native persistent indexes fork, update, rollback and seek without enumerating the old root" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Memory = struct {
        a: A,
        rows: std.StringHashMapUnmanaged([]const u8) = .empty,
        reads: usize = 0,
        writes: usize = 0,
        fn get(ptr: *anyopaque, _: A, key: []const u8) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reads += 1;
            return self.rows.get(key);
        }
        fn put(ptr: *anyopaque, _: A, key: []const u8, value: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.writes += 1;
            try self.rows.put(self.a, key, value);
        }
    };
    var memory: Memory = .{ .a = a };
    var index: Index = .{ .a = a, .prefix = "node/", .store = .{ .ptr = &memory, .get = Memory.get, .put = Memory.put }, .max_steps = 1000000 };
    var root: Root = null;
    for (0..10000) |i| root = try index.put(root, try std.fmt.allocPrint(a, "item/{d:0>5}", .{i}), "record-reference");
    try std.testing.expectEqual(@as(usize, 0), memory.writes);
    try index.publish(root);
    try std.testing.expectEqual(@as(usize, 10000), memory.writes);
    const parent = root;
    var cold: Index = .{ .a = a, .prefix = "node/", .store = index.store };
    memory.reads = 0;
    memory.writes = 0;
    root = try cold.put(root, "item/05000", "replacement-reference");
    try cold.publish(root);
    try std.testing.expect(memory.reads < 128 and memory.writes < 128);
    try std.testing.expectEqualStrings("record-reference", (try cold.get(parent, "item/05000")).?);
    try std.testing.expectEqualStrings("replacement-reference", (try cold.get(root, "item/05000")).?);
    const fork = try cold.remove(root, "item/05000");
    try std.testing.expect((try cold.get(fork, "item/05000")) == null);
    try std.testing.expectEqualStrings("replacement-reference", (try cold.get(root, "item/05000")).?);
    memory.reads = 0;
    var reader: Index = .{ .a = a, .prefix = "node/", .store = index.store };
    const page = try reader.page(parent, "item/09000", "item/09100", null, 16);
    try std.testing.expectEqual(@as(usize, 16), page.items.len);
    try std.testing.expectEqualStrings("item/09000", page.items[0].key);
    try std.testing.expect(page.next != null and memory.reads < 128);
    const next = try reader.page(parent, "item/09000", "item/09100", page.next, 16);
    try std.testing.expectEqualStrings("item/09016", next.items[0].key);
}
