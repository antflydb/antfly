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

//! Canonical compressed binary Merkle trie. Keys determine the shape, so load
//! order cannot change its root. Updates visit at most 64 branches; compression
//! bounds memory to two nodes per live chunk, regardless of historical IDs.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Digest = [32]u8;

pub const Tree = struct {
    root: ?*Node = null,
    nodes: usize = 0,
    hashes: u64 = 0,
    pub const Entry = struct { key: u64, value: Digest };

    const Node = struct {
        key: u64,
        digest: Digest,
        branch: ?struct { bit: u6, children: [2]*Node } = null,
    };

    pub fn deinit(self: *Tree, allocator: Allocator) void {
        if (self.root) |root| destroy(allocator, root);
        self.* = .{};
    }

    fn destroy(allocator: Allocator, node: *Node) void {
        if (node.branch) |branch| for (branch.children) |child| destroy(allocator, child);
        allocator.destroy(node);
    }

    fn direction(key: u64, bit: u6) u1 {
        return @truncate(key >> bit);
    }

    /// Cold ledger loads already enumerate every chunk. Hash each trie node
    /// once instead of rebuilding ancestor paths for each initial insertion.
    pub fn fromSorted(allocator: Allocator, entries: []const Entry) !Tree {
        return fromSortedWithCancel(allocator, entries, null);
    }

    pub fn fromSortedWithCancel(allocator: Allocator, entries: []const Entry, cancel: ?*const std.atomic.Value(bool)) !Tree {
        for (entries, 0..) |entry, i| if (i != 0 and entries[i - 1].key >= entry.key) return error.InvalidDigestOrder;
        var result: Tree = .{};
        if (entries.len != 0) result.root = try result.build(allocator, entries, cancel);
        return result;
    }

    fn build(self: *Tree, allocator: Allocator, entries: []const Entry, cancel: ?*const std.atomic.Value(bool)) !*Node {
        if (cancel) |flag| if (flag.load(.acquire)) return error.MaintenanceCanceled;
        if (entries.len == 1) {
            const leaf = try allocator.create(Node);
            leaf.* = .{ .key = entries[0].key, .digest = leafDigest(entries[0].key, entries[0].value) };
            self.nodes += 1;
            self.hashes +|= 1;
            return leaf;
        }
        const bit: u6 = @intCast(63 - @clz(entries[0].key ^ entries[entries.len - 1].key));
        var low: usize = 0;
        var high = entries.len;
        while (low < high) {
            const middle = low + (high - low) / 2;
            if (direction(entries[middle].key, bit) == 0) low = middle + 1 else high = middle;
        }
        const left = try self.build(allocator, entries[0..low], cancel);
        errdefer destroy(allocator, left);
        const right = try self.build(allocator, entries[low..], cancel);
        errdefer destroy(allocator, right);
        const node = try allocator.create(Node);
        const prefix = if (bit == 63) 0 else entries[0].key & (~@as(u64, 0) << (bit + 1));
        node.* = .{ .key = prefix, .digest = undefined, .branch = .{ .bit = bit, .children = .{ left, right } } };
        self.rehash(node);
        self.nodes += 1;
        return node;
    }

    fn leafDigest(key: u64, value: Digest) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("Antfly allocator Merkle leaf v2");
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, key, .little);
        hash.update(&encoded);
        hash.update(&value);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }

    fn rehash(self: *Tree, node: *Node) void {
        const branch = node.branch.?;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("Antfly allocator Merkle branch v2");
        var encoded: [9]u8 = undefined;
        encoded[0] = branch.bit;
        std.mem.writeInt(u64, encoded[1..9], node.key, .little);
        hash.update(&encoded);
        hash.update(&branch.children[0].digest);
        hash.update(&branch.children[1].digest);
        hash.final(&node.digest);
        self.hashes +|= 1;
    }

    pub fn digest(self: *const Tree) Digest {
        if (self.root) |root| return root.digest;
        var result: Digest = undefined;
        std.crypto.hash.Blake3.hash("Antfly allocator Merkle empty v2", &result, .{});
        return result;
    }

    pub fn put(self: *Tree, allocator: Allocator, key: u64, value: Digest) !void {
        var path: [64]*Node = undefined;
        var directions: [64]u1 = undefined;
        var depth: usize = 0;
        var current = self.root;
        while (current) |node| {
            const branch = node.branch orelse break;
            path[depth] = node;
            const side = direction(key, branch.bit);
            directions[depth] = side;
            depth += 1;
            current = branch.children[side];
        }
        const encoded = leafDigest(key, value);
        if (current) |leaf| if (leaf.key == key) {
            if (std.mem.eql(u8, &leaf.digest, &encoded)) return;
            leaf.digest = encoded;
            self.hashes +|= 1;
            while (depth != 0) {
                depth -= 1;
                self.rehash(path[depth]);
            }
            return;
        };
        const leaf = try allocator.create(Node);
        errdefer allocator.destroy(leaf);
        leaf.* = .{ .key = key, .digest = encoded };
        if (current == null) {
            self.root = leaf;
            self.nodes += 1;
            self.hashes +|= 1;
            return;
        }
        const bit: u6 = @intCast(63 - @clz(key ^ current.?.key));
        var insertion: usize = 0;
        while (insertion < depth and path[insertion].branch.?.bit > bit) : (insertion += 1) {}
        const sibling = if (insertion < depth) path[insertion] else current.?;
        const branch = try allocator.create(Node);
        const side = direction(key, bit);
        const prefix = if (bit == 63) 0 else key & (~@as(u64, 0) << (@as(u6, bit) + 1));
        var children: [2]*Node = undefined;
        children[side] = leaf;
        children[1 - side] = sibling;
        branch.* = .{ .key = prefix, .digest = undefined, .branch = .{ .bit = bit, .children = children } };
        self.rehash(branch);
        if (insertion == 0) self.root = branch else path[insertion - 1].branch.?.children[directions[insertion - 1]] = branch;
        while (insertion != 0) {
            insertion -= 1;
            self.rehash(path[insertion]);
        }
        self.nodes += 2;
        self.hashes +|= 1;
    }

    pub fn remove(self: *Tree, allocator: Allocator, key: u64) bool {
        var path: [64]*Node = undefined;
        var directions: [64]u1 = undefined;
        var depth: usize = 0;
        var current = self.root orelse return false;
        while (current.branch) |branch| {
            path[depth] = current;
            const side = direction(key, branch.bit);
            directions[depth] = side;
            depth += 1;
            current = branch.children[side];
        }
        if (current.key != key) return false;
        allocator.destroy(current);
        self.nodes -= 1;
        if (depth == 0) {
            self.root = null;
            return true;
        }
        depth -= 1;
        const parent = path[depth];
        const sibling = parent.branch.?.children[1 - directions[depth]];
        if (depth == 0) self.root = sibling else path[depth - 1].branch.?.children[directions[depth - 1]] = sibling;
        allocator.destroy(parent);
        self.nodes -= 1;
        while (depth != 0) {
            depth -= 1;
            self.rehash(path[depth]);
        }
        return true;
    }
};

test "ledger Merkle root is canonical across insertion removal and replacement" {
    const a = std.testing.allocator;
    const keys = [_]u64{ 0, 1, 63, 64, 256, 9999, @as(u64, 1) << 63, std.math.maxInt(u64) };
    var forward: Tree = .{};
    defer forward.deinit(a);
    var reverse: Tree = .{};
    defer reverse.deinit(a);
    for (keys, 0..) |key, index| try forward.put(a, key, @splat(@intCast(index)));
    var i = keys.len;
    while (i != 0) {
        i -= 1;
        try reverse.put(a, keys[i], @splat(@intCast(i)));
    }
    try std.testing.expectEqualSlices(u8, &forward.digest(), &reverse.digest());
    try std.testing.expectEqual(@as(usize, keys.len * 2 - 1), forward.nodes);
    const initial = forward.digest();
    try forward.put(a, 64, @splat(99));
    try std.testing.expect(!std.mem.eql(u8, &initial, &forward.digest()));
    try forward.put(a, 64, @splat(3));
    try std.testing.expectEqualSlices(u8, &initial, &forward.digest());
    for (keys) |key| try std.testing.expect(forward.remove(a, key));
    try std.testing.expectEqual(@as(usize, 0), forward.nodes);
    try std.testing.expect(forward.root == null);
}

test "ledger Merkle update work is bounded independently of retained chunks" {
    const a = std.testing.allocator;
    for ([_]usize{ 8, 4096 }) |count| {
        var tree: Tree = .{};
        defer tree.deinit(a);
        for (0..count) |key| try tree.put(a, @intCast(key), @splat(1));
        const before = tree.hashes;
        try tree.put(a, 3, @splat(2));
        try std.testing.expect(tree.hashes - before <= 65);
        try std.testing.expectEqual(count * 2 - 1, tree.nodes);
    }
}

test "ledger Merkle sparse deletions match a fresh canonical rebuild" {
    const a = std.testing.allocator;
    var tree: Tree = .{};
    defer tree.deinit(a);
    var rebuilt: Tree = .{};
    defer rebuilt.deinit(a);
    const stride: u64 = 0x9e3779b97f4a7c15;
    for (0..512) |i| try tree.put(a, @as(u64, @intCast(i)) *% stride, @splat(1));
    for (0..256) |i| try std.testing.expect(tree.remove(a, @as(u64, @intCast(i * 2)) *% stride));
    var i: usize = 512;
    while (i != 0) {
        i -= 1;
        if (i % 2 == 1) try rebuilt.put(a, @as(u64, @intCast(i)) *% stride, @splat(1));
    }
    try std.testing.expectEqualSlices(u8, &rebuilt.digest(), &tree.digest());
    try std.testing.expectEqual(@as(usize, 511), tree.nodes);
}

test "ledger Merkle bulk load hashes each node once and matches incremental updates" {
    const a = std.testing.allocator;
    var entries: [256]Tree.Entry = undefined;
    var incremental: Tree = .{};
    defer incremental.deinit(a);
    for (&entries, 0..) |*entry, i| {
        entry.* = .{ .key = @as(u64, @intCast(i)) << 55, .value = @splat(1) };
        try incremental.put(a, entry.key, entry.value);
    }
    var bulk = try Tree.fromSorted(a, &entries);
    defer bulk.deinit(a);
    try std.testing.expectEqualSlices(u8, &incremental.digest(), &bulk.digest());
    try std.testing.expectEqual(@as(u64, entries.len * 2 - 1), bulk.hashes);
    try bulk.put(a, entries[3].key, @splat(2));
    try incremental.put(a, entries[3].key, @splat(2));
    try std.testing.expectEqualSlices(u8, &incremental.digest(), &bulk.digest());
}

test "ledger Merkle bulk load allocation failures release partial subtrees" {
    const a = std.testing.allocator;
    var entries: [8]Tree.Entry = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = i, .value = @splat(1) };
    for (0..entries.len * 2) |failure| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = failure });
        if (Tree.fromSorted(failing.allocator(), &entries)) |value| {
            var tree = value;
            tree.deinit(failing.allocator());
            try std.testing.expectEqual(@as(usize, entries.len * 2 - 1), failure);
        } else |err| try std.testing.expectEqual(error.OutOfMemory, err);
    }
}
