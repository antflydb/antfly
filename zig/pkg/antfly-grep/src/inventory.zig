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

//! Canonical persistent Cartesian tree. Shape depends only on paths, never on
//! insertion history or dense entry slots. Roots share unchanged subtrees;
//! bounded path copying permits transactional publication without O(N) rebuilds.
const std = @import("std");
const A = std.mem.Allocator;
pub const Node = struct {
    refs: usize = 1,
    path: []const u8,
    slot: usize,
    priority: [32]u8,
    leaf: [32]u8,
    hash: [32]u8,
    left: ?*Node,
    right: ?*Node,
    count: usize,
    height: usize,
};
pub fn retain(root: ?*Node) ?*Node {
    if (root) |n| n.refs += 1;
    return root;
}
pub fn release(a: A, root: ?*Node) void {
    const n = root orelse return;
    n.refs -= 1;
    if (n.refs != 0) return;
    release(a, n.left);
    release(a, n.right);
    freeNode(a, n);
}
// Keep the immutable key adjacent to its node: one allocation, one budget
// charge and one free, including persistent copies and cold construction.
fn freeNode(a: A, n: *Node) void {
    const bytes: [*]align(@alignOf(Node)) u8 = @ptrCast(n);
    a.free(bytes[0 .. @sizeOf(Node) + n.path.len]);
}
fn allocate(a: A, path: []const u8, slot: usize, leaf: [32]u8) !*Node {
    const size = try std.math.add(usize, @sizeOf(Node), path.len);
    const bytes = try a.alignedAlloc(u8, .of(Node), size);
    const n: *Node = @ptrCast(bytes.ptr);
    const key = bytes[@sizeOf(Node)..];
    @memcpy(key, path);
    n.* = .{ .path = key, .slot = slot, .leaf = leaf, .priority = undefined, .hash = undefined, .left = null, .right = null, .height = 1, .count = 1 };
    std.crypto.hash.sha2.Sha256.hash(path, &n.priority, .{});
    return n;
}
fn rehash(n: *Node) !void {
    n.height = 1 + @max(if (n.left) |v| v.height else 0, if (n.right) |v| v.height else 0);
    if (n.height > 128) return error.InventoryDepthLimit;
    n.count = 1 + (if (n.left) |v| v.count else 0) + (if (n.right) |v| v.count else 0);
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update("antfly-workspace-inventory-v3");
    h.update(if (n.left) |v| &v.hash else &@as([32]u8, @splat(0)));
    h.update(&n.leaf);
    h.update(if (n.right) |v| &v.hash else &@as([32]u8, @splat(0)));
    h.final(&n.hash);
}
fn make(a: A, path: []const u8, slot: usize, leaf: [32]u8, left: ?*Node, right: ?*Node) !*Node {
    const n = try allocate(a, path, slot, leaf);
    errdefer freeNode(a, n);
    n.left = left;
    n.right = right;
    try rehash(n);
    _ = retain(left);
    _ = retain(right);
    return n;
}
fn before(a: *const Node, b: *const Node) bool {
    return switch (std.mem.order(u8, &a.priority, &b.priority)) {
        .lt => true,
        .gt => false,
        .eq => std.mem.order(u8, a.path, b.path) == .lt,
    };
}
pub fn put(a: A, root: ?*Node, path: []const u8, slot: usize, leaf: [32]u8) !*Node {
    const n = root orelse return make(a, path, slot, leaf, null, null);
    switch (std.mem.order(u8, path, n.path)) {
        .eq => return make(a, path, slot, leaf, n.left, n.right),
        .lt => {
            const child = try put(a, n.left, path, slot, leaf);
            defer release(a, child);
            if (before(child, n)) {
                const right = try make(a, n.path, n.slot, n.leaf, child.right, n.right);
                defer release(a, right);
                return make(a, child.path, child.slot, child.leaf, child.left, right);
            }
            return make(a, n.path, n.slot, n.leaf, child, n.right);
        },
        .gt => {
            const child = try put(a, n.right, path, slot, leaf);
            defer release(a, child);
            if (before(child, n)) {
                const left = try make(a, n.path, n.slot, n.leaf, n.left, child.left);
                defer release(a, left);
                return make(a, child.path, child.slot, child.leaf, left, child.right);
            }
            return make(a, n.path, n.slot, n.leaf, n.left, child);
        },
    }
}
fn merge(a: A, left: ?*Node, right: ?*Node) anyerror!?*Node {
    const l = left orelse return retain(right);
    const r = right orelse return retain(left);
    if (before(l, r)) {
        const child = try merge(a, l.right, r);
        defer release(a, child);
        return try make(a, l.path, l.slot, l.leaf, l.left, child);
    }
    const child = try merge(a, l, r.left);
    defer release(a, child);
    return try make(a, r.path, r.slot, r.leaf, child, r.right);
}
pub fn remove(a: A, root: ?*Node, path: []const u8) !?*Node {
    const n = root orelse return null;
    switch (std.mem.order(u8, path, n.path)) {
        .eq => return merge(a, n.left, n.right),
        .lt => {
            const child = try remove(a, n.left, path);
            defer release(a, child);
            return try make(a, n.path, n.slot, n.leaf, child, n.right);
        },
        .gt => {
            const child = try remove(a, n.right, path);
            defer release(a, child);
            return try make(a, n.path, n.slot, n.leaf, n.left, child);
        },
    }
}
pub fn find(root: ?*const Node, path: []const u8) ?usize {
    var current = root;
    while (current) |n| switch (std.mem.order(u8, path, n.path)) {
        .eq => return n.slot,
        .lt => current = n.left,
        .gt => current = n.right,
    };
    return null;
}
pub const Iterator = struct {
    stack: [128]*const Node = undefined,
    len: usize = 0,
    pub fn init(root: ?*const Node, lower: ?[]const u8) Iterator {
        var self: Iterator = .{};
        var current = root;
        while (current) |n| {
            if (lower == null or std.mem.order(u8, n.path, lower.?) != .lt) {
                self.stack[self.len] = n;
                self.len += 1;
                current = n.left;
            } else current = n.right;
        }
        return self;
    }
    pub fn next(self: *Iterator) ?usize {
        if (self.len == 0) return null;
        self.len -= 1;
        const n = self.stack[self.len];
        var current = n.right;
        while (current) |child| {
            self.stack[self.len] = child;
            self.len += 1;
            current = child.left;
        }
        return n.slot;
    }
};
/// Linear construction from a sorted cold discovery. Reparenting transfers
/// ownership; no transient persistent copies or per-node searches are needed.
pub fn build(a: A, entries: anytype, comptime leafHash: anytype) !?*Node {
    var root: ?*Node = null;
    errdefer release(a, root);
    var stack: [128]*Node = undefined;
    var len: usize = 0;
    for (entries, 0..) |entry, slot| {
        // Hash only after final children are installed by the Cartesian build.
        const n = try allocate(a, entry.path, slot, leafHash(entry));
        var last: ?*Node = null;
        while (len > 0 and before(n, stack[len - 1])) {
            len -= 1;
            last = stack[len];
        }
        n.left = last;
        if (len == 0) root = n else stack[len - 1].right = n;
        if (len == stack.len) return error.InventoryDepthLimit;
        stack[len] = n;
        len += 1;
    }
    try finish(root, 0);
    return root;
}
fn finish(root: ?*Node, depth: usize) !void {
    const n = root orelse return;
    if (depth >= 128) return error.InventoryDepthLimit;
    try finish(n.left, depth + 1);
    try finish(n.right, depth + 1);
    try rehash(n);
}

test "inventory identities ignore insertion history and dense slots; old roots survive updates" {
    const a = std.testing.allocator;
    var root: ?*Node = null;
    defer release(a, root);
    const paths = [_][]const u8{ "a", "b", "c", "d", "e" };
    for (paths, 0..) |path, i| {
        const next = try put(a, root, path, i, @splat(@intCast(i)));
        release(a, root);
        root = next;
    }
    const old = retain(root);
    defer release(a, old);
    const removed = try remove(a, root, "c");
    release(a, root);
    root = removed;
    try std.testing.expect(find(root, "c") == null);
    try std.testing.expectEqual(@as(usize, 2), find(old, "c").?);
    const replaced = try put(a, root, "c", 99, @splat(2));
    release(a, root);
    root = replaced;
    try std.testing.expectEqualSlices(u8, &old.?.hash, &root.?.hash);
    var reverse: ?*Node = null;
    defer release(a, reverse);
    var i = paths.len;
    while (i > 0) {
        i -= 1;
        const next = try put(a, reverse, paths[i], i, @splat(@intCast(i)));
        release(a, reverse);
        reverse = next;
    }
    try std.testing.expectEqualSlices(u8, &root.?.hash, &reverse.?.hash);
    var it = Iterator.init(root, "b");
    try std.testing.expectEqual(@as(usize, 1), it.next().?);
    try std.testing.expectEqual(@as(usize, 99), it.next().?);
}

fn allocationCase(a: A) !void {
    var root: ?*Node = null;
    defer release(a, root);
    for ([_][]const u8{ "a", "b", "c", "d" }, 0..) |path, slot| {
        const next = try put(a, root, path, slot, @splat(@intCast(slot)));
        release(a, root);
        root = next;
    }
    const next = try remove(a, root, "b");
    release(a, root);
    root = next;
}
fn coldAllocationCase(a: A) !void {
    const Record = struct {
        path: []const u8,
        fn hash(_: @This()) [32]u8 {
            return @splat(7);
        }
    };
    const records = [_]Record{ .{ .path = "" }, .{ .path = "a" }, .{ .path = "longer/path" }, .{ .path = "z" } };
    const root = try build(a, &records, Record.hash);
    defer release(a, root);
    var copied: ?*Node = null;
    defer release(a, copied);
    for (records, 0..) |record, i| {
        const next = try put(a, copied, record.path, i, Record.hash(record));
        release(a, copied);
        copied = next;
    }
    try std.testing.expectEqualSlices(u8, &root.?.hash, &copied.?.hash);
}
test "inventory allocation failures release every staged path and shared node" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, coldAllocationCase, .{});
}

test "mixed inventory mutations equal linear canonical rebuilds" {
    const Record = struct {
        path: []const u8,
        leaf: [32]u8,
        fn hash(self: @This()) [32]u8 {
            return self.leaf;
        }
    };
    const a = std.testing.allocator;
    var root: ?*Node = null;
    defer release(a, root);
    var names: [64][16]u8 = undefined;
    var paths: [64][]const u8 = undefined;
    var values: [64]?[32]u8 = @splat(null);
    for (&names, &paths, 0..) |*buffer, *path, i| path.* = try std.fmt.bufPrint(buffer, "file-{d:0>3}", .{i});
    for (0..256) |step| {
        const slot = (step * 37) % 64;
        const path = paths[slot];
        const next = if (values[slot] != null and step % 3 == 0) blk: {
            values[slot] = null;
            break :blk try remove(a, root, path);
        } else blk: {
            values[slot] = @splat(@intCast(step % 256));
            break :blk try put(a, root, path, step, values[slot].?);
        };
        release(a, root);
        root = next;
        var records: [64]Record = undefined;
        var len: usize = 0;
        for (paths, values) |key, value| if (value) |leaf| {
            records[len] = .{ .path = key, .leaf = leaf };
            len += 1;
        };
        const rebuilt = try build(a, records[0..len], Record.hash);
        defer release(a, rebuilt);
        try std.testing.expectEqualSlices(u8, &root.?.hash, &rebuilt.?.hash);
    }
}
