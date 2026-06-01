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

//! Persistent ordered map used by the in-memory backend.
//!
//! Entries are keyed by `(namespace, key)` and kept in sort order (a null
//! namespace sorts before any named namespace, then by key bytes). The map is a
//! treap: a binary search tree on the key with a heap order on a hash-derived
//! priority, which keeps it balanced in expectation regardless of insertion
//! order without storing any per-tree RNG state.
//!
//! Crucially the tree is *persistent*: insert/delete copy only the O(log n)
//! nodes on the path and share every untouched subtree, so producing a new
//! version is O(log n) and taking a read snapshot is O(1) (retain the root).
//! Nodes are reference counted, so older snapshots stay valid until released.
//! This replaces an earlier sorted-array store whose every transaction cloned
//! the whole array (O(n) snapshots, O(n^2) ingest).

const std = @import("std");
const Allocator = std.mem.Allocator;
const Order = std.math.Order;

const priority_seed: u64 = 0x2545f4914f6cdd1d;

fn priorityFor(name: ?[]const u8, key: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(priority_seed);
    if (name) |namespace| {
        hasher.update(&.{1});
        hasher.update(namespace);
    } else {
        hasher.update(&.{0});
    }
    hasher.update(key);
    return hasher.final();
}

/// Order of `(a_name, a_key)` relative to `(b_name, b_key)`: a null namespace
/// sorts first, then namespaces compare by bytes, then keys compare by bytes.
pub fn compareKeys(a_name: ?[]const u8, a_key: []const u8, b_name: ?[]const u8, b_key: []const u8) Order {
    const namespace_order = if (a_name == null and b_name == null)
        Order.eq
    else if (a_name == null)
        Order.lt
    else if (b_name == null)
        Order.gt
    else
        std.mem.order(u8, a_name.?, b_name.?);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, a_key, b_key);
}

const Node = struct {
    // Atomic: nodes are shared across snapshots, and the backend mutates these
    // refcounts off the backend mutex — a writer `retain`s shared path nodes
    // during an unlocked `put` while a concurrent reader `releaseNode`s the same
    // nodes on abort/commit. A non-atomic counter loses updates here, dropping
    // refs to zero early (use-after-free) or underflowing (the `-= 1` overflow
    // panic). The node *contents* stay immutable after makeNode (copy-on-write),
    // so only this counter needs synchronization.
    refs: usize,
    name: ?[]const u8,
    key: []const u8,
    value: []const u8,
    priority: u64,
    left: ?*Node = null,
    right: ?*Node = null,

    fn order(self: *const Node, name: ?[]const u8, key: []const u8) Order {
        return compareKeys(self.name, self.key, name, key);
    }
};

fn retain(node: ?*Node) ?*Node {
    // Monotonic suffices: a retain races only with other refcount ops, and the
    // caller already holds a live reference to `node`, so no ordering against the
    // node's contents is needed.
    if (node) |present| _ = @atomicRmw(usize, &present.refs, .Add, 1, .monotonic);
    return node;
}

fn releaseNode(alloc: Allocator, node: ?*Node) void {
    const present = node orelse return;
    // acq_rel on the decrement: the release half publishes this thread's prior
    // uses of the node before the drop, and the acquire half makes the thread
    // that wins the final decrement observe every other thread's uses before it
    // frees — so the destroy below can't race a still-in-flight reader.
    if (@atomicRmw(usize, &present.refs, .Sub, 1, .acq_rel) > 1) return;
    releaseNode(alloc, present.left);
    releaseNode(alloc, present.right);
    if (present.name) |namespace| alloc.free(namespace);
    alloc.free(present.key);
    alloc.free(present.value);
    alloc.destroy(present);
}

/// Builds a fresh node (refs = 1) owning copies of name/key/value and *taking
/// ownership* of the `left`/`right` child references — on both success and
/// failure those references are consumed, so callers pass owned (often freshly
/// `retain`ed) children and never release them afterwards.
fn makeNode(
    alloc: Allocator,
    name: ?[]const u8,
    key: []const u8,
    value: []const u8,
    priority: u64,
    left: ?*Node,
    right: ?*Node,
) !*Node {
    errdefer {
        releaseNode(alloc, left);
        releaseNode(alloc, right);
    }
    const node = try alloc.create(Node);
    errdefer alloc.destroy(node);
    const name_copy = if (name) |namespace| try alloc.dupe(u8, namespace) else null;
    errdefer if (name_copy) |namespace| alloc.free(namespace);
    const key_copy = try alloc.dupe(u8, key);
    errdefer alloc.free(key_copy);
    const value_copy = try alloc.dupe(u8, value);
    node.* = .{
        .refs = 1,
        .name = name_copy,
        .key = key_copy,
        .value = value_copy,
        .priority = priority,
        .left = left,
        .right = right,
    };
    return node;
}

// Consumes `node` on success; leaves it owned by the caller on error.
fn rotateRight(alloc: Allocator, node: *Node) !*Node {
    const left = node.left.?;
    const new_right = try makeNode(alloc, node.name, node.key, node.value, node.priority, retain(left.right), retain(node.right));
    const new_root = makeNode(alloc, left.name, left.key, left.value, left.priority, retain(left.left), new_right) catch |err| return err;
    releaseNode(alloc, node);
    return new_root;
}

// Consumes `node` on success; leaves it owned by the caller on error.
fn rotateLeft(alloc: Allocator, node: *Node) !*Node {
    const right = node.right.?;
    const new_left = try makeNode(alloc, node.name, node.key, node.value, node.priority, retain(node.left), retain(right.left));
    const new_root = makeNode(alloc, right.name, right.key, right.value, right.priority, new_left, retain(right.right)) catch |err| return err;
    releaseNode(alloc, node);
    return new_root;
}

const InsertResult = struct { root: *Node, added: bool };

// Borrows `node` (never consumes it); returns a new owned root sharing untouched
// subtrees, or an error after cleaning up any partial work.
fn insert(alloc: Allocator, node: ?*Node, name: ?[]const u8, key: []const u8, value: []const u8, priority: u64) !InsertResult {
    const current = node orelse {
        const leaf = try makeNode(alloc, name, key, value, priority, null, null);
        return .{ .root = leaf, .added = true };
    };
    switch (compareKeys(name, key, current.name, current.key)) {
        .eq => {
            const updated = try makeNode(alloc, current.name, current.key, value, current.priority, retain(current.left), retain(current.right));
            return .{ .root = updated, .added = false };
        },
        .lt => {
            const child = try insert(alloc, current.left, name, key, value, priority);
            var new_node = makeNode(alloc, current.name, current.key, current.value, current.priority, child.root, retain(current.right)) catch |err| return err;
            if (new_node.left.?.priority > new_node.priority) {
                new_node = rotateRight(alloc, new_node) catch |err| {
                    releaseNode(alloc, new_node);
                    return err;
                };
            }
            return .{ .root = new_node, .added = child.added };
        },
        .gt => {
            const child = try insert(alloc, current.right, name, key, value, priority);
            var new_node = makeNode(alloc, current.name, current.key, current.value, current.priority, retain(current.left), child.root) catch |err| return err;
            if (new_node.right.?.priority > new_node.priority) {
                new_node = rotateLeft(alloc, new_node) catch |err| {
                    releaseNode(alloc, new_node);
                    return err;
                };
            }
            return .{ .root = new_node, .added = child.added };
        },
    }
}

// Consumes `left` and `right`; returns their treap merge (heap order preserved).
fn mergeTreaps(alloc: Allocator, left: ?*Node, right: ?*Node) !?*Node {
    const l = left orelse return right;
    const r = right orelse return left;
    if (l.priority >= r.priority) {
        const merged_right = mergeTreaps(alloc, retain(l.right), r) catch |err| {
            releaseNode(alloc, l);
            return err;
        };
        const node = makeNode(alloc, l.name, l.key, l.value, l.priority, retain(l.left), merged_right) catch |err| {
            releaseNode(alloc, l);
            return err;
        };
        releaseNode(alloc, l);
        return node;
    } else {
        const merged_left = mergeTreaps(alloc, l, retain(r.left)) catch |err| {
            releaseNode(alloc, r);
            return err;
        };
        const node = makeNode(alloc, r.name, r.key, r.value, r.priority, merged_left, retain(r.right)) catch |err| {
            releaseNode(alloc, r);
            return err;
        };
        releaseNode(alloc, r);
        return node;
    }
}

const RemoveResult = struct { root: ?*Node, removed: bool };

// Borrows `node`; returns a new owned root (sharing untouched subtrees).
fn removeKey(alloc: Allocator, node: ?*Node, name: ?[]const u8, key: []const u8) !RemoveResult {
    const current = node orelse return .{ .root = null, .removed = false };
    switch (compareKeys(name, key, current.name, current.key)) {
        .eq => {
            const merged = try mergeTreaps(alloc, retain(current.left), retain(current.right));
            return .{ .root = merged, .removed = true };
        },
        .lt => {
            const child = try removeKey(alloc, current.left, name, key);
            if (!child.removed) {
                releaseNode(alloc, child.root);
                return .{ .root = retain(current).?, .removed = false };
            }
            const new_node = makeNode(alloc, current.name, current.key, current.value, current.priority, child.root, retain(current.right)) catch |err| return err;
            return .{ .root = new_node, .removed = true };
        },
        .gt => {
            const child = try removeKey(alloc, current.right, name, key);
            if (!child.removed) {
                releaseNode(alloc, child.root);
                return .{ .root = retain(current).?, .removed = false };
            }
            const new_node = makeNode(alloc, current.name, current.key, current.value, current.priority, retain(current.left), child.root) catch |err| return err;
            return .{ .root = new_node, .removed = true };
        },
    }
}

pub const Entry = struct {
    name: ?[]const u8,
    key: []const u8,
    value: []const u8,
};

/// A persistent ordered map. Copying a `Tree` value is *not* a snapshot; use
/// `snapshot()`/`release()` to manage the shared root's lifetime.
pub const Tree = struct {
    root: ?*Node = null,
    len: usize = 0,

    pub const empty: Tree = .{};

    /// O(1) snapshot: shares the current root, kept alive until `release`.
    pub fn snapshot(self: Tree) Tree {
        return .{ .root = retain(self.root), .len = self.len };
    }

    pub fn release(self: *Tree, alloc: Allocator) void {
        releaseNode(alloc, self.root);
        self.* = .{};
    }

    pub fn count(self: Tree) usize {
        return self.len;
    }

    pub fn get(self: Tree, name: ?[]const u8, key: []const u8) ?[]const u8 {
        var node = self.root;
        while (node) |present| {
            node = switch (present.order(name, key)) {
                .eq => return present.value,
                .gt => present.left,
                .lt => present.right,
            };
        }
        return null;
    }

    /// Returns a new tree with `key` set to `value`, sharing untouched subtrees
    /// with `self`. `self` remains valid (and must still be released).
    pub fn put(self: Tree, alloc: Allocator, name: ?[]const u8, key: []const u8, value: []const u8) !Tree {
        const result = try insert(alloc, self.root, name, key, value, priorityFor(name, key));
        return .{ .root = result.root, .len = self.len + @intFromBool(result.added) };
    }

    /// Returns a new tree with `key` removed (or `self` re-shared when absent).
    pub fn remove(self: Tree, alloc: Allocator, name: ?[]const u8, key: []const u8) !Tree {
        const result = try removeKey(alloc, self.root, name, key);
        return .{ .root = result.root, .len = self.len - @intFromBool(result.removed) };
    }
};

/// Stack-based in-order cursor over a `Tree` snapshot. The cursor borrows the
/// tree's nodes, so the snapshot must outlive the cursor.
pub const Cursor = struct {
    alloc: Allocator,
    stack: std.ArrayListUnmanaged(*Node) = .empty,
    current: ?*Node = null,

    pub fn init(alloc: Allocator) Cursor {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Cursor) void {
        self.stack.deinit(self.alloc);
        self.* = undefined;
    }

    fn reset(self: *Cursor) void {
        self.stack.clearRetainingCapacity();
        self.current = null;
    }

    fn pushLeftSpine(self: *Cursor, node: ?*Node) !void {
        var current = node;
        while (current) |present| {
            try self.stack.append(self.alloc, present);
            current = present.left;
        }
    }

    fn pushRightSpine(self: *Cursor, node: ?*Node) !void {
        var current = node;
        while (current) |present| {
            try self.stack.append(self.alloc, present);
            current = present.right;
        }
    }

    pub fn first(self: *Cursor, tree: Tree) !?Entry {
        self.reset();
        try self.pushLeftSpine(tree.root);
        return self.settle();
    }

    pub fn last(self: *Cursor, tree: Tree) !?Entry {
        self.reset();
        try self.pushRightSpine(tree.root);
        return self.settleReverse();
    }

    /// Positions at the first entry whose key is >= `(name, key)`.
    pub fn seekAtOrAfter(self: *Cursor, tree: Tree, name: ?[]const u8, key: []const u8) !?Entry {
        self.reset();
        var node = tree.root;
        while (node) |present| {
            if (present.order(name, key) != .lt) {
                try self.stack.append(self.alloc, present);
                node = present.left;
            } else {
                node = present.right;
            }
        }
        return self.settle();
    }

    /// Positions at the last entry whose key is <= `(name, key)`.
    pub fn seekAtOrBefore(self: *Cursor, tree: Tree, name: ?[]const u8, key: []const u8) !?Entry {
        self.reset();
        var node = tree.root;
        while (node) |present| {
            if (present.order(name, key) != .gt) {
                try self.stack.append(self.alloc, present);
                node = present.right;
            } else {
                node = present.left;
            }
        }
        return self.settleReverse();
    }

    fn settle(self: *Cursor) ?Entry {
        if (self.stack.items.len == 0) {
            self.current = null;
            return null;
        }
        self.current = self.stack.items[self.stack.items.len - 1];
        return entryOf(self.current.?);
    }

    fn settleReverse(self: *Cursor) ?Entry {
        return self.settle();
    }

    pub fn next(self: *Cursor) !?Entry {
        const node = self.popCurrent() orelse return null;
        try self.pushLeftSpine(node.right);
        return self.settle();
    }

    pub fn prev(self: *Cursor) !?Entry {
        const node = self.popCurrent() orelse return null;
        try self.pushRightSpine(node.left);
        return self.settle();
    }

    fn popCurrent(self: *Cursor) ?*Node {
        if (self.stack.items.len == 0) return null;
        return self.stack.pop();
    }
};

fn entryOf(node: *Node) Entry {
    return .{ .name = node.name, .key = node.key, .value = node.value };
}

const testing = std.testing;

fn expectGet(tree: Tree, name: ?[]const u8, key: []const u8, expected: ?[]const u8) !void {
    const got = tree.get(name, key);
    if (expected) |want| {
        try testing.expect(got != null);
        try testing.expectEqualStrings(want, got.?);
    } else {
        try testing.expect(got == null);
    }
}

test "put/get/remove and update" {
    const alloc = testing.allocator;
    var tree: Tree = .empty;
    defer tree.release(alloc);

    var next_tree = try tree.put(alloc, null, "b", "2");
    tree.release(alloc);
    tree = next_tree;
    next_tree = try tree.put(alloc, null, "a", "1");
    tree.release(alloc);
    tree = next_tree;
    next_tree = try tree.put(alloc, null, "c", "3");
    tree.release(alloc);
    tree = next_tree;

    try testing.expectEqual(@as(usize, 3), tree.count());
    try expectGet(tree, null, "a", "1");
    try expectGet(tree, null, "b", "2");
    try expectGet(tree, null, "c", "3");
    try expectGet(tree, null, "z", null);

    // Update keeps count and replaces the value.
    next_tree = try tree.put(alloc, null, "b", "22");
    tree.release(alloc);
    tree = next_tree;
    try testing.expectEqual(@as(usize, 3), tree.count());
    try expectGet(tree, null, "b", "22");

    next_tree = try tree.remove(alloc, null, "b");
    tree.release(alloc);
    tree = next_tree;
    try testing.expectEqual(@as(usize, 2), tree.count());
    try expectGet(tree, null, "b", null);
    try expectGet(tree, null, "a", "1");
    try expectGet(tree, null, "c", "3");
}

test "snapshots are isolated from later writes" {
    const alloc = testing.allocator;
    var base: Tree = .empty;
    defer base.release(alloc);
    {
        const t = try base.put(alloc, null, "k", "v1");
        base.release(alloc);
        base = t;
    }

    var snap = base.snapshot();
    defer snap.release(alloc);

    // Mutating forward does not change the retained snapshot.
    {
        const t = try base.put(alloc, null, "k", "v2");
        base.release(alloc);
        base = t;
    }
    try expectGet(snap, null, "k", "v1");
    try expectGet(base, null, "k", "v2");
}

test "namespaces order before keys and isolate lookups" {
    const alloc = testing.allocator;
    var tree: Tree = .empty;
    defer tree.release(alloc);
    const pairs = [_]struct { name: ?[]const u8, key: []const u8 }{
        .{ .name = null, .key = "z" },
        .{ .name = "ns1", .key = "a" },
        .{ .name = "ns2", .key = "a" },
        .{ .name = "ns1", .key = "b" },
    };
    for (pairs) |pair| {
        const t = try tree.put(alloc, pair.name, pair.key, "x");
        tree.release(alloc);
        tree = t;
    }
    try expectGet(tree, "ns1", "a", "x");
    try expectGet(tree, "ns2", "a", "x");
    try expectGet(tree, "ns1", "a", "x");
    try expectGet(tree, "nsX", "a", null);

    // In-order iteration: null namespace first, then ns1's keys, then ns2.
    var cursor = Cursor.init(alloc);
    defer cursor.deinit();
    var entry = try cursor.first(tree);
    var seen: usize = 0;
    var last_name: ?[]const u8 = null;
    var last_key: []const u8 = "";
    while (entry) |present| : (entry = try cursor.next()) {
        if (seen > 0) {
            try testing.expect(compareKeys(last_name, last_key, present.name, present.key) == .lt);
        }
        last_name = present.name;
        last_key = present.key;
        seen += 1;
    }
    try testing.expectEqual(@as(usize, 4), seen);
}

test "cursor seek and ranged forward scan" {
    const alloc = testing.allocator;
    var tree: Tree = .empty;
    defer tree.release(alloc);
    for (0..50) |i| {
        var buf: [8]u8 = undefined;
        const key = std.fmt.bufPrint(&buf, "k{d:0>3}", .{i}) catch unreachable;
        const t = try tree.put(alloc, null, key, "v");
        tree.release(alloc);
        tree = t;
    }
    var cursor = Cursor.init(alloc);
    defer cursor.deinit();
    var entry = try cursor.seekAtOrAfter(tree, null, "k010");
    try testing.expect(entry != null);
    try testing.expectEqualStrings("k010", entry.?.key);
    var count: usize = 0;
    while (entry) |present| : (entry = try cursor.next()) {
        try testing.expect(std.mem.order(u8, "k010", present.key) != .gt);
        count += 1;
    }
    try testing.expectEqual(@as(usize, 40), count);

    // seek past the end yields nothing.
    try testing.expect((try cursor.seekAtOrAfter(tree, null, "z")) == null);
}

test "randomized operations match a reference map without leaking" {
    const alloc = testing.allocator;
    var reference = std.StringHashMapUnmanaged([]const u8).empty;
    defer {
        var it = reference.iterator();
        while (it.next()) |kv| {
            alloc.free(kv.key_ptr.*);
            alloc.free(kv.value_ptr.*);
        }
        reference.deinit(alloc);
    }
    var tree: Tree = .empty;
    defer tree.release(alloc);

    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rand = prng.random();
    for (0..4000) |_| {
        var key_buf: [4]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{d:0>3}", .{rand.intRangeAtMost(u32, 0, 400)}) catch unreachable;
        if (rand.boolean()) {
            var value_buf: [8]u8 = undefined;
            const value = std.fmt.bufPrint(&value_buf, "v{d}", .{rand.int(u16)}) catch unreachable;
            const t = try tree.put(alloc, null, key, value);
            tree.release(alloc);
            tree = t;
            const gop = try reference.getOrPut(alloc, key);
            if (!gop.found_existing) gop.key_ptr.* = try alloc.dupe(u8, key) else alloc.free(gop.value_ptr.*);
            gop.value_ptr.* = try alloc.dupe(u8, value);
        } else {
            const t = try tree.remove(alloc, null, key);
            tree.release(alloc);
            tree = t;
            if (reference.fetchRemove(key)) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
        }
    }

    try testing.expectEqual(reference.count(), tree.count());
    var it = reference.iterator();
    while (it.next()) |kv| {
        try expectGet(tree, null, kv.key_ptr.*, kv.value_ptr.*);
    }
}

// Regression: node refcounts are shared across snapshots and mutated off any
// lock — the backend `retain`s shared path nodes during an unlocked write while
// other threads release their own snapshots of the same nodes. A non-atomic
// counter loses updates under that interleaving and underflows on the next
// decrement (the `present.refs -= 1` overflow panic seen in derived-replay
// worker teardown). With an atomic refcount the snapshot/release churn below is
// clean; the GeneralPurposeAllocator's leak/double-free checks catch a refcount
// that drops early. The shared base stays read-only, so no data races on
// contents — only the counter is contended.
test "concurrent snapshot retain/release does not corrupt the refcount" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const alloc = testing.allocator;

    var base: Tree = .empty;
    defer base.release(alloc);
    var key_buf: [4]u8 = undefined;
    for (0..256) |i| {
        const key = std.fmt.bufPrint(&key_buf, "{d:0>3}", .{i}) catch unreachable;
        const t = try base.put(alloc, null, key, "v");
        base.release(alloc);
        base = t;
    }

    const Worker = struct {
        shared: *const Tree,
        gpa: Allocator,
        ok: bool = true,

        fn run(self: *@This()) void {
            // Each iteration takes an O(1) snapshot (retaining the shared root)
            // and releases it, racing every other worker on the same nodes'
            // refcounts. A lost update would either free a still-shared node
            // (allocator double-free) or wrap the counter (overflow panic).
            for (0..2000) |_| {
                var snap = self.shared.snapshot();
                if (snap.get(null, "128") == null) self.ok = false;
                snap.release(self.gpa);
            }
        }
    };

    var workers: [8]Worker = undefined;
    for (&workers) |*w| w.* = .{ .shared = &base, .gpa = alloc };
    var threads: [8]std.Thread = undefined;
    for (&threads, &workers) |*t, *w| t.* = try std.Thread.spawn(.{}, Worker.run, .{w});
    for (&threads) |t| t.join();

    for (&workers) |w| try testing.expect(w.ok);
    // The base must survive with exactly its original single reference: every
    // worker snapshot was released, so a corrupted count would have freed nodes
    // out from under `base` (caught by the allocator on the final release).
    try expectGet(base, null, "128", "v");
    try testing.expectEqual(@as(usize, 256), base.count());
}
