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

const std = @import("std");
const Account = @import("memory_account.zig").Account;

/// Persistent rank-indexed AVL tree. A snapshot retains only its root. Writers
/// mutate unique paths and copy shared paths; no reader observes a mutation.
/// An edit reserves its worst-case node demand before publishing any changes.
/// Unused nodes stay in the writer's small pool, avoiding O(height) allocator
/// calls on ordinary unshared inserts and updates.
pub fn Index(comptime Entry: type, comptime compare: fn (Entry, Entry) std.math.Order) type {
    return SummarizedIndex(Entry, compare, Entry);
}

/// Multiple indexes can share an owned entry without duplicating unrelated
/// aggregates on every node. `void` selects a rank-only index.
pub fn SummarizedIndex(comptime Entry: type, comptime compare: fn (Entry, Entry) std.math.Order, comptime SummaryPolicy: type) type {
    return struct {
        const Self = @This();
        pub const Node = struct {
            const Summary = if (SummaryPolicy != void and @hasDecl(SummaryPolicy, "Summary")) SummaryPolicy.Summary else void;
            summary: Summary = if (Summary == void) {} else .{},
            account: ?*Account = null,
            refs: std.atomic.Value(usize) = .init(1),
            entry: Entry,
            left: ?*Node = null,
            right: ?*Node = null,
            count: usize = 1,
            height: u8 = 1,
            bytes: u64 = 0,

            pub fn retain(self: *Node) *Node {
                _ = self.refs.fetchAdd(1, .monotonic);
                return self;
            }

            pub fn release(self: *Node, allocator: std.mem.Allocator) void {
                if (self.refs.fetchSub(1, .acq_rel) != 1) return;
                if (self.left) |node| node.release(allocator);
                if (self.right) |node| node.release(allocator);
                self.entry.deinit(allocator);
                if (self.account) |account| account.discharge(@sizeOf(Node));
                allocator.destroy(self);
            }

            fn refresh(self: *Node) void {
                self.count = 1 + size(self.left) + size(self.right);
                self.height = 1 + @max(depth(self.left), depth(self.right));
                self.bytes = @sizeOf(Node) + self.entry.retainedBytes() +
                    (if (self.left) |node| node.bytes else 0) + (if (self.right) |node| node.bytes else 0);
                if (Summary != void) self.summary = SummaryPolicy.summarize(self.entry, if (self.left) |node| node.summary else .{}, if (self.right) |node| node.summary else .{});
            }

            pub fn at(root: *const Node, rank: usize) Entry {
                std.debug.assert(rank < root.count);
                var node = root;
                var offset = rank;
                while (true) {
                    const left_count = size(node.left);
                    if (offset < left_count) node = node.left.? else if (offset == left_count) return node.entry else {
                        offset -= left_count + 1;
                        node = node.right.?;
                    }
                }
            }

            pub fn lowerBound(root: *const Node, probe: Entry) usize {
                var current: ?*const Node = root;
                var rank: usize = 0;
                while (current) |node| {
                    if (compare(node.entry, probe) == .lt) {
                        rank += size(node.left) + 1;
                        current = node.right;
                    } else current = node.left;
                }
                return rank;
            }
        };

        root: ?*Node = null,
        account: ?*Account = null,
        spare: std.ArrayListUnmanaged(*Node) = .empty,

        /// Borrowed cursor: its owner pins the immutable root. AVL height is
        /// less than twice the key-count bit width. Sequential access walks
        /// each edge at most twice instead of rank-searching every row.
        pub const Cursor = struct {
            path: [2 * @bitSizeOf(usize)]*const Node = undefined,
            len: usize = 0,
            rank: usize = 0,
            root: ?*const Node = null,

            pub fn at(self: *@This(), root: *const Node, rank: usize) Entry {
                std.debug.assert(rank < root.count);
                if (self.root == root and self.len > 0) {
                    if (rank == self.rank) return self.path[self.len - 1].entry;
                    if (rank == self.rank + 1) {
                        var node = self.path[self.len - 1];
                        if (node.right) |right| {
                            self.push(right);
                            while (self.path[self.len - 1].left) |left| self.push(left);
                        } else {
                            self.len -= 1;
                            while (self.len > 0 and self.path[self.len - 1].right == node) {
                                node = self.path[self.len - 1];
                                self.len -= 1;
                            }
                        }
                        self.rank = rank;
                        return self.path[self.len - 1].entry;
                    }
                }
                self.root = root;
                self.len = 0;
                self.rank = rank;
                var node = root;
                var offset = rank;
                while (true) {
                    self.push(node);
                    const left_count = size(node.left);
                    if (offset < left_count) node = node.left.? else if (offset == left_count) return node.entry else {
                        offset -= left_count + 1;
                        node = node.right.?;
                    }
                }
            }

            fn push(self: *@This(), node: *const Node) void {
                self.path[self.len] = node;
                self.len += 1;
            }
        };

        fn size(node: ?*const Node) usize {
            return if (node) |value| value.count else 0;
        }
        fn depth(node: ?*const Node) u8 {
            return if (node) |value| value.height else 0;
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            if (self.root) |root| root.release(allocator);
            for (self.spare.items) |node| {
                if (self.account) |account| account.discharge(@sizeOf(Node));
                allocator.destroy(node);
            }
            self.spare.deinit(allocator);
            if (self.account) |account| account.release();
            self.* = .{};
        }

        /// An owned destruction continuation. A bounded DFS stack replaces
        /// recursive last-reference destruction; shared subtrees cost one
        /// credit regardless of their size. No allocation is needed to retire
        /// a root, including on OOM and cancellation paths.
        pub const Reclaimer = struct {
            owned: Self,
            pending: [2 * @bitSizeOf(usize)]*Node = undefined,
            len: usize = 0,
            complete: bool = false,

            pub fn init(owned: Self) @This() {
                var out = @This(){ .owned = owned };
                if (owned.root) |root| {
                    out.pending[0] = root;
                    out.len = 1;
                }
                out.owned.root = null;
                return out;
            }
            pub fn step(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
                if (self.complete) return true;
                while (credits.* != 0 and self.len != 0) {
                    credits.* -= 1;
                    self.len -= 1;
                    const node = self.pending[self.len];
                    if (node.refs.fetchSub(1, .acq_rel) != 1) continue;
                    if (node.left) |child| {
                        self.pending[self.len] = child;
                        self.len += 1;
                    }
                    if (node.right) |child| {
                        self.pending[self.len] = child;
                        self.len += 1;
                    }
                    node.entry.deinit(allocator);
                    if (node.account) |account| account.discharge(@sizeOf(Node));
                    allocator.destroy(node);
                }
                if (self.len != 0) return false;
                while (credits.* != 0) {
                    const node = self.owned.spare.pop() orelse break;
                    credits.* -= 1;
                    if (self.owned.account) |account| account.discharge(@sizeOf(Node));
                    allocator.destroy(node);
                }
                if (self.owned.spare.items.len != 0) return false;
                self.owned.spare.deinit(allocator);
                if (self.owned.account) |account| account.release();
                self.owned = .{};
                self.complete = true;
                return true;
            }
        };

        pub fn fork(self: *const Self) Self {
            return .{ .root = if (self.root) |root| root.retain() else null, .account = if (self.account) |account| account.retain() else null };
        }

        pub fn memoryBytes(self: *const Self) u64 {
            // Entry bytes are charged by the active table; only the index's
            // node allocations and spare-pointer vector are additional here.
            return (size(self.root) + self.spare.items.len) * @sizeOf(Node) + self.spare.capacity * @sizeOf(*Node);
        }

        pub fn prepare(self: *Self, allocator: std.mem.Allocator) !void {
            return self.prepareEdits(allocator, 1);
        }

        /// Reserve a publication's edits before changing any shared root.
        /// The AVL height bound includes growth caused by the entire batch.
        fn preparedNodeCount(self: *const Self, edits: usize) !usize {
            if (edits == 0) return 0;
            const growth = if (edits == 1) 0 else std.math.log2_int(usize, edits) + 1;
            return std.math.mul(usize, edits, 3 * (@as(usize, depth(self.root)) + growth) + 4);
        }

        /// Conservative allocation bound, including pointer-vector growth.
        pub fn prepareMemoryBound(self: *const Self, edits: usize) !u64 {
            const needed = try self.preparedNodeCount(edits);
            return std.math.add(u64, @sizeOf(Account), try std.math.mul(u64, needed, @sizeOf(Node) + 2 * @sizeOf(*Node)));
        }

        pub fn prepareEdits(self: *Self, allocator: std.mem.Allocator, edits: usize) !void {
            if (edits == 0) return;
            if (self.account == null) self.account = try Account.create(allocator);
            const needed = try self.preparedNodeCount(edits);
            try self.spare.ensureTotalCapacity(allocator, needed);
            while (self.spare.items.len < needed) {
                const node = try allocator.create(Node);
                self.account.?.charge(@sizeOf(Node));
                self.spare.appendAssumeCapacity(node);
            }
        }

        fn unique(self: *Self, allocator: std.mem.Allocator, node: *Node) *Node {
            if (node.refs.load(.acquire) == 1) return node;
            const copy = self.spare.pop().?;
            copy.* = .{ .account = self.account, .entry = node.entry.retainShared(), .left = if (node.left) |child| child.retain() else null, .right = if (node.right) |child| child.retain() else null, .count = node.count, .height = node.height, .bytes = node.bytes, .summary = node.summary };
            node.release(allocator);
            return copy;
        }

        fn rotateLeft(self: *Self, allocator: std.mem.Allocator, root: *Node) *Node {
            const pivot = self.unique(allocator, root.right.?);
            root.right = pivot.left;
            pivot.left = root;
            root.refresh();
            pivot.refresh();
            return pivot;
        }

        fn rotateRight(self: *Self, allocator: std.mem.Allocator, root: *Node) *Node {
            const pivot = self.unique(allocator, root.left.?);
            root.left = pivot.right;
            pivot.right = root;
            root.refresh();
            pivot.refresh();
            return pivot;
        }

        fn insert(self: *Self, allocator: std.mem.Allocator, old: ?*Node, entry: Entry) *Node {
            const root = if (old) |node| self.unique(allocator, node) else {
                const node = self.spare.pop().?;
                node.* = .{ .account = self.account, .entry = entry.retainShared() };
                node.refresh();
                return node;
            };
            switch (compare(entry, root.entry)) {
                .lt => root.left = self.insert(allocator, root.left, entry),
                .gt => root.right = self.insert(allocator, root.right, entry),
                .eq => {
                    root.entry.deinit(allocator);
                    root.entry = entry.retainShared();
                },
            }
            return self.rebalance(allocator, root);
        }

        fn rebalance(self: *Self, allocator: std.mem.Allocator, root: *Node) *Node {
            root.refresh();
            const balance = @as(i16, depth(root.left)) - @as(i16, depth(root.right));
            if (balance > 1) {
                const left = root.left.?;
                if (depth(left.right) > depth(left.left)) root.left = self.rotateLeft(allocator, self.unique(allocator, left));
                return self.rotateRight(allocator, root);
            }
            if (balance < -1) {
                const right = root.right.?;
                if (depth(right.left) > depth(right.right)) root.right = self.rotateRight(allocator, self.unique(allocator, right));
                return self.rotateLeft(allocator, root);
            }
            return root;
        }

        /// prepare() must precede every edit. No allocation or error is possible
        /// here, so the ordered and hash indexes can publish atomically.
        pub fn putPrepared(self: *Self, allocator: std.mem.Allocator, entry: Entry) void {
            self.root = self.insert(allocator, self.root, entry);
        }

        fn remove(self: *Self, allocator: std.mem.Allocator, old: ?*Node, entry: Entry) ?*Node {
            const root = self.unique(allocator, old orelse return null);
            switch (compare(entry, root.entry)) {
                .lt => root.left = self.remove(allocator, root.left, entry),
                .gt => root.right = self.remove(allocator, root.right, entry),
                .eq => {
                    if (root.left == null or root.right == null) {
                        const child = root.left orelse root.right;
                        root.left = null;
                        root.right = null;
                        root.release(allocator);
                        return child;
                    }
                    var successor = root.right.?;
                    while (successor.left) |left| successor = left;
                    const replacement = successor.entry.retainShared();
                    root.entry.deinit(allocator);
                    root.entry = replacement;
                    root.right = self.remove(allocator, root.right, replacement);
                },
            }
            return self.rebalance(allocator, root);
        }

        /// Like insertion, removal consumes only the nodes reserved by prepare.
        pub fn removePrepared(self: *Self, allocator: std.mem.Allocator, entry: Entry) void {
            self.root = self.remove(allocator, self.root, entry);
        }

        pub fn find(root: ?*const Node, probe: Entry) ?*const Node {
            var current = root;
            while (current) |node| switch (compare(probe, node.entry)) {
                .lt => current = node.left,
                .gt => current = node.right,
                .eq => return node,
            };
            return null;
        }

        /// Shared subtree identity avoids a complete merge walk, including
        /// when rotations moved a shared subtree to a different parent.
        pub fn changesSince(self: *const Self, previous: *const Self, visitor: anytype) !void {
            try removed(previous.root, self.root, visitor);
            try added(self.root, previous.root, visitor);
        }

        fn removed(root: ?*const Node, current: ?*const Node, visitor: anytype) !void {
            const node = root orelse return;
            const match = find(current, node.entry);
            if (match == node) return;
            try removed(node.left, current, visitor);
            if (match == null) try visitor.remove(node.entry);
            try removed(node.right, current, visitor);
        }

        fn added(root: ?*const Node, previous: ?*const Node, visitor: anytype) !void {
            const node = root orelse return;
            const match = find(previous, node.entry);
            if (match == node) return;
            try added(node.left, previous, visitor);
            if (match == null or !Entry.eql(node.entry, match.?.entry)) try visitor.put(node.entry);
            try added(node.right, previous, visitor);
        }
    };
}
