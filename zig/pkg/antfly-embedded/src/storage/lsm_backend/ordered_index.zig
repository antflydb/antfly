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

            /// Only a private unique node may be refreshed under admission.
            /// Caller freezes its entry, child links and child summaries until
            /// completion and retains every operand/payload authority.
            pub const Refresh = if (Summary != void and @hasDecl(SummaryPolicy, "Preparation")) struct {
                node: *Node,
                left: ?*Node,
                right: ?*Node,
                count: usize,
                height: u8,
                bytes: u64,
                left_count: usize,
                right_count: usize,
                left_height: u8,
                right_height: u8,
                left_bytes: u64,
                right_bytes: u64,
                preparation: SummaryPolicy.Preparation,
                committed: bool = false,
                fn stable(self: *const @This()) bool {
                    const n = self.node;
                    return n.refs.load(.acquire) == 1 and n.left == self.left and n.right == self.right and
                        self.preparation.matches(n.entry, if (n.left) |child| child.summary else .{}, if (n.right) |child| child.summary else .{}) and
                        size(n.left) == self.left_count and size(n.right) == self.right_count and
                        depth(n.left) == self.left_height and depth(n.right) == self.right_height and
                        (if (n.left) |child| child.bytes else @as(u64, 0)) == self.left_bytes and
                        (if (n.right) |child| child.bytes else @as(u64, 0)) == self.right_bytes;
                }
                pub fn resumeWithAdmission(self: *@This(), original: ?@import("../../common/read_admission.zig").Port) !void {
                    if (original) |port| try port.checkpoint();
                    if (!self.stable()) return error.CompactionPlanningStale;
                    if (self.committed) return;
                    const summary = try self.preparation.resumeWithAdmission(original);
                    // A progress callback may have invalidated the private shape.
                    if (!self.stable()) return error.CompactionPlanningStale;
                    self.node.count = self.count;
                    self.node.height = self.height;
                    self.node.bytes = self.bytes;
                    self.node.summary = summary;
                    self.committed = true;
                }
            } else void;
            pub fn prepareRefresh(self: *Node) !Refresh {
                if (comptime Refresh == void) @compileError("summary policy has no retained preparation");
                if (self.refs.load(.acquire) != 1) return error.ResourceBudgetExceeded;
                const left_bytes = if (self.left) |n| n.bytes else 0;
                const right_bytes = if (self.right) |n| n.bytes else 0;
                return .{ .node = self, .left = self.left, .right = self.right, .count = 1 + size(self.left) + size(self.right), .height = 1 + @max(depth(self.left), depth(self.right)), .bytes = @sizeOf(Node) + self.entry.retainedBytes() + left_bytes + right_bytes, .left_count = size(self.left), .right_count = size(self.right), .left_height = depth(self.left), .right_height = depth(self.right), .left_bytes = left_bytes, .right_bytes = right_bytes, .preparation = .init(self.entry, if (self.left) |n| n.summary else .{}, if (self.right) |n| n.summary else .{}) };
            }

            fn refresh(self: *Node) void {
                if (comptime Refresh != void) {
                    var preparation = self.prepareRefresh() catch unreachable;
                    preparation.resumeWithAdmission(null) catch unreachable;
                    return;
                }
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
            spare_offset: usize = 0,
            retiring_node: if (@hasDecl(Entry, "Retirement")) ?*Node else void = if (@hasDecl(Entry, "Retirement")) null else {},
            retiring_entry: if (@hasDecl(Entry, "Retirement")) ?Entry.Retirement else void = if (@hasDecl(Entry, "Retirement")) null else {},

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
                return self.stepWithAdmission(allocator, credits, null) catch unreachable;
            }
            /// The original port reaches physical entry work as well as node
            /// release. Refusal retains the detached node and entry cursor.
            pub fn stepWithAdmission(self: *@This(), allocator: std.mem.Allocator, credits: *usize, original: ?@import("../../common/read_admission.zig").Port) !bool {
                if (original) |port| try port.checkpoint();
                if (self.complete) return true;
                while (true) {
                    if (comptime @hasDecl(Entry, "Retirement")) {
                        if (self.retiring_node) |node| {
                            const finished = if (comptime @hasDecl(Entry.Retirement, "stepWithAdmission"))
                                if (original != null) try self.retiring_entry.?.stepWithAdmission(allocator, credits, original) else self.retiring_entry.?.step(allocator, credits)
                            else
                                self.retiring_entry.?.step(allocator, credits);
                            if (!finished) return false;
                            if (node.account) |account| account.discharge(@sizeOf(Node));
                            allocator.destroy(node);
                            self.retiring_node = null;
                            self.retiring_entry = null;
                        }
                    }
                    if (credits.* == 0 or self.len == 0) break;
                    if (original) |port| try port.checkpoint();
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
                    if (comptime @hasDecl(Entry, "Retirement")) {
                        self.retiring_node = node;
                        self.retiring_entry = .init(node.entry);
                    } else {
                        node.entry.deinit(allocator);
                        if (node.account) |account| account.discharge(@sizeOf(Node));
                        allocator.destroy(node);
                    }
                }
                if (self.len != 0) return false;
                while (credits.* != 0) {
                    if (original) |port| try port.checkpoint();
                    const node = self.owned.spare.pop() orelse break;
                    credits.* -= 1;
                    if (self.owned.account) |account| account.discharge(@sizeOf(Node));
                    allocator.destroy(node);
                }
                if (self.owned.spare.items.len != 0) return false;
                const vector = std.mem.sliceAsBytes(self.owned.spare.allocatedSlice());
                while (self.spare_offset < vector.len) {
                    if (credits.* == 0) return false;
                    if (original) |port| try port.checkpoint();
                    credits.* -= 1;
                    const end = @min(vector.len, self.spare_offset + 16 * 1024);
                    @memset(vector[self.spare_offset..end], undefined);
                    self.spare_offset = end;
                }
                if (vector.len != 0) allocator.rawFree(vector, .of(*Node), @returnAddress());
                self.owned.spare = .empty;
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
            // ArrayList growth includes a cache-line minimum, which can exceed
            // twice a small node count (notably on 128-byte cache-line hosts).
            // Include both vector allocations during a possible relocation.
            const capacity = if (needed == 0) 0 else std.ArrayListUnmanaged(*Node).growCapacity(needed);
            const vectors = try std.math.mul(u64, capacity, 2 * @sizeOf(*Node));
            const nodes = try std.math.mul(u64, needed, @sizeOf(Node));
            return std.math.add(u64, @sizeOf(Account), try std.math.add(u64, nodes, vectors));
        }

        /// Additional resident memory for one private fork edited in place,
        /// replenishing only one edit's spare frontier at a time. No intermediate
        /// candidate may be retained/published: each old node is then copied at
        /// most once, and each insertion adds at most one private tree node.
        pub fn incrementalPublicationMemoryBound(self: *const Self, edits: usize) !u64 {
            const maximum_size = try std.math.add(usize, size(self.root), edits);
            const tree_nodes = @min(try self.preparedNodeCount(edits), maximum_size);
            // An AVL tree's height is below twice its node-count bit length.
            // Include all one-edit spares even at the final grown tree height.
            const height: usize = if (maximum_size == 0) 0 else 2 * (@as(usize, std.math.log2_int(usize, maximum_size)) + 1);
            const frontier = if (edits == 0) 0 else 3 * height + 4;
            const needed = try std.math.add(usize, tree_nodes, frontier);
            const capacity = if (frontier == 0) 0 else std.ArrayListUnmanaged(*Node).growCapacity(frontier);
            const vectors = try std.math.mul(u64, capacity, 2 * @sizeOf(*Node));
            const nodes = try std.math.mul(u64, needed, @sizeOf(Node));
            return std.math.add(u64, @sizeOf(Account), try std.math.add(u64, nodes, vectors));
        }

        pub fn prepareEdits(self: *Self, allocator: std.mem.Allocator, edits: usize) !void {
            try self.prepareEditsControlled(allocator, edits, null);
        }

        /// One insertion, or an endpoint removal followed by insertion. Retain
        /// every prepared node/vector across refusal. Never call mid-traversal.
        pub fn prepareEditsWithAdmission(self: *Self, allocator: std.mem.Allocator, edits: usize, original: @import("../../common/read_admission.zig").Port) !void {
            if (edits > 2) return error.InvalidArgument;
            try self.prepareEditsControlled(allocator, edits, original);
        }
        pub fn prepareWithAdmission(self: *Self, allocator: std.mem.Allocator, original: @import("../../common/read_admission.zig").Port) !void {
            try self.prepareEditsWithAdmission(allocator, 1, original);
        }
        fn prepareEditsControlled(self: *Self, allocator: std.mem.Allocator, edits: usize, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (edits == 0) return;
            const needed = try self.preparedNodeCount(edits);
            if (self.account == null) self.account = try Account.create(allocator);
            if (original) |port| try port.checkpoint();
            try self.spare.ensureTotalCapacity(allocator, needed);
            while (self.spare.items.len < needed) {
                if (original) |port| try port.checkpoint();
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

        /// Borrows an exclusively owned, unpublished tree and its prepared
        /// node pool. Entry retain/retainedBytes and policies without a retained
        /// summary preparation must have bounded, allocation-free operations.
        /// Every reachable subtree remains owned by the tree at every pause.
        pub const Rebalance = struct {
            tree: *Self,
            slot: *?*Node,
            stage: enum { root, plan, inner_lower, inner_upper, outer, outer_lower, outer_upper, done } = .root,
            refresh: if (Node.Refresh != void) ?Node.Refresh else void = if (Node.Refresh != void) null else {},
            lower: ?*Node = null,
            upper: ?*Node = null,
            left: bool = false,

            pub fn init(tree: *Self, slot: *?*Node) @This() {
                return .{ .tree = tree, .slot = slot };
            }
            fn refreshNode(self: *@This(), node: *Node, original: ?@import("../../common/read_admission.zig").Port) !void {
                if (comptime Node.Refresh != void) {
                    if (self.refresh == null) self.refresh = try node.prepareRefresh();
                    try self.refresh.?.resumeWithAdmission(original);
                    self.refresh = null;
                } else {
                    if (original) |port| try port.checkpoint();
                    if (node.refs.load(.acquire) != 1) return error.ResourceBudgetExceeded;
                    node.refresh();
                }
            }
            // Up to two copies and three link assignments. The checkpoint and
            // pool check precede all ownership changes; there is no fallible
            // gap between retaining a copy and installing its owning link.
            fn rotate(self: *@This(), allocator: std.mem.Allocator, slot: *?*Node, left: bool, original: ?@import("../../common/read_admission.zig").Port) !void {
                if (original) |port| try port.checkpoint();
                const old = slot.* orelse return error.CompactionPlanningStale;
                const pivot = (if (left) old.right else old.left) orelse return error.CompactionPlanningStale;
                const copy_root = old.refs.load(.acquire) != 1;
                const copy_pivot = copy_root or pivot.refs.load(.acquire) != 1;
                const needed = @as(usize, @intFromBool(copy_root)) + @intFromBool(copy_pivot);
                if (self.tree.spare.items.len < needed) return error.ResourceBudgetExceeded;
                const root = self.tree.unique(allocator, old);
                const top = self.tree.unique(allocator, if (left) root.right.? else root.left.?);
                if (left) {
                    root.right = top.left;
                    top.left = root;
                } else {
                    root.left = top.right;
                    top.right = root;
                }
                slot.* = top;
                self.lower = root;
                self.upper = top;
            }
            pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
                if (original) |port| try port.checkpoint();
                while (self.stage != .done) {
                    if (original) |port| try port.checkpoint();
                    switch (self.stage) {
                        .root => {
                            try self.refreshNode(self.slot.* orelse return error.CompactionPlanningStale, original);
                            self.stage = .plan;
                        },
                        .plan => {
                            const root = self.slot.*.?;
                            const balance = @as(i16, depth(root.left)) - @as(i16, depth(root.right));
                            if (balance >= -1 and balance <= 1) {
                                self.stage = .done;
                                continue;
                            }
                            self.left = balance < -1;
                            const child = if (self.left) root.right.? else root.left.?;
                            const inner = if (self.left) depth(child.left) > depth(child.right) else depth(child.right) > depth(child.left);
                            if (inner) {
                                try self.rotate(allocator, if (self.left) &root.right else &root.left, !self.left, original);
                                self.stage = .inner_lower;
                            } else self.stage = .outer;
                        },
                        .inner_lower, .outer_lower => {
                            try self.refreshNode(self.lower.?, original);
                            self.stage = if (self.stage == .inner_lower) .inner_upper else .outer_upper;
                        },
                        .inner_upper, .outer_upper => {
                            try self.refreshNode(self.upper.?, original);
                            self.stage = if (self.stage == .inner_upper) .outer else .done;
                        },
                        .outer => {
                            try self.rotate(allocator, self.slot, self.left, original);
                            self.stage = .outer_lower;
                        },
                        .done => unreachable,
                    }
                }
            }
        };

        /// A rank belongs to this exact private root. Caller pins the entry,
        /// excludes other edits/readers, prepays prepareEdits(1), and retains
        /// this cursor until completion. Rejected work owns all path copies in
        /// the tree; abort retires that entire private tree before the cursor.
        /// Replacement moves the old entry to retired without invoking its
        /// potentially unbounded destructor. The caller must take and retire it.
        pub const RankInsertion = struct {
            tree: *Self,
            entry: Entry,
            rank: usize,
            replace: bool,
            slot: *?*Node,
            path: [2 * @bitSizeOf(usize)]*?*Node = undefined,
            len: usize = 0,
            balancing: ?Rebalance = null,
            retired: ?Entry = null,
            stage: enum { descend, unwind, done } = .descend,

            pub fn init(tree: *Self, entry: Entry, rank: usize, replace: bool) !@This() {
                if (rank > size(tree.root) or (replace and rank == size(tree.root))) return error.InvalidArgument;
                return .{ .tree = tree, .entry = entry, .rank = rank, .replace = replace, .slot = &tree.root };
            }
            pub fn takeRetired(self: *@This()) !?Entry {
                // After abort, the caller first fully retires the private tree.
                if (self.stage != .done and self.tree.root != null) return error.ResourceBudgetExceeded;
                const out = self.retired;
                self.retired = null;
                return out;
            }
            pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
                if (original) |port| try port.checkpoint();
                while (self.stage != .done) {
                    if (original) |port| try port.checkpoint();
                    switch (self.stage) {
                        .descend => {
                            if (self.len == self.path.len) return error.ResourceBudgetExceeded;
                            if (self.slot.*) |old| {
                                if (old.refs.load(.acquire) != 1 and self.tree.spare.items.len == 0) return error.ResourceBudgetExceeded;
                                const node = self.tree.unique(allocator, old);
                                self.slot.* = node;
                                self.path[self.len] = self.slot;
                                self.len += 1;
                                const left_count = size(node.left);
                                if (self.rank < left_count or (self.rank == left_count and !self.replace)) {
                                    self.slot = &node.left;
                                } else if (self.rank > left_count) {
                                    self.rank -= left_count + 1;
                                    self.slot = &node.right;
                                } else {
                                    self.retired = node.entry;
                                    node.entry = self.entry.retainShared();
                                    self.stage = .unwind;
                                }
                            } else {
                                if (self.replace or self.rank != 0) return error.CompactionPlanningStale;
                                const node = self.tree.spare.pop() orelse return error.ResourceBudgetExceeded;
                                node.* = .{ .account = self.tree.account, .entry = self.entry.retainShared() };
                                self.slot.* = node;
                                self.path[self.len] = self.slot;
                                self.len += 1;
                                self.stage = .unwind;
                            }
                        },
                        .unwind => {
                            if (self.len == 0) {
                                self.stage = .done;
                                continue;
                            }
                            if (self.balancing == null) self.balancing = Rebalance.init(self.tree, self.path[self.len - 1]);
                            try self.balancing.?.resumeWithAdmission(allocator, original);
                            self.balancing = null;
                            self.len -= 1;
                        },
                        .done => unreachable,
                    }
                }
            }
        };

        /// Legacy comparison and retirement driver. Structural insertion and
        /// balancing share the retained engine used by admitted private edits.
        pub fn putPrepared(self: *Self, allocator: std.mem.Allocator, entry: Entry) void {
            const rank = if (self.root) |root| Node.lowerBound(root, entry) else 0;
            const replace = rank < size(self.root) and compare(Node.at(self.root.?, rank), entry) == .eq;
            self.putPreparedAtRank(allocator, entry, rank, replace);
        }
        pub fn putPreparedAtRank(self: *Self, allocator: std.mem.Allocator, entry: Entry, rank: usize, replace: bool) void {
            var cursor = RankInsertion.init(self, entry, rank, replace) catch unreachable;
            cursor.resumeWithAdmission(allocator, null) catch unreachable;
            if (cursor.takeRetired() catch unreachable) |value| {
                var retired = value;
                retired.deinit(allocator);
            }
        }

        /// Borrows a private tree and its prepaid node pool, like RankInsertion.
        /// The rank belongs to the exact initial root. Every path copy stays
        /// reachable during rejection; no entry destructor runs during resume.
        /// A detached node owns the removed entry until every stale ancestor
        /// summary has been replaced, or the entire aborted tree is retired.
        pub const RankRemoval = struct {
            tree: *Self,
            rank: usize,
            slot: *?*Node,
            path: [2 * @bitSizeOf(usize)]*?*Node = undefined,
            len: usize = 0,
            target: ?*Node = null,
            detached: ?*Node = null,
            balancing: ?Rebalance = null,
            stage: enum { descend, successor, unwind, done } = .descend,

            pub fn init(tree: *Self, rank: usize) !@This() {
                if (rank >= size(tree.root)) return error.InvalidArgument;
                return .{ .tree = tree, .rank = rank, .slot = &tree.root };
            }
            /// Returned node is retirement-only, never a readable index root.
            /// Its entry may require separately admitted physical retirement.
            /// Caller may wrap it in Reclaimer with an otherwise empty tree.
            pub fn takeRetired(self: *@This()) !?*Node {
                if (self.stage != .done and self.tree.root != null) return error.ResourceBudgetExceeded;
                const out = self.detached;
                self.detached = null;
                return out;
            }
            fn detach(self: *@This(), node: *Node, child: ?*Node) void {
                self.slot.* = child;
                node.left = null;
                node.right = null;
                node.count = 1;
                node.height = 1;
                node.bytes = @sizeOf(Node) + node.entry.retainedBytes();
                self.detached = node;
                self.stage = .unwind;
            }
            pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
                if (original) |port| try port.checkpoint();
                while (self.stage != .done) {
                    if (original) |port| try port.checkpoint();
                    switch (self.stage) {
                        .descend, .successor => {
                            if (self.len == self.path.len) return error.ResourceBudgetExceeded;
                            const old = self.slot.* orelse return error.CompactionPlanningStale;
                            if (old.refs.load(.acquire) != 1 and self.tree.spare.items.len == 0) return error.ResourceBudgetExceeded;
                            const node = self.tree.unique(allocator, old);
                            self.slot.* = node;
                            if (self.stage == .successor) {
                                if (node.left != null) {
                                    self.path[self.len] = self.slot;
                                    self.len += 1;
                                    self.slot = &node.left;
                                } else {
                                    // Transfer both entry owners without a retain/drop
                                    // or rejection gap. The target keeps the successor;
                                    // the quarantined node keeps the removed entry.
                                    const entry = self.target.?.entry;
                                    self.target.?.entry = node.entry;
                                    node.entry = entry;
                                    self.detach(node, node.right);
                                }
                            } else {
                                const left_count = size(node.left);
                                if (self.rank == left_count and (node.left == null or node.right == null)) {
                                    self.detach(node, node.left orelse node.right);
                                    continue;
                                }
                                self.path[self.len] = self.slot;
                                self.len += 1;
                                if (self.rank < left_count) {
                                    self.slot = &node.left;
                                } else if (self.rank > left_count) {
                                    self.rank -= left_count + 1;
                                    self.slot = &node.right;
                                } else {
                                    self.target = node;
                                    self.slot = &node.right;
                                    self.stage = .successor;
                                }
                            }
                        },
                        .unwind => {
                            if (self.len == 0) {
                                self.stage = .done;
                                continue;
                            }
                            if (self.balancing == null) self.balancing = Rebalance.init(self.tree, self.path[self.len - 1]);
                            try self.balancing.?.resumeWithAdmission(allocator, original);
                            self.balancing = null;
                            self.len -= 1;
                        },
                        .done => unreachable,
                    }
                }
            }
        };

        /// Legacy lookup and retirement drive the same retained removal engine.
        pub fn removePrepared(self: *Self, allocator: std.mem.Allocator, entry: Entry) void {
            const root = self.root orelse return;
            const rank = Node.lowerBound(root, entry);
            if (rank == root.count or compare(Node.at(root, rank), entry) != .eq) return;
            self.removePreparedAtRank(allocator, rank);
        }
        pub fn removePreparedAtRank(self: *Self, allocator: std.mem.Allocator, rank: usize) void {
            var cursor = RankRemoval.init(self, rank) catch unreachable;
            cursor.resumeWithAdmission(allocator, null) catch unreachable;
            if (cursor.takeRetired() catch unreachable) |node| node.release(allocator);
        }

        /// Borrowed immutable-root search. Comparison supplies init(Entry,
        /// Entry) and resumeWithAdmission(?Port). Caller pins root and probe;
        /// rank/match apply only to this exact root until publication.
        pub fn Search(comptime Comparison: type) type {
            return struct {
                root: ?*const Node,
                current: ?*const Node,
                probe: Entry,
                rank: usize = 0,
                matched: ?*const Node = null,
                comparison: ?Comparison = null,
                visited: usize = 0,
                done: bool = false,
                pub fn init(root: ?*const Node, probe: Entry) @This() {
                    return .{ .root = root, .current = root, .probe = probe };
                }
                pub fn resumeWithAdmission(self: *@This(), original: ?@import("../../common/read_admission.zig").Port) !void {
                    if (original) |port| try port.checkpoint();
                    if (self.done) return;
                    while (self.current) |node| {
                        if (original) |port| try port.checkpoint();
                        if (self.comparison == null) {
                            self.comparison = Comparison.init(node.entry, self.probe);
                            self.visited += 1;
                        }
                        const order = try self.comparison.?.resumeWithAdmission(original);
                        // No rejection gap between comparison and descent.
                        self.comparison = null;
                        switch (order) {
                            .lt => {
                                self.rank += size(node.left) + 1;
                                self.current = node.right;
                            },
                            .gt => self.current = node.left,
                            .eq => {
                                self.rank += size(node.left);
                                self.matched = node;
                                self.done = true;
                                return;
                            },
                        }
                    }
                    self.done = true;
                }
            };
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

test "controlled physical sync rank replacement retains retirement ownership across rejection and private tree abort" {
    const a = std.testing.allocator;
    const Stats = struct { refs: usize = 0, drops: usize = 0 };
    const E = struct {
        key: usize,
        stats: *Stats,
        pub fn retainShared(self: @This()) @This() {
            self.stats.refs += 1;
            return self;
        }
        pub fn deinit(self: @This(), _: std.mem.Allocator) void {
            self.stats.refs -= 1;
            self.stats.drops += 1;
        }
        pub fn retainedBytes(_: @This()) usize {
            return 0;
        }
        fn compare(x: @This(), y: @This()) std.math.Order {
            return std.math.order(x.key, y.key);
        }
    };
    const Tree = SummarizedIndex(E, E.compare, void);
    const Fault = struct {
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    var old: Stats = .{};
    var fresh: Stats = .{};
    {
        var base: Tree = .{};
        defer base.deinit(a);
        for (0..7) |key| {
            try base.prepare(a);
            base.putPrepared(a, .{ .key = key, .stats = &old });
        }
        var checkpoints: usize = 1;
        var target: usize = 0;
        while (target <= checkpoints) : (target += 1) {
            for ([_]bool{ false, true }) |abort| {
                if (target == 0 and abort) continue;
                var tree = base.fork();
                try tree.prepare(a);
                var edit = try Tree.RankInsertion.init(&tree, .{ .key = 0, .stats = &fresh }, 0, true);
                var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload: @import("../../common/read_admission.zig").Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                const port = @import("../../common/read_admission.zig").Port.init(&payload);
                const old_drops = old.drops;
                const new_drops = fresh.drops;
                var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                const result = edit.resumeWithAdmission(failing.allocator(), port);
                if (target == 0) {
                    try result;
                    checkpoints = fault.checks;
                } else try std.testing.expectError(fault.failure, result);
                try std.testing.expectEqual(old_drops, old.drops);
                try std.testing.expectEqual(new_drops, fresh.drops);
                if (abort) {
                    try std.testing.expectError(error.ResourceBudgetExceeded, edit.takeRetired());
                    tree.deinit(a);
                    if (try edit.takeRetired()) |entry| entry.deinit(a);
                } else {
                    try edit.resumeWithAdmission(failing.allocator(), port);
                    try std.testing.expectEqual(old_drops, old.drops);
                    try std.testing.expect(tree.root.?.at(0).stats == &fresh);
                    const retired = (try edit.takeRetired()).?;
                    try std.testing.expect(retired.stats == &old);
                    retired.deinit(a);
                    try std.testing.expect((try edit.takeRetired()) == null);
                    tree.deinit(a);
                }
                try std.testing.expect(base.root.?.at(0).stats == &old);
                try std.testing.expectEqual(@as(usize, 7), old.refs);
                try std.testing.expectEqual(@as(usize, 0), fresh.refs);
            }
        }
        try std.testing.expectError(error.InvalidArgument, Tree.RankInsertion.init(&base, .{ .key = 8, .stats = &fresh }, 8, false));
        try std.testing.expectError(error.InvalidArgument, Tree.RankInsertion.init(&base, .{ .key = 7, .stats = &fresh }, 7, true));
    }
    try std.testing.expectEqual(@as(usize, 0), old.refs);
    var empty: Tree = .{};
    defer empty.deinit(a);
    var edit = try Tree.RankInsertion.init(&empty, .{ .key = 0, .stats = &fresh }, 0, false);
    try std.testing.expectError(error.ResourceBudgetExceeded, edit.resumeWithAdmission(a, null));
    try std.testing.expect(empty.root == null and fresh.refs == 0);
    try empty.prepare(a);
    try edit.resumeWithAdmission(a, null);
    try std.testing.expectEqual(@as(usize, 1), empty.root.?.count);
}

test "controlled physical sync retained rank removal preserves AVL ordering ownership and every original rejection" {
    const a = std.testing.allocator;
    const Stats = struct { refs: usize = 0, drops: usize = 0 };
    const E = struct {
        key: usize,
        stats: *Stats,
        pub fn retainShared(self: @This()) @This() {
            self.stats.refs += 1;
            return self;
        }
        pub fn deinit(self: @This(), _: std.mem.Allocator) void {
            self.stats.refs -= 1;
            self.stats.drops += 1;
        }
        pub fn retainedBytes(_: @This()) usize {
            return 0;
        }
        fn compare(x: @This(), y: @This()) std.math.Order {
            return std.math.order(x.key, y.key);
        }
    };
    const Tree = SummarizedIndex(E, E.compare, void);
    const Fault = struct {
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    const Check = struct {
        fn shape(node: ?*const Tree.Node) !struct { count: usize, height: u8, bytes: u64 } {
            const n = node orelse return .{ .count = 0, .height = 0, .bytes = 0 };
            const left = try shape(n.left);
            const right = try shape(n.right);
            const count = 1 + left.count + right.count;
            const height = 1 + @max(left.height, right.height);
            const bytes = @sizeOf(Tree.Node) + left.bytes + right.bytes;
            try std.testing.expectEqual(count, n.count);
            try std.testing.expectEqual(height, n.height);
            try std.testing.expectEqual(bytes, n.bytes);
            try std.testing.expect(@abs(@as(i16, left.height) - @as(i16, right.height)) <= 1);
            return .{ .count = count, .height = height, .bytes = bytes };
        }
        fn run(keys: []const usize, rank: usize) !void {
            var stats: Stats = .{};
            {
                var base: Tree = .{};
                defer base.deinit(a);
                for (keys) |key| {
                    try base.prepare(a);
                    base.putPrepared(a, .{ .key = key, .stats = &stats });
                }
                const root = base.root.?;
                const removed = root.at(rank).key;
                var checkpoints: usize = 1;
                var target: usize = 0;
                var used: usize = 0;
                while (target <= checkpoints) : (target += 1) {
                    for ([_]bool{ false, true }) |abort| {
                        if (target == 0 and abort) continue;
                        var tree = base.fork();
                        try tree.prepare(a);
                        const spare = tree.spare.items.len;
                        var edit = try Tree.RankRemoval.init(&tree, rank);
                        var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                            0 => error.Canceled,
                            1 => error.DeadlineExceeded,
                            else => error.FileNotFound,
                        } };
                        const payload: @import("../../common/read_admission.zig").Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                        const port = @import("../../common/read_admission.zig").Port.init(&payload);
                        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                        const drops = stats.drops;
                        const result = edit.resumeWithAdmission(failing.allocator(), port);
                        if (target == 0) {
                            try result;
                            checkpoints = fault.checks;
                            used = spare - tree.spare.items.len;
                        } else try std.testing.expectError(fault.failure, result);
                        try std.testing.expectEqual(drops, stats.drops);
                        if (abort) {
                            if (tree.root != null) try std.testing.expectError(error.ResourceBudgetExceeded, edit.takeRetired());
                            tree.deinit(a);
                            if (try edit.takeRetired()) |node| node.release(a);
                        } else {
                            try edit.resumeWithAdmission(failing.allocator(), port);
                            try std.testing.expectEqual(drops, stats.drops);
                            try std.testing.expectEqual(used, spare - tree.spare.items.len);
                            const shape_result = try shape(tree.root);
                            try std.testing.expectEqual(keys.len - 1, shape_result.count);
                            for (0..keys.len - 1) |i| try std.testing.expectEqual(root.at(if (i < rank) i else i + 1).key, tree.root.?.at(i).key);
                            const retired = (try edit.takeRetired()).?;
                            try std.testing.expectEqual(removed, retired.entry.key);
                            try std.testing.expect(retired.left == null and retired.right == null and retired.refs.load(.acquire) == 1);
                            retired.release(a);
                            try std.testing.expect((try edit.takeRetired()) == null);
                            const saved = tree.root;
                            fault.at = fault.checks;
                            try std.testing.expectError(fault.failure, edit.resumeWithAdmission(failing.allocator(), port));
                            try std.testing.expect(tree.root == saved);
                            tree.deinit(a);
                        }
                        try std.testing.expect(base.root == root);
                        try std.testing.expectEqual(keys.len, root.count);
                        try std.testing.expectEqual(keys.len, stats.refs);
                        _ = try shape(base.root);
                    }
                }
                var unprepared = base.fork();
                defer unprepared.deinit(a);
                var refused = try Tree.RankRemoval.init(&unprepared, rank);
                try std.testing.expectError(error.ResourceBudgetExceeded, refused.resumeWithAdmission(a, null));
                try std.testing.expect(unprepared.root == root and refused.detached == null);
                try std.testing.expectError(error.InvalidArgument, Tree.RankRemoval.init(&base, keys.len));
            }
            try std.testing.expectEqual(@as(usize, 0), stats.refs);
        }
    };
    // Single and double deletion rotations in both directions.
    try Check.run(&.{ 3, 2, 4, 1 }, 3);
    try Check.run(&.{ 2, 1, 3, 4 }, 0);
    try Check.run(&.{ 3, 1, 4, 2 }, 3);
    try Check.run(&.{ 2, 1, 4, 3 }, 0);
    // Root/leaf, one child, and direct/deep in-order successor ownership.
    try Check.run(&.{1}, 0);
    try Check.run(&.{ 1, 2 }, 0);
    try Check.run(&.{ 2, 1 }, 1);
    try Check.run(&.{ 2, 1, 3 }, 1);
    try Check.run(&.{ 3, 1, 5, 0, 2, 4, 6 }, 3);
    var keys: [31]usize = undefined;
    for (&keys, 0..) |*key, i| key.* = (i * 13) % keys.len;
    for (0..keys.len) |rank| try Check.run(&keys, rank);
    var empty: Tree = .{};
    try std.testing.expectError(error.InvalidArgument, Tree.RankRemoval.init(&empty, 0));
}
