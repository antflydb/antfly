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

//! Resumable overlap closure. The caller pins the directory for this job's
//! lifetime. Discovery never grows a flat vector or rehashes a global map:
//! each selected run gets one arena node, indexed by ID and read precedence.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const Run = @import("repository.zig").Run;
const state = @import("state.zig");

pub const Job = struct {
    const Node = struct {
        handle: Directory.Handle,
        left: [2]?*Node = .{ null, null },
        right: [2]?*Node = .{ null, null },
        height: [2]u8 = .{ 1, 1 },
    };
    arena: std.heap.ArenaAllocator,
    directory: *const Directory,
    roots: [2]?*Node = .{ null, null },
    count: usize = 0,
    bytes: u64 = 0,
    source_level: u32,
    visibility: u64 = 0,
    all_levels: bool,
    max_bytes: u64,
    lower_ns: ?[]const u8,
    lower: []const u8,
    upper_ns: ?[]const u8,
    upper: []const u8,
    cursor: ?Directory.OverlapCursor = null,
    changed: bool = true,
    phase: enum { discover, emit, done, oversized } = .discover,
    path: [2 * @bitSizeOf(usize)]*Node = undefined,
    depth: usize = 0,
    emitted: usize = 0,
    source_len: usize = 0,
    handles: ?[]Directory.Handle = null,
    indices: ?[]usize = null,
    projection_cursor: ?Directory.Cursor = null,
    visits: usize = 0,

    pub fn init(allocator: std.mem.Allocator, directory: *const Directory, seeds: []const Directory.Handle, max_bytes: u64, all_levels: bool) !Job {
        std.debug.assert(seeds.len != 0);
        const run = seeds[0].run;
        var job = Job{
            .arena = .init(allocator),
            .directory = directory,
            .source_level = run.level,
            .all_levels = all_levels,
            .max_bytes = max_bytes,
            .lower_ns = run.smallest_namespace_name,
            .lower = run.smallest_key,
            .upper_ns = run.largest_namespace_name,
            .upper = run.largest_key,
        };
        errdefer job.deinit(allocator);
        for (seeds) |seed| {
            try job.add(seed);
            job.visibility = @max(job.visibility, visibilityOf(seed.run));
        }
        return job;
    }

    pub fn deinit(self: *Job, allocator: std.mem.Allocator) void {
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(self.deinitStep(allocator, &credits));
        self.* = undefined;
    }

    pub fn deinitStep(self: *Job, allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.handles) |handles| {
            while (self.emitted != 0 and credits.* != 0) {
                self.emitted -= 1;
                credits.* -= 1;
                handles[self.emitted].release(allocator);
            }
            if (self.emitted != 0) return false;
            allocator.free(handles);
            self.handles = null;
        }
        if (self.indices) |indices| allocator.free(indices);
        self.indices = null;
        // Reclaim one arena allocation per credit using the arena's own
        // destructor, without depending on its private allocation header.
        for ([_]*@TypeOf(self.arena.state.used_list){ &self.arena.state.used_list, &self.arena.state.free_list }) |list| {
            while (list.*) |node| {
                if (credits.* == 0) return false;
                credits.* -= 1;
                list.* = node.next;
                node.next = null;
                var single = std.heap.ArenaAllocator.init(self.arena.child_allocator);
                single.state.used_list = node;
                single.deinit();
            }
        }
        return true;
    }

    fn visibilityOf(run: *const Run) u64 {
        return if (run.visibility_id == 0) run.id else run.visibility_id;
    }
    fn bound(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8) std.math.Order {
        const ns = state.compareNamespace(.{ .name = a_ns }, .{ .name = b_ns });
        return if (ns == .eq) std.mem.order(u8, a, b) else ns;
    }
    fn height(node: ?*Node, comptime index: usize) u8 {
        return if (node) |n| n.height[index] else 0;
    }
    fn refresh(node: *Node, comptime index: usize) void {
        node.height[index] = 1 + @max(height(node.left[index], index), height(node.right[index], index));
    }
    fn rotateLeft(node: *Node, comptime index: usize) *Node {
        const right = node.right[index].?;
        node.right[index] = right.left[index];
        right.left[index] = node;
        refresh(node, index);
        refresh(right, index);
        return right;
    }
    fn rotateRight(node: *Node, comptime index: usize) *Node {
        const left = node.left[index].?;
        node.left[index] = left.right[index];
        left.right[index] = node;
        refresh(node, index);
        refresh(left, index);
        return left;
    }
    fn insert(root: ?*Node, added: *Node, comptime index: usize) *Node {
        const node = root orelse return added;
        const less = if (index == 0) added.handle.run.id < node.handle.run.id else Directory.readLess({}, added.handle, node.handle);
        if (less) node.left[index] = insert(node.left[index], added, index) else node.right[index] = insert(node.right[index], added, index);
        refresh(node, index);
        const balance = @as(i16, height(node.left[index], index)) - height(node.right[index], index);
        if (balance > 1) {
            const left = node.left[index].?;
            if (height(left.right[index], index) > height(left.left[index], index)) node.left[index] = rotateLeft(left, index);
            return rotateRight(node, index);
        }
        if (balance < -1) {
            const right = node.right[index].?;
            if (height(right.left[index], index) > height(right.right[index], index)) node.right[index] = rotateRight(right, index);
            return rotateLeft(node, index);
        }
        return node;
    }
    fn add(self: *Job, handle: Directory.Handle) !void {
        var search = self.roots[0];
        while (search) |node| {
            if (handle.run.id == node.handle.run.id) return;
            search = if (handle.run.id < node.handle.run.id) node.left[0] else node.right[0];
        }
        const node = try self.arena.allocator().create(Node);
        node.* = .{ .handle = handle };
        inline for (0..2) |index| self.roots[index] = insert(self.roots[index], node, index);
        self.count += 1;
        self.bytes +|= handle.run.size_bytes;
        const run = handle.run;
        if (bound(run.smallest_namespace_name, run.smallest_key, self.lower_ns, self.lower) == .lt) {
            self.lower_ns = run.smallest_namespace_name;
            self.lower = run.smallest_key;
            self.changed = true;
        }
        if (bound(run.largest_namespace_name, run.largest_key, self.upper_ns, self.upper) == .gt) {
            self.upper_ns = run.largest_namespace_name;
            self.upper = run.largest_key;
            self.changed = true;
        }
    }
    fn descend(self: *Job, root: ?*Node) void {
        var current = root;
        while (current) |node| {
            self.path[self.depth] = node;
            self.depth += 1;
            current = node.left[1];
        }
    }

    /// Each credit visits one directory node or emits one selected handle.
    /// The caller chooses the slice and may cancel between calls. A zero
    /// credit call is observational and never advances or allocates.
    pub fn step(self: *Job, allocator: std.mem.Allocator, credits: usize) !bool {
        return self.stepUntil(allocator, credits, null);
    }

    /// A time deadline complements the node budget for long keys and slow
    /// allocators. Check between operations, never midway through an AVL edit.
    pub fn stepUntil(self: *Job, allocator: std.mem.Allocator, credits: usize, deadline_ns: ?u64) !bool {
        if (self.phase == .done or self.phase == .oversized) return true;
        var remaining = credits;
        while (remaining != 0) {
            if (deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return false;
            switch (self.phase) {
                .done, .oversized => return true,
                .discover => {
                    if (self.max_bytes != 0 and self.bytes > self.max_bytes) {
                        self.phase = .oversized;
                        return true;
                    }
                    if (self.cursor == null) {
                        self.changed = false;
                        self.cursor = self.directory.overlaps(self.lower_ns, self.lower, self.upper_ns, self.upper);
                    }
                    const before = remaining;
                    const next = self.cursor.?.next(&remaining);
                    self.visits += before - remaining;
                    if (next) |handle| {
                        const run = handle.run;
                        const older = self.source_level == 0 and run.level == 0 and visibilityOf(run) <= self.visibility;
                        if (self.all_levels or older or (self.source_level != std.math.maxInt(u32) and run.level == self.source_level + 1)) try self.add(handle);
                    } else if (self.cursor.?.done()) {
                        self.cursor = null;
                        if (self.changed) continue;
                        self.handles = try allocator.alloc(Directory.Handle, self.count);
                        self.indices = try allocator.alloc(usize, self.count);
                        // Dense selections can resolve positions with one
                        // resumable merge walk (N <= 4K), avoiding K rank
                        // searches. Sparse selections retain O(K log N) work.
                        if (self.count > self.directory.count() / 4) self.projection_cursor = self.directory.readCursor();
                        self.descend(self.roots[1]);
                        self.phase = .emit;
                    }
                },
                .emit => {
                    if (self.depth == 0) {
                        self.phase = .done;
                        return true;
                    }
                    remaining -= 1;
                    const node = self.path[self.depth - 1];
                    const rank = if (self.projection_cursor) |*cursor| blk: {
                        const candidate = cursor.next().?;
                        if (candidate.run.id != node.handle.run.id) continue;
                        break :blk cursor.rank - 1;
                    } else self.directory.rankOf(node.handle.run).?;
                    self.depth -= 1;
                    self.handles.?[self.emitted] = node.handle.retain();
                    self.indices.?[self.emitted] = rank;
                    self.emitted += 1;
                    self.source_len += @intFromBool(node.handle.run.level == self.source_level);
                    self.descend(node.right[1]);
                },
            }
        }
        return self.phase == .done or self.phase == .oversized;
    }
};
