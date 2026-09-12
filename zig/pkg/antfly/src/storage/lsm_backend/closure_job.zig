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
const resource_manager = @import("../resource_manager.zig");

const TestFixture = struct {
    allocator: std.mem.Allocator,
    pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
    pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}

    fn put(self: *@This(), directory: *Directory, id: usize, level: u32, lower_ns: ?[]const u8, lower: u64, upper_ns: ?[]const u8, upper: u64) !void {
        var first: [8]u8 = undefined;
        var last: [8]u8 = undefined;
        std.mem.writeInt(u64, &first, lower, .big);
        std.mem.writeInt(u64, &last, upper, .big);
        try directory.put(self, .{ .id = id, .visibility_id = 1, .level = level, .size_bytes = 1, .path = @constCast("frontier.sst"), .smallest_namespace_name = @constCast(lower_ns), .smallest_key = &first, .largest_namespace_name = @constCast(upper_ns), .largest_key = &last, .entry_count = 1, .bloom_filter = null, .state = null });
    }
};

test "closure frontier visits chained overlaps once in both directions" {
    const allocator = std.testing.allocator;
    var fixture = TestFixture{ .allocator = allocator };
    for ([_]usize{ 1000, 2000, 4000 }) |count| {
        const directory = try Directory.create(allocator);
        defer directory.destroy(allocator);
        for (0..count) |i| try fixture.put(directory, i + 1, 0, null, i, null, i + 1);
        for ([_]usize{ 0, count / 2, count - 1 }) |seed| {
            const handle = directory.at(directory.rankOf(directory.byId(seed + 1).?).?);
            var job = try Job.init(allocator, directory, &.{handle}, 0, seed == count / 2);
            defer job.deinit(allocator);
            try std.testing.expect(!try job.step(allocator, 0));
            try std.testing.expect(!try job.stepUntil(allocator, 100, 0));
            try std.testing.expectEqual(@as(usize, 0), job.visits);
            const started = @import("antfly_platform").time.monotonicNs();
            var turns: usize = 0;
            const credits: usize = if (@import("builtin").mode == .ReleaseFast) 2048 else 1;
            while (!try job.step(allocator, credits)) {
                turns += 1;
                if (turns > 16 * count) return error.FrontierDidNotConverge;
            }
            try std.testing.expectEqual(count, job.count);
            try std.testing.expect(job.visits < 8 * count + 256);
            if (@import("builtin").mode == .ReleaseFast) std.debug.print("\nLSM frontier inputs={d} seed={d} visits={d} elapsed_ns={d}\n", .{ count, seed, job.visits, @import("antfly_platform").time.monotonicNs() - started });
        }
    }
}

test "closure frontier matches fixed point oracle across nested ranges namespaces and levels" {
    const allocator = std.testing.allocator;
    var fixture = TestFixture{ .allocator = allocator };
    for (0..24) |variant| {
        const directory = try Directory.create(allocator);
        defer directory.destroy(allocator);
        for (0..64) |i| {
            const namespace: ?[]const u8 = if (i % 3 == 0) null else if (i % 3 == 1) "" else "docs";
            const start = (i * 37 + variant * 13) % 256;
            const end = start + (i * 17 + variant) % 48;
            try fixture.put(directory, i + 1, @intCast(i % 4), namespace, start, if (i % 11 == 0) "docs" else namespace, end);
        }
        const all_levels = variant % 2 == 0;
        const handle = directory.at(variant);
        var job = try Job.init(allocator, directory, &.{handle}, 0, all_levels);
        defer job.deinit(allocator);
        var expected: [64]bool = @splat(false);
        expected[variant] = true;
        var lower_ns = job.lower_ns;
        var lower = job.lower;
        var upper_ns = job.upper_ns;
        var upper = job.upper;
        var changed = true;
        while (changed) {
            changed = false;
            for (0..64) |i| {
                if (expected[i]) continue;
                const run = directory.at(i).run;
                if (!all_levels and !(run.level == 0 and job.source_level == 0) and run.level != job.source_level + 1) continue;
                if (Job.bound(run.smallest_namespace_name, run.smallest_key, upper_ns, upper) == .gt or Job.bound(run.largest_namespace_name, run.largest_key, lower_ns, lower) == .lt) continue;
                expected[i] = true;
                changed = true;
                if (Job.bound(run.smallest_namespace_name, run.smallest_key, lower_ns, lower) == .lt) {
                    lower_ns = run.smallest_namespace_name;
                    lower = run.smallest_key;
                }
                if (Job.bound(run.largest_namespace_name, run.largest_key, upper_ns, upper) == .gt) {
                    upper_ns = run.largest_namespace_name;
                    upper = run.largest_key;
                }
            }
        }
        var turns: usize = 0;
        while (!try job.step(allocator, 1)) {
            turns += 1;
            if (turns > 4096) return error.FrontierDidNotConverge;
        }
        var actual: [64]bool = @splat(false);
        for (job.indices.?) |rank| actual[rank] = true;
        try std.testing.expectEqualSlices(bool, &expected, &actual);
    }
}

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
    left: ?Directory.FrontierCursor(true) = null,
    right: ?Directory.FrontierCursor(false) = null,
    frontier_right: bool = false,
    phase: enum { discover, frontier, emit, done, oversized } = .discover,
    path: [2 * @bitSizeOf(usize)]*Node = undefined,
    depth: usize = 0,
    emitted: usize = 0,
    source_len: usize = 0,
    handles: ?[]Directory.Handle = null,
    indices: ?[]usize = null,
    // Discovery scratch may use an independently owned budgeted arena. The
    // emitted arrays use the caller's allocator and carry their own credit
    // when ownership moves out of this job.
    output_manager: ?*resource_manager.ResourceManager = null,
    output_reservation: ?resource_manager.Reservation = null,
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
        if (self.output_reservation) |*lease| lease.release();
        self.output_reservation = null;
        return self.reclaimScratchStep(credits);
    }

    /// Retain emitted handles/ranks but retire the discovery-only arena.
    /// No discovery/emit operation may run after this handoff starts.
    pub fn reclaimScratchStep(self: *Job, credits: *usize) bool {
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
        }
        if (bound(run.largest_namespace_name, run.largest_key, self.upper_ns, self.upper) == .gt) {
            self.upper_ns = run.largest_namespace_name;
            self.upper = run.largest_key;
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

    fn consider(self: *Job, handle: Directory.Handle) !void {
        const run = handle.run;
        const older = self.source_level == 0 and run.level == 0 and visibilityOf(run) <= self.visibility;
        if (self.all_levels or older or (self.source_level != std.math.maxInt(u32) and run.level == self.source_level + 1)) try self.add(handle);
    }

    fn beginEmission(self: *Job, allocator: std.mem.Allocator) !void {
        if (self.output_manager) |manager| self.output_reservation = try manager.reserve(
            .lsm_table_builder_working_set,
            self.count * (@sizeOf(Directory.Handle) + @sizeOf(usize)),
        );
        self.handles = try allocator.alloc(Directory.Handle, self.count);
        self.indices = try allocator.alloc(usize, self.count);
        if (self.count > self.directory.count() / 4) self.projection_cursor = self.directory.readCursor();
        self.descend(self.roots[1]);
        self.phase = .emit;
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
            if ((self.phase == .discover or self.phase == .frontier) and self.max_bytes != 0 and self.bytes > self.max_bytes) {
                self.phase = .oversized;
                return true;
            }
            switch (self.phase) {
                .done, .oversized => return true,
                .discover => {
                    if (self.cursor == null) {
                        self.cursor = self.directory.overlaps(self.lower_ns, self.lower, self.upper_ns, self.upper);
                        self.left = .init(self.directory, self.lower_ns, self.lower);
                        self.right = .init(self.directory, self.upper_ns, self.upper);
                    }
                    const before = remaining;
                    const next = self.cursor.?.next(&remaining);
                    self.visits += before - remaining;
                    if (next) |handle| {
                        try self.consider(handle);
                    } else if (self.cursor.?.done()) {
                        self.cursor = null;
                        self.phase = .frontier;
                    }
                },
                .frontier => {
                    if (!self.frontier_right) {
                        const before = remaining;
                        const left = self.left.?.next(self.lower_ns, self.lower, &remaining);
                        self.visits += before - remaining;
                        if (left) |handle| {
                            try self.consider(handle);
                            continue;
                        }
                        if (self.left.?.caught_up) self.frontier_right = true;
                        continue;
                    }
                    const before_right = remaining;
                    const right = self.right.?.next(self.upper_ns, self.upper, &remaining);
                    self.visits += before_right - remaining;
                    if (right) |handle| {
                        try self.consider(handle);
                        self.frontier_right = false;
                        continue;
                    }
                    if (self.right.?.caught_up) try self.beginEmission(allocator);
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
