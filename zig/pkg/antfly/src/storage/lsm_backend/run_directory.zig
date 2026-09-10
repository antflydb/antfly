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

//! Persistent run metadata, separate from mutable cache hints in Backend.runs.
//! A publication copies only changed search paths. Readers pin a root in O(1)
//! and materialize their shared read/planning projection outside the writer lock.
const std = @import("std");
const repository = @import("repository.zig");
const state = @import("state.zig");
const Account = @import("memory_account.zig").Account;
const Run = repository.Run;
const generation_index = @import("generation_index.zig");

const Payload = struct {
    refs: std.atomic.Value(usize) = .init(1),
    run: Run,
    owner: *anyopaque,
    release_pin: *const fn (*anyopaque, *Run) void,
    account: *Account,
    bytes: usize,
};

const Entry = struct {
    run: *const Run,
    payload: ?*Payload = null,
    domain: []const u8 = "",

    pub const Summary = struct {
        tombstone_runs: usize = 0,
        unknown_tombstone_runs: usize = 0,
        gc_requested: bool = false,
        oldest_tombstone: u64 = std.math.maxInt(u64),
        newest_tombstone: u64 = 0,
    };
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        const deletes = (entry.run.tombstone_count orelse 0) != 0;
        return .{
            .tombstone_runs = left.tombstone_runs + right.tombstone_runs + @intFromBool(deletes),
            .unknown_tombstone_runs = left.unknown_tombstone_runs + right.unknown_tombstone_runs + @intFromBool(entry.run.tombstone_count == null),
            .gc_requested = left.gc_requested or right.gc_requested or (deletes and entry.run.gc_requested),
            .oldest_tombstone = @min(left.oldest_tombstone, right.oldest_tombstone, if (deletes) entry.run.oldest_tombstone_unix_ns else std.math.maxInt(u64)),
            .newest_tombstone = @max(left.newest_tombstone, right.newest_tombstone, if (deletes) entry.run.oldest_tombstone_unix_ns else 0),
        };
    }

    pub fn retainShared(self: Entry) Entry {
        _ = self.payload.?.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn deinit(self: Entry, allocator: std.mem.Allocator) void {
        const payload = self.payload orelse return;
        if (payload.refs.fetchSub(1, .acq_rel) != 1) return;
        payload.release_pin(payload.owner, &payload.run);
        payload.run.deinit(allocator);
        payload.account.discharge(payload.bytes);
        allocator.destroy(payload);
    }
    pub fn retainedBytes(_: Entry) usize {
        return 0;
    }
};

const BoundsSummary = struct {
    pub const Summary = struct { largest: ?*const Run = null };
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        var largest = entry.run;
        for ([_]?*const Run{ left.largest, right.largest }) |candidate| if (candidate) |run| {
            if (compareBound(largest.largest_namespace_name, largest.largest_key, run.largest_namespace_name, run.largest_key) == .lt) largest = run;
        };
        return .{ .largest = largest };
    }
};

fn compareBound(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8) std.math.Order {
    const ns = state.compareNamespace(.{ .name = a_ns }, .{ .name = b_ns });
    return if (ns == .eq) std.mem.order(u8, a, b) else ns;
}

fn compareDomain(a: Entry, b: Entry) std.math.Order {
    const ns = state.compareNamespace(.{ .name = a.run.smallest_namespace_name }, .{ .name = b.run.smallest_namespace_name });
    if (ns != .eq) return ns;
    const domain = std.mem.order(u8, a.domain, b.domain);
    return if (domain != .eq) domain else compare(a, b);
}

fn compareId(a: Entry, b: Entry) std.math.Order {
    return std.math.order(a.run.id, b.run.id);
}

fn compareBounds(a: Entry, b: Entry) std.math.Order {
    const ns = state.compareNamespace(.{ .name = a.run.smallest_namespace_name }, .{ .name = b.run.smallest_namespace_name });
    if (ns != .eq) return ns;
    const key = std.mem.order(u8, a.run.smallest_key, b.run.smallest_key);
    return if (key != .eq) key else compare(a, b);
}

fn compareEnds(a: Entry, b: Entry) std.math.Order {
    const order = compareBound(a.run.largest_namespace_name, a.run.largest_key, b.run.largest_namespace_name, b.run.largest_key);
    return if (order != .eq) order else compare(a, b);
}

pub const LevelAggregate = struct {
    level: u32,
    count: usize = 0,
    bytes: u64 = 0,
    tombstone_runs: usize = 0,
    pub fn retainShared(self: @This()) @This() {
        return self;
    }
    pub fn deinit(_: @This(), _: std.mem.Allocator) void {}
    pub fn retainedBytes(_: @This()) usize {
        return 0;
    }
};

fn compareLevel(a: LevelAggregate, b: LevelAggregate) std.math.Order {
    return std.math.order(a.level, b.level);
}

fn compare(a: Entry, b: Entry) std.math.Order {
    const lhs = a.run;
    const rhs = b.run;
    if (lhs.level != rhs.level) return std.math.order(lhs.level, rhs.level);
    if (lhs.level == 0) {
        const av = if (lhs.visibility_id == 0) lhs.id else lhs.visibility_id;
        const bv = if (rhs.visibility_id == 0) rhs.id else rhs.visibility_id;
        if (av != bv) return std.math.order(bv, av);
    }
    const ns = state.compareNamespace(.{ .name = lhs.smallest_namespace_name }, .{ .name = rhs.smallest_namespace_name });
    if (ns != .eq) return ns;
    const key = std.mem.order(u8, lhs.smallest_key, rhs.smallest_key);
    if (key != .eq) return key;
    return std.math.order(lhs.id, rhs.id);
}

pub const Directory = struct {
    pub fn runLess(a: *const Run, b: *const Run) bool {
        return compare(.{ .run = a }, .{ .run = b }) == .lt;
    }
    pub fn containsReadOrdered(handles: []const Handle, run: *const Run) bool {
        var lo: usize = 0;
        var hi = handles.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (runLess(handles[mid].run, run)) lo = mid + 1 else hi = mid;
        }
        return lo < handles.len and handles[lo].run.id == run.id;
    }
    const Tree = @import("ordered_index.zig").Index(Entry, compare);
    const IdTree = @import("ordered_index.zig").SummarizedIndex(Entry, compareId, void);
    const BoundsTree = @import("ordered_index.zig").SummarizedIndex(Entry, compareBounds, BoundsSummary);
    const EndsTree = @import("ordered_index.zig").SummarizedIndex(Entry, compareEnds, void);
    const LevelTree = @import("ordered_index.zig").Index(LevelAggregate, compareLevel);
    tree: Tree = .{},
    ids: IdTree = .{},
    bounds: BoundsTree = .{},
    ends: EndsTree = .{},
    levels: LevelTree = .{},
    generations: generation_index.Tree = .{},
    total_run_bytes: u64 = 0,
    memory_run_count: usize = 0,
    retired_next: ?*Directory = null,

    pub fn create(allocator: std.mem.Allocator) !*Directory {
        const self = try allocator.create(Directory);
        self.* = .{};
        return self;
    }
    pub fn fork(self: *const Directory, allocator: std.mem.Allocator) !*Directory {
        const out = try create(allocator);
        out.tree = self.tree.fork();
        out.ids = self.ids.fork();
        out.bounds = self.bounds.fork();
        out.ends = self.ends.fork();
        out.levels = self.levels.fork();
        out.generations = self.generations.fork();
        out.total_run_bytes = self.total_run_bytes;
        out.memory_run_count = self.memory_run_count;
        return out;
    }
    pub fn destroy(self: *Directory, allocator: std.mem.Allocator) void {
        self.destroyContents(allocator);
        allocator.destroy(self);
    }

    pub const Reclaimer = struct {
        directory: *Directory,
        tree: Tree.Reclaimer,
        ids: IdTree.Reclaimer,
        bounds: BoundsTree.Reclaimer,
        ends: EndsTree.Reclaimer,
        levels: LevelTree.Reclaimer,
        generations: generation_index.Tree.Reclaimer,

        pub fn init(directory: *Directory) @This() {
            directory.retainAccounting();
            return .{ .directory = directory, .tree = .init(directory.tree), .ids = .init(directory.ids), .bounds = .init(directory.bounds), .ends = .init(directory.ends), .levels = .init(directory.levels), .generations = .init(directory.generations) };
        }
        pub fn step(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
            return self.tree.step(allocator, credits) and self.ids.step(allocator, credits) and self.bounds.step(allocator, credits) and self.ends.step(allocator, credits) and self.levels.step(allocator, credits) and self.generations.step(allocator, credits);
        }
        /// Only after step reports completion, back under the accounting lock.
        pub fn finish(self: *@This(), allocator: std.mem.Allocator) void {
            self.directory.releaseAccounting();
            allocator.destroy(self.directory);
        }
    };

    /// Keep headers immutable while a detached reclamation batch is charged.
    pub fn destroyContents(self: *const Directory, allocator: std.mem.Allocator) void {
        var tree = self.tree;
        tree.deinit(allocator);
        var ids = self.ids;
        ids.deinit(allocator);
        var bounds = self.bounds;
        bounds.deinit(allocator);
        var ends = self.ends;
        ends.deinit(allocator);
        var levels = self.levels;
        levels.deinit(allocator);
        var generations = self.generations;
        generations.deinit(allocator);
    }

    pub fn retainAccounting(self: *const Directory) void {
        if (self.tree.account) |account| _ = account.retain();
        if (self.ids.account) |account| _ = account.retain();
        if (self.bounds.account) |account| _ = account.retain();
        if (self.ends.account) |account| _ = account.retain();
        if (self.levels.account) |account| _ = account.retain();
        if (self.generations.account) |account| _ = account.retain();
    }

    pub fn releaseAccounting(self: *const Directory) void {
        if (self.tree.account) |account| account.release();
        if (self.ids.account) |account| account.release();
        if (self.bounds.account) |account| account.release();
        if (self.ends.account) |account| account.release();
        if (self.levels.account) |account| account.release();
        if (self.generations.account) |account| account.release();
    }
    pub fn put(self: *Directory, backend: anytype, run: Run) !void {
        const allocator = backend.allocator;
        const previous = find(self.tree.root, .{ .run = &run });
        try self.tree.prepare(allocator);
        try self.ids.prepare(allocator);
        try self.bounds.prepare(allocator);
        try self.ends.prepareEdits(allocator, if (previous) |node| if (compareEnds(node.entry, .{ .run = &run }) != .eq) 2 else 1 else 1);
        try self.levels.prepare(allocator);
        if (run.level == 0) try self.generations.prepare(allocator);
        const payload = try allocator.create(Payload);
        errdefer allocator.destroy(payload);
        var owned = try repository.cloneRunCompactionSnapshot(allocator, run);
        errdefer owned.deinit(allocator);
        try backend.retainRunSnapshotRef(&owned);
        const release = struct {
            fn call(ctx: *anyopaque, value: *Run) void {
                if (comptime @hasDecl(@TypeOf(backend.*), "releaseDirectoryRunSnapshotRef")) {
                    @TypeOf(backend.*).releaseDirectoryRunSnapshotRef(value);
                    return;
                }
                const owner: @TypeOf(backend) = @ptrCast(@alignCast(ctx));
                owner.releaseRunSnapshotRef(value);
            }
        }.call;
        var bytes: usize = @sizeOf(Payload) + owned.smallest_key.len + owned.largest_key.len;
        if (owned.path) |path| bytes += path.len;
        if (owned.smallest_namespace_name) |name| bytes += name.len;
        if (owned.largest_namespace_name) |name| bytes += name.len;
        if (owned.state) |*present| bytes += @intCast(present.estimatedMemoryBytes());
        const account = self.tree.account.?;
        account.charge(bytes);
        payload.* = .{ .run = owned, .owner = @ptrCast(backend), .release_pin = release, .account = account, .bytes = bytes };
        const domain = if (comptime @hasField(@TypeOf(backend.*), "options")) blk: {
            if (comptime @hasField(@TypeOf(backend.options), "run_partition_key")) {
                if (backend.options.run_partition_key) |partition| break :blk partition(payload.run.smallest_key);
            }
            break :blk "";
        } else "";
        const entry = Entry{ .run = &payload.run, .payload = payload, .domain = domain };
        defer entry.deinit(allocator);
        self.memory_run_count += @intFromBool(run.path == null);
        if (previous) |node| self.memory_run_count -= @intFromBool(node.entry.run.path == null);
        const old_bytes = if (previous) |node| node.entry.run.size_bytes else 0;
        var level = self.levelStats(run.level);
        level.count += @intFromBool(previous == null);
        level.bytes = level.bytes - old_bytes + run.size_bytes;
        level.tombstone_runs += @intFromBool((run.tombstone_count orelse 0) != 0);
        if (previous) |node| level.tombstone_runs -= @intFromBool((node.entry.run.tombstone_count orelse 0) != 0);
        self.total_run_bytes = self.total_run_bytes - old_bytes + run.size_bytes;
        // Replacing metadata may also change an upper endpoint without
        // changing the read-order key. Remove its old secondary key first.
        if (previous) |node| if (compareEnds(node.entry, entry) != .eq) self.ends.removePrepared(allocator, node.entry);
        self.levels.putPrepared(allocator, level);
        if (run.level == 0) {
            const key = generation_index.Generation{ .sequence = generation_index.sequence(run) };
            var generation = if (generation_index.Tree.find(self.generations.root, key)) |node| node.entry else key;
            generation.files += @intFromBool(previous == null);
            generation.bytes = generation.bytes - old_bytes + run.size_bytes;
            self.generations.putPrepared(allocator, generation);
        }
        self.tree.putPrepared(allocator, entry);
        self.ids.putPrepared(allocator, entry);
        self.bounds.putPrepared(allocator, entry);
        self.ends.putPrepared(allocator, entry);
    }
    pub fn remove(self: *Directory, allocator: std.mem.Allocator, run: *const Run) !void {
        try self.tree.prepare(allocator);
        try self.ids.prepare(allocator);
        try self.bounds.prepare(allocator);
        try self.ends.prepare(allocator);
        try self.levels.prepare(allocator);
        if (run.level == 0) try self.generations.prepare(allocator);
        const existing = find(self.tree.root, .{ .run = run }) orelse return;
        // All run indexes own this payload until their prepared edits finish.
        const entry = existing.entry.retainShared();
        defer entry.deinit(allocator);
        self.memory_run_count -= @intFromBool(entry.run.path == null);
        var level = self.levelStats(entry.run.level);
        level.count -= 1;
        level.bytes -= entry.run.size_bytes;
        level.tombstone_runs -= @intFromBool((entry.run.tombstone_count orelse 0) != 0);
        self.total_run_bytes -= entry.run.size_bytes;
        if (level.count == 0) self.levels.removePrepared(allocator, level) else self.levels.putPrepared(allocator, level);
        if (entry.run.level == 0) {
            var generation = generation_index.Tree.find(self.generations.root, .{ .sequence = generation_index.sequence(entry.run.*) }).?.entry;
            generation.files -= 1;
            generation.bytes -= entry.run.size_bytes;
            if (generation.files == 0) self.generations.removePrepared(allocator, generation) else self.generations.putPrepared(allocator, generation);
        }
        self.tree.removePrepared(allocator, entry);
        self.ids.removePrepared(allocator, entry);
        self.bounds.removePrepared(allocator, entry);
        self.ends.removePrepared(allocator, entry);
    }
    pub fn count(self: *const Directory) usize {
        return if (self.tree.root) |root| root.count else 0;
    }

    /// Handles borrow immutable payloads from a pinned directory, never slots
    /// in the mutable Backend.runs array. A move/replacement changes identity.
    pub const Handle = struct {
        run: *const Run,
        revision: *const anyopaque,

        pub fn retain(self: @This()) @This() {
            const payload: *Payload = @ptrCast(@alignCast(@constCast(self.revision)));
            _ = payload.account.retain();
            _ = payload.refs.fetchAdd(1, .monotonic);
            return self;
        }

        pub fn release(self: @This(), allocator: std.mem.Allocator) void {
            const payload: *Payload = @ptrCast(@alignCast(@constCast(self.revision)));
            const account = payload.account;
            (Entry{ .run = self.run, .payload = payload }).deinit(allocator);
            account.release();
        }
        pub fn accountedMemoryBytes(self: @This(), pass: u64) u64 {
            const payload: *const Payload = @ptrCast(@alignCast(self.revision));
            return payload.account.chargeOnce(pass);
        }
        pub fn retainAccounting(self: @This()) *Account {
            const payload: *const Payload = @ptrCast(@alignCast(self.revision));
            return payload.account.retain();
        }
    };

    pub fn at(self: *const Directory, rank: usize) Handle {
        const entry = self.tree.root.?.at(rank);
        return .{ .run = entry.run, .revision = entry.payload.? };
    }

    pub fn readLess(_: void, a: Handle, b: Handle) bool {
        return compare(.{ .run = a.run }, .{ .run = b.run }) == .lt;
    }

    pub const Cursor = struct {
        directory: *const Directory,
        rank: usize = 0,
        path: Tree.Cursor = .{},

        pub fn next(self: *@This()) ?Handle {
            if (self.rank == self.directory.count()) return null;
            const entry = self.path.at(self.directory.tree.root.?, self.rank);
            self.rank += 1;
            return .{ .run = entry.run, .revision = entry.payload.? };
        }
    };

    pub fn readCursor(self: *const Directory) Cursor {
        return .{ .directory = self };
    }

    /// First candidate rank (forward), or one past the last candidate
    /// (reverse), in a disjoint lower level. One AVL descent replaces binary
    /// search over rank lookups, which would otherwise cost O(log² N).
    pub fn levelBoundRank(self: *const Directory, level: u32, namespace: ?[]const u8, key: ?[]const u8, reverse: bool, inclusive: bool) usize {
        std.debug.assert(level != 0);
        var root = self.tree.root;
        var rank: usize = 0;
        while (root) |node| {
            const run = node.entry.run;
            const order = if (key) |target|
                compareBound(if (reverse) run.smallest_namespace_name else run.largest_namespace_name, if (reverse) run.smallest_key else run.largest_key, namespace, target)
            else
                state.compareNamespace(.{ .name = run.smallest_namespace_name }, .{ .name = namespace });
            const past = order == .lt or (order == .eq and (if (reverse) key == null or inclusive else !inclusive));
            if (run.level < level or (run.level == level and past)) {
                rank += 1 + (if (node.left) |left| left.count else 0);
                root = node.right;
            } else root = node.left;
        }
        return rank;
    }

    pub fn resolve(self: *const Directory, handle: Handle) ?usize {
        const node = find(self.tree.root, .{ .run = handle.run }) orelse return null;
        if (@as(*const anyopaque, node.entry.payload.?) != handle.revision) return null;
        return self.tree.root.?.lowerBound(node.entry);
    }

    pub fn rankOf(self: *const Directory, run: *const Run) ?usize {
        const node = find(self.tree.root, .{ .run = run }) orelse return null;
        return self.tree.root.?.lowerBound(node.entry);
    }

    pub const OverlapCursor = struct {
        stack: [2 * @bitSizeOf(usize)]*const BoundsTree.Node = undefined,
        len: usize = 0,
        lower_ns: ?[]const u8,
        lower: []const u8,
        upper_ns: ?[]const u8,
        upper: []const u8,
        visited: usize = 0,

        /// A budget counts visited nodes, not just matches. Call again with a
        /// replenished budget to resume a large overlap without restarting.
        pub fn next(self: *@This(), budget: *usize) ?Handle {
            while (self.len != 0 and budget.* != 0) {
                budget.* -= 1;
                self.visited += 1;
                self.len -= 1;
                const node = self.stack[self.len];
                const largest = node.summary.largest.?;
                if (compareBound(largest.largest_namespace_name, largest.largest_key, self.lower_ns, self.lower) == .lt) continue;
                const run = node.entry.run;
                const starts_before_end = compareBound(run.smallest_namespace_name, run.smallest_key, self.upper_ns, self.upper) != .gt;
                if (starts_before_end) if (node.right) |right| {
                    self.stack[self.len] = right;
                    self.len += 1;
                };
                if (node.left) |left| {
                    self.stack[self.len] = left;
                    self.len += 1;
                }
                if (starts_before_end and compareBound(run.largest_namespace_name, run.largest_key, self.lower_ns, self.lower) != .lt)
                    return .{ .run = run, .revision = node.entry.payload.? };
            }
            return null;
        }

        pub fn done(self: *const @This()) bool {
            return self.len == 0;
        }
    };

    pub fn overlaps(self: *const Directory, lower_ns: ?[]const u8, lower: []const u8, upper_ns: ?[]const u8, upper: []const u8) OverlapCursor {
        var cursor = OverlapCursor{ .lower_ns = lower_ns, .lower = lower, .upper_ns = upper_ns, .upper = upper };
        if (self.bounds.root) |root| {
            cursor.stack[0] = root;
            cursor.len = 1;
        }
        return cursor;
    }

    /// After the initial overlap query, every unseen interval lies strictly
    /// left (end < initial lower) or right (start > initial upper). Walk those
    /// endpoints once as the closure expands. A separate end index prevents
    /// rescanning long/nested intervals on leftward expansion. Both indexes
    /// share immutable payloads; the cursors allocate nothing and charge each
    /// seek/descent to the caller's quantum.
    pub fn FrontierCursor(comptime reverse: bool) type {
        return struct {
            const Node = if (reverse) EndsTree.Node else BoundsTree.Node;
            path: [2 * @bitSizeOf(usize)]*const Node = undefined,
            len: usize = 0,
            descend: ?*const Node,
            seeking: bool = true,
            initial_ns: ?[]const u8,
            initial: []const u8,
            caught_up: bool = false,

            pub fn init(directory: *const Directory, namespace: ?[]const u8, key: []const u8) @This() {
                return .{ .descend = if (reverse) directory.ends.root else directory.bounds.root, .initial_ns = namespace, .initial = key };
            }

            pub fn next(self: *@This(), namespace: ?[]const u8, key: []const u8, credits: *usize) ?Handle {
                self.caught_up = false;
                while (credits.* != 0) {
                    credits.* -= 1;
                    if (self.descend) |node| {
                        const run = node.entry.run;
                        const qualifies = !self.seeking or compareBound(if (reverse) run.largest_namespace_name else run.smallest_namespace_name, if (reverse) run.largest_key else run.smallest_key, self.initial_ns, self.initial) == (if (reverse) std.math.Order.lt else .gt);
                        if (qualifies) {
                            self.path[self.len] = node;
                            self.len += 1;
                            self.descend = if (reverse) node.right else node.left;
                        } else self.descend = if (reverse) node.left else node.right;
                        continue;
                    }
                    self.seeking = false;
                    if (self.len == 0) {
                        self.caught_up = true;
                        return null;
                    }
                    const node = self.path[self.len - 1];
                    const run = node.entry.run;
                    const order = compareBound(if (reverse) run.largest_namespace_name else run.smallest_namespace_name, if (reverse) run.largest_key else run.smallest_key, namespace, key);
                    if (order == (if (reverse) std.math.Order.lt else .gt)) {
                        self.caught_up = true;
                        return null;
                    }
                    self.len -= 1;
                    self.descend = if (reverse) node.left else node.right;
                    return .{ .run = run, .revision = node.entry.payload.? };
                }
                return null;
            }
        };
    }

    pub fn levelStats(self: *const Directory, number: u32) LevelAggregate {
        const root = self.levels.root orelse return .{ .level = number };
        const rank = root.lowerBound(.{ .level = number });
        if (rank < root.count) {
            const present = root.at(rank);
            if (present.level == number) return present;
        }
        return .{ .level = number };
    }

    pub fn levelCount(self: *const Directory) usize {
        return if (self.levels.root) |root| root.count else 0;
    }

    pub fn levelStart(self: *const Directory, level: u32) usize {
        var start: usize = 0;
        for (0..self.levelCount()) |rank| {
            const item = self.levelAt(rank);
            if (item.level >= level) break;
            start += item.count;
        }
        return start;
    }

    pub fn levelAt(self: *const Directory, rank: usize) LevelAggregate {
        return self.levels.root.?.at(rank);
    }

    pub fn maxLevel(self: *const Directory) u32 {
        return if (self.levels.root) |root| root.at(root.count - 1).level else 0;
    }

    /// Visit only changed search paths. Looking up shared subtree roots in the
    /// other version also skips unchanged subtrees after AVL rotations.
    /// The visitor borrows runs from both pinned roots for the call's lifetime.
    pub fn changesSince(self: *const Directory, previous: *const Directory, visitor: anytype) !void {
        try visitRemoved(previous.tree.root, self.tree.root, visitor);
        try visitAdded(self.tree.root, previous.tree.root, visitor);
    }

    /// Allocation-free persistent-root diff. Shared subtrees are skipped even
    /// across rotations; callers pin both roots and pay one credit per node.
    pub const ChangeCursor = struct {
        pub const Change = struct { kind: enum { remove, put }, run: *const Run };
        previous: ?*Tree.Node,
        current: ?*Tree.Node,
        stack: [2 * @bitSizeOf(usize)]*Tree.Node = undefined,
        len: usize = 0,
        phase: enum { removed, added, done } = .removed,

        pub fn init(previous: *const Directory, current: *const Directory) ChangeCursor {
            var cursor = ChangeCursor{ .previous = previous.tree.root, .current = current.tree.root };
            cursor.push(previous.tree.root);
            return cursor;
        }
        fn push(self: *ChangeCursor, node: ?*Tree.Node) void {
            if (node) |value| {
                self.stack[self.len] = value;
                self.len += 1;
            }
        }
        pub fn done(self: *const ChangeCursor) bool {
            return self.phase == .done;
        }
        pub fn next(self: *ChangeCursor, credits: *usize) ?Change {
            while (credits.* != 0 and !self.done()) {
                if (self.len == 0) {
                    if (self.phase == .removed) {
                        self.phase = .added;
                        self.push(self.current);
                    } else self.phase = .done;
                    continue;
                }
                credits.* -= 1;
                self.len -= 1;
                const node = self.stack[self.len];
                const matched = find(if (self.phase == .removed) self.current else self.previous, node.entry);
                if (matched == node) continue;
                self.push(node.right);
                self.push(node.left);
                if (self.phase == .removed) {
                    if (matched == null) return .{ .kind = .remove, .run = node.entry.run };
                } else if (matched == null or matched.?.entry.payload != node.entry.payload) {
                    return .{ .kind = .put, .run = node.entry.run };
                }
            }
            return null;
        }
    };

    /// A validated predecessor remains valid after removals. Only inserted or
    /// replaced runs and their immediate final neighbors can introduce a new
    /// ordering/overlap violation, including both sides of a level move.
    pub fn validateChangesSince(self: *const Directory, previous: *const Directory, validate: *const fn ([]const Run) anyerror!void) !void {
        const Check = struct {
            directory: *const Directory,
            validate: *const fn ([]const Run) anyerror!void,
            pub fn put(check: *@This(), run: Run) !void {
                const root = check.directory.tree.root.?;
                const rank = root.lowerBound(.{ .run = &run });
                const start = rank -| 1;
                const end = @min(root.count, rank + 2);
                var neighbors: [3]Run = undefined;
                for (start..end, 0..) |position, i| neighbors[i] = root.at(position).run.*;
                try check.validate(neighbors[0 .. end - start]);
            }
        };
        var check = Check{ .directory = self, .validate = validate };
        try visitAdded(self.tree.root, previous.tree.root, &check);
    }

    fn find(root: ?*Tree.Node, entry: Entry) ?*Tree.Node {
        var current = root;
        while (current) |node| switch (compare(entry, node.entry)) {
            .eq => return node,
            .lt => current = node.left,
            .gt => current = node.right,
        };
        return null;
    }

    fn visitRemoved(root: ?*Tree.Node, other: ?*Tree.Node, visitor: anytype) !void {
        const node = root orelse return;
        const matched = find(other, node.entry);
        if (matched == node) return;
        try visitRemoved(node.left, other, visitor);
        if (matched == null) try visitor.remove(node.entry.run.*);
        try visitRemoved(node.right, other, visitor);
    }

    fn visitAdded(root: ?*Tree.Node, other: ?*Tree.Node, visitor: anytype) !void {
        const node = root orelse return;
        const matched = find(other, node.entry);
        if (matched == node) return;
        try visitAdded(node.left, other, visitor);
        if (matched == null or matched.?.entry.payload != node.entry.payload) try visitor.put(node.entry.run.*);
        try visitAdded(node.right, other, visitor);
    }
    pub fn project(self: *const Directory, allocator: std.mem.Allocator) ![]Run {
        const runs = try allocator.alloc(Run, self.count());
        var cursor: Tree.Cursor = .{};
        for (runs, 0..) |*run, i| {
            run.* = cursor.at(self.tree.root.?, i).run.*;
            run.owns_metadata = false;
            run.owns_path = false;
            run.owns_bloom_filter = false;
            run.version_ref_pinned = false;
            run.shared_read_version = true;
        }
        return runs;
    }
    pub fn accountedMemoryBytes(self: *const Directory, pass: u64) u64 {
        return @sizeOf(Directory) + (self.tree.spare.capacity + self.ids.spare.capacity + self.bounds.spare.capacity + self.ends.spare.capacity + self.levels.spare.capacity + self.generations.spare.capacity) * @sizeOf(*Tree.Node) +
            (if (self.generations.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.ids.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.bounds.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.ends.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.levels.account) |account| account.chargeOnce(pass) else 0);
    }

    pub const PlanningOrder = struct { domain: []usize, bounds: []usize };

    pub fn byId(self: *const Directory, id: u64) ?*const Run {
        var node = self.ids.root;
        while (node) |current| {
            switch (std.math.order(id, current.entry.run.id)) {
                .lt => node = current.left,
                .gt => node = current.right,
                .eq => return current.entry.run,
            }
        }
        return null;
    }

    /// Scheduling is independent of component discovery. In particular, a
    /// clock rollback makes any future-dated tombstone immediately eligible;
    /// retaining only the minimum timestamp would lose that condition.
    pub fn tombstoneGcDelay(self: *const Directory, age: u64, now: u64) ?u64 {
        const summary = (self.tree.root orelse return null).summary;
        if (summary.tombstone_runs == 0) return null;
        if (summary.gc_requested) return 0;
        if (age == 0) return null;
        if (summary.oldest_tombstone == 0 or summary.newest_tombstone > now) return 0;
        return (summary.oldest_tombstone +| age) -| now;
    }

    pub fn tombstoneRunCount(self: *const Directory) usize {
        return if (self.tree.root) |root| root.summary.tombstone_runs else 0;
    }

    pub fn unknownTombstoneRunCount(self: *const Directory) usize {
        return if (self.tree.root) |root| root.summary.unknown_tombstone_runs else 0;
    }
    pub fn nextUnknownTombstone(self: *const Directory, after: usize) ?Handle {
        const found = nextUnknown(self.tree.root, after, 0) orelse return null;
        return .{ .run = found.entry.run, .revision = found.entry.payload.? };
    }
    fn nextUnknown(root: ?*const Tree.Node, after: usize, base: usize) ?*const Tree.Node {
        const node = root orelse return null;
        if (node.summary.unknown_tombstone_runs == 0 or base + node.count <= after) return null;
        const rank = base + (if (node.left) |left| left.count else 0);
        if (nextUnknown(node.left, after, base)) |found| return found;
        if (rank >= after and node.entry.run.tombstone_count == null) return node;
        return nextUnknown(node.right, after, rank + 1);
    }
    pub fn generationCount(self: *const Directory) usize {
        return if (self.generations.root) |root| root.count else 0;
    }
    pub fn generationPrefix(self: *const Directory, count_arg: usize) generation_index.Generation.Summary {
        return generation_index.prefix(self.generations.root, count_arg);
    }

    pub const TombstoneCursor = struct {
        directory: *const Directory,
        rank: usize = 0,

        pub fn next(self: *@This()) ?Handle {
            const found = nextMarked(self.directory.tree.root, self.rank, 0) orelse return null;
            self.rank = found.rank + 1;
            return .{ .run = found.node.entry.run, .revision = found.node.entry.payload.? };
        }
        const Found = struct { node: *const Tree.Node, rank: usize };
        fn nextMarked(root: ?*const Tree.Node, after: usize, base: usize) ?Found {
            const node = root orelse return null;
            if (node.summary.tombstone_runs == 0 or base + node.count <= after) return null;
            const rank = base + (if (node.left) |left| left.count else 0);
            if (nextMarked(node.left, after, base)) |found| return found;
            if (rank >= after and (node.entry.run.tombstone_count orelse 0) != 0) return .{ .node = node, .rank = rank };
            return nextMarked(node.right, after, rank + 1);
        }
    };

    /// Skip tombstone-free subtrees when finding component anchors. Iteration
    /// preserves read precedence, unlike a pre-order walk of marked nodes.
    pub fn tombstoneCursor(self: *const Directory) TombstoneCursor {
        return .{ .directory = self };
    }

    /// Diagnostic/oracle adapter only. Production selection walks immutable
    /// overlap cursors; do not pay for a separate domain-order tree on every
    /// publication just to accelerate an exceptional full projection.
    pub fn planningOrder(self: *const Directory, allocator: std.mem.Allocator) !PlanningOrder {
        const domain = try allocator.alloc(usize, self.count());
        errdefer allocator.free(domain);
        const ordered_bounds = try allocator.alloc(usize, self.count());
        errdefer allocator.free(ordered_bounds);
        if (self.count() == 0) return .{ .domain = domain, .bounds = ordered_bounds };
        const entries = try allocator.alloc(Entry, self.count());
        defer allocator.free(entries);
        var by_level: Tree.Cursor = .{};
        var one_domain = true;
        for (entries, 0..) |*entry, i| {
            entry.* = by_level.at(self.tree.root.?, i);
            if (state.compareNamespace(.{ .name = entries[0].run.smallest_namespace_name }, .{ .name = entry.run.smallest_namespace_name }) != .eq or !std.mem.eql(u8, entries[0].domain, entry.domain)) one_domain = false;
            domain[i] = i;
        }
        if (!one_domain) std.mem.sort(usize, domain, entries, struct {
            fn less(all: []Entry, a: usize, b: usize) bool {
                return compareDomain(all[a], all[b]) == .lt;
            }
        }.less);
        const one_sorted_level = self.levelCount() == 1 and self.levelAt(0).level != 0;
        if (one_domain and one_sorted_level) {
            for (0..self.count()) |i| {
                domain[i] = i;
                ordered_bounds[i] = i;
            }
            return .{ .domain = domain, .bounds = ordered_bounds };
        }
        var positions: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        defer positions.deinit(allocator);
        try positions.ensureTotalCapacity(allocator, @intCast(self.count()));
        for (entries, 0..) |entry, i| positions.putAssumeCapacity(entry.run.id, i);
        var by_bounds: BoundsTree.Cursor = .{};
        for (0..self.count()) |i| {
            ordered_bounds[i] = if (one_sorted_level) i else positions.get(by_bounds.at(self.bounds.root.?, i).run.id).?;
        }
        return .{ .domain = domain, .bounds = ordered_bounds };
    }
};

test "run directory endpoint replacement preserves old cursors and removes old secondary keys" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
        fn check(allocator: std.mem.Allocator) !void {
            var fixture = @This(){ .allocator = allocator };
            const original = try Directory.create(allocator);
            defer original.destroy(allocator);
            var run = Run{ .id = 1, .level = 1, .size_bytes = 1, .path = @constCast("end.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("b"), .entry_count = 1, .bloom_filter = null, .state = null };
            try original.put(&fixture, run);
            const changed = try original.fork(allocator);
            defer changed.destroy(allocator);
            var old_cursor = Directory.FrontierCursor(true).init(original, null, "e");
            run.largest_key = @constCast("d");
            try changed.put(&fixture, run);
            try std.testing.expectEqual(@as(usize, 1), changed.ends.root.?.count);
            var cursor = Directory.FrontierCursor(true).init(changed, null, "e");
            var credits: usize = 100;
            try std.testing.expectEqualStrings("d", cursor.next(null, "a", &credits).?.run.largest_key);
            try std.testing.expect(cursor.next(null, "a", &credits) == null);
            try std.testing.expectEqualStrings("b", old_cursor.next(null, "a", &credits).?.run.largest_key);
            try changed.remove(allocator, &run);
            try std.testing.expect(changed.ends.root == null);
            try std.testing.expectEqual(@as(usize, 1), original.count());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "run directory path copies preserve pinned epochs through inserts removals and OOM" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), run: *Run) !void {
            self.pins += 1;
            run.version_ref_pinned = true;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), run: *Run) void {
            std.debug.assert(run.version_ref_pinned);
            self.pins -= 1;
            run.version_ref_pinned = false;
        }
    };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var backend = Fixture{ .allocator = allocator };
    const original = try Directory.create(allocator);
    defer original.destroy(allocator);
    for (0..256) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try original.put(&backend, .{ .id = i + 1, .level = 1, .size_bytes = 1, .path = @constCast("test.sst"), .smallest_namespace_name = null, .smallest_key = &key, .largest_namespace_name = null, .largest_key = &key, .entry_count = 1, .bloom_filter = null, .state = null });
    }
    const baseline_pins = backend.pins;
    const snapshot = try original.fork(allocator);
    defer snapshot.destroy(allocator);
    try std.testing.expectEqual(baseline_pins, backend.pins);
    const projected = try snapshot.project(allocator);
    defer allocator.free(projected);
    {
        const gc = try snapshot.fork(allocator);
        defer gc.destroy(allocator);
        try std.testing.expectEqual(@as(?u64, null), gc.tombstoneGcDelay(100, 1000));
        var old = projected[0];
        old.tombstone_count = 1;
        old.oldest_tombstone_unix_ns = 950;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 50), gc.tombstoneGcDelay(100, 1000));
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1100));
        try std.testing.expectEqual(@as(?u64, null), gc.tombstoneGcDelay(0, 1000));
        var future = projected[1];
        future.tombstone_count = 1;
        future.oldest_tombstone_unix_ns = 1001;
        try gc.put(&backend, future);
        var deletes = gc.tombstoneCursor();
        try std.testing.expectEqual(old.id, deletes.next().?.run.id);
        try std.testing.expectEqual(future.id, deletes.next().?.run.id);
        try std.testing.expect(deletes.next() == null);
        try std.testing.expect(deletes.next() == null);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1000));
        try gc.remove(allocator, &future);
        try std.testing.expectEqual(@as(?u64, 50), gc.tombstoneGcDelay(100, 1000));
        old.gc_requested = true;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(0, 1000));
        old.gc_requested = false;
        old.oldest_tombstone_unix_ns = 0;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1000));
        old.tombstone_count = 0;
        old.gc_requested = true;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, null), gc.tombstoneGcDelay(100, 1000));
        try std.testing.expectEqual(@as(?u64, null), snapshot.tombstoneGcDelay(100, 1000));
        var no_deletes = snapshot.tombstoneCursor();
        try std.testing.expect(no_deletes.next() == null);
    }
    {
        const detached = try Directory.create(allocator);
        var alive = true;
        defer if (alive) detached.destroy(allocator);
        try detached.put(&backend, projected[0]);
        const handle = detached.at(0).retain();
        defer handle.release(allocator);
        detached.destroy(allocator);
        alive = false;
        // A selected handle owns both its payload and accounting lifetime,
        // even after every directory from that lineage has been reclaimed.
        try std.testing.expectEqual(projected[0].id, handle.run.id);
        try std.testing.expectEqualStrings(projected[0].smallest_key, handle.run.smallest_key);
    }
    const candidate = try original.fork(allocator);
    defer candidate.destroy(allocator);
    for (projected, 0..) |*run, i| if (i % 2 == 0) {
        try candidate.remove(allocator, run);
    };
    try std.testing.expectEqual(@as(usize, 128), candidate.count());
    try std.testing.expectEqual(@as(usize, 256), snapshot.count());
    const remaining = try candidate.project(allocator);
    defer allocator.free(remaining);
    for (remaining, 0..) |run, i| try std.testing.expectEqual(@as(u64, 2 * i + 2), run.id);
    try std.testing.expect(candidate.resolve(snapshot.at(0)) == null);
    try std.testing.expectEqual(@as(?usize, 0), candidate.resolve(snapshot.at(1)));
    var overlap = candidate.overlaps(null, projected[30].smallest_key, null, projected[40].largest_key);
    var matches: usize = 0;
    while (!overlap.done()) {
        var budget: usize = 1;
        if (overlap.next(&budget)) |handle| {
            try std.testing.expect(handle.run.id >= 31 and handle.run.id <= 41);
            try std.testing.expect(handle.run.id % 2 == 0);
            matches += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), matches);
    try std.testing.expect(overlap.visited < candidate.count() / 2);
    const Changes = struct {
        removed: [256]bool = @splat(false),
        pub fn put(_: *@This(), _: Run) !void {
            return error.UnexpectedAddition;
        }
        pub fn remove(self: *@This(), run: Run) !void {
            const index: usize = @intCast(run.id - 1);
            try std.testing.expect(index % 2 == 0);
            try std.testing.expect(!self.removed[index]);
            self.removed[index] = true;
        }
    };
    var changes: Changes = .{};
    // Removing alternating keys rotates shared subtrees. Diffs must skip only
    // genuinely shared roots, without hiding removals or visiting them twice.
    try candidate.changesSince(snapshot, &changes);
    for (changes.removed, 0..) |removed, i| try std.testing.expectEqual(i % 2 == 0, removed);
    var incremental = Directory.ChangeCursor.init(snapshot, candidate);
    var incremental_changes: Changes = .{};
    var no_credit: usize = 0;
    try std.testing.expect(incremental.next(&no_credit) == null);
    while (!incremental.done()) {
        var credit: usize = 1;
        if (incremental.next(&credit)) |change| switch (change.kind) {
            .remove => try incremental_changes.remove(change.run.*),
            .put => try incremental_changes.put(change.run.*),
        };
    }
    try std.testing.expectEqualSlices(bool, &changes.removed, &incremental_changes.removed);
    const Validator = struct {
        fn validate(runs: []const Run) !void {
            for (runs, 0..) |run, i| {
                if (run.entry_count == 0) return error.InvalidTableFile;
                if (i > 0 and std.mem.order(u8, runs[i - 1].largest_key, run.smallest_key) != .lt) return error.InvalidTableFile;
            }
        }
    };
    try candidate.validateChangesSince(snapshot, Validator.validate);
    {
        const overlapping = try candidate.fork(allocator);
        defer overlapping.destroy(allocator);
        var replacement = remaining[0];
        replacement.largest_key = remaining[1].smallest_key;
        try overlapping.put(&backend, replacement);
        try std.testing.expectError(error.InvalidTableFile, overlapping.validateChangesSince(candidate, Validator.validate));
        replacement = remaining[remaining.len - 1];
        replacement.entry_count = 0;
        const empty_run = try candidate.fork(allocator);
        defer empty_run.destroy(allocator);
        try empty_run.put(&backend, replacement);
        try std.testing.expectError(error.InvalidTableFile, empty_run.validateChangesSince(candidate, Validator.validate));
    }
    const doomed = try candidate.fork(allocator);
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, doomed.remove(allocator, &projected[1]));
    failing.fail_index = std.math.maxInt(usize);
    doomed.destroy(allocator);
    try std.testing.expectEqual(@as(usize, 128), candidate.count());
    for (remaining) |*run| try candidate.remove(allocator, run);
    try std.testing.expectEqual(@as(usize, 0), candidate.count());
    try std.testing.expectEqual(baseline_pins, backend.pins);
}
