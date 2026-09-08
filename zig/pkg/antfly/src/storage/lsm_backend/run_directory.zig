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

fn compareDomain(a: Entry, b: Entry) std.math.Order {
    const ns = state.compareNamespace(.{ .name = a.run.smallest_namespace_name }, .{ .name = b.run.smallest_namespace_name });
    if (ns != .eq) return ns;
    const domain = std.mem.order(u8, a.domain, b.domain);
    return if (domain != .eq) domain else compare(a, b);
}

fn compareBounds(a: Entry, b: Entry) std.math.Order {
    const ns = state.compareNamespace(.{ .name = a.run.smallest_namespace_name }, .{ .name = b.run.smallest_namespace_name });
    if (ns != .eq) return ns;
    const key = std.mem.order(u8, a.run.smallest_key, b.run.smallest_key);
    return if (key != .eq) key else compare(a, b);
}

pub const LevelAggregate = struct {
    level: u32,
    count: usize = 0,
    bytes: u64 = 0,
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
    const Tree = @import("ordered_index.zig").Index(Entry, compare);
    const DomainTree = @import("ordered_index.zig").Index(Entry, compareDomain);
    const BoundsTree = @import("ordered_index.zig").Index(Entry, compareBounds);
    const LevelTree = @import("ordered_index.zig").Index(LevelAggregate, compareLevel);
    tree: Tree = .{},
    domains: DomainTree = .{},
    bounds: BoundsTree = .{},
    levels: LevelTree = .{},
    total_run_bytes: u64 = 0,
    retired_next: ?*Directory = null,

    pub fn create(allocator: std.mem.Allocator) !*Directory {
        const self = try allocator.create(Directory);
        self.* = .{};
        return self;
    }
    pub fn fork(self: *const Directory, allocator: std.mem.Allocator) !*Directory {
        const out = try create(allocator);
        out.tree = self.tree.fork();
        out.domains = self.domains.fork();
        out.bounds = self.bounds.fork();
        out.levels = self.levels.fork();
        out.total_run_bytes = self.total_run_bytes;
        return out;
    }
    pub fn destroy(self: *Directory, allocator: std.mem.Allocator) void {
        self.destroyContents(allocator);
        allocator.destroy(self);
    }

    /// Keep headers immutable while a detached reclamation batch is charged.
    pub fn destroyContents(self: *const Directory, allocator: std.mem.Allocator) void {
        var tree = self.tree;
        tree.deinit(allocator);
        var domains = self.domains;
        domains.deinit(allocator);
        var bounds = self.bounds;
        bounds.deinit(allocator);
        var levels = self.levels;
        levels.deinit(allocator);
    }

    pub fn retainAccounting(self: *const Directory) void {
        if (self.tree.account) |account| _ = account.retain();
        if (self.domains.account) |account| _ = account.retain();
        if (self.bounds.account) |account| _ = account.retain();
        if (self.levels.account) |account| _ = account.retain();
    }

    pub fn releaseAccounting(self: *const Directory) void {
        if (self.tree.account) |account| account.release();
        if (self.domains.account) |account| account.release();
        if (self.bounds.account) |account| account.release();
        if (self.levels.account) |account| account.release();
    }
    pub fn put(self: *Directory, backend: anytype, run: Run) !void {
        const allocator = backend.allocator;
        try self.tree.prepare(allocator);
        try self.domains.prepare(allocator);
        try self.bounds.prepare(allocator);
        try self.levels.prepare(allocator);
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
        const previous = find(self.tree.root, entry);
        const old_bytes = if (previous) |node| node.entry.run.size_bytes else 0;
        var level = self.levelStats(run.level);
        level.count += @intFromBool(previous == null);
        level.bytes = level.bytes - old_bytes + run.size_bytes;
        self.total_run_bytes = self.total_run_bytes - old_bytes + run.size_bytes;
        self.levels.putPrepared(allocator, level);
        self.tree.putPrepared(allocator, entry);
        self.domains.putPrepared(allocator, entry);
        self.bounds.putPrepared(allocator, entry);
    }
    pub fn remove(self: *Directory, allocator: std.mem.Allocator, run: *const Run) !void {
        try self.tree.prepare(allocator);
        try self.domains.prepare(allocator);
        try self.bounds.prepare(allocator);
        try self.levels.prepare(allocator);
        const existing = find(self.tree.root, .{ .run = run }) orelse return;
        // All three trees own this payload until their prepared edits finish.
        const entry = existing.entry.retainShared();
        defer entry.deinit(allocator);
        var level = self.levelStats(entry.run.level);
        level.count -= 1;
        level.bytes -= entry.run.size_bytes;
        self.total_run_bytes -= entry.run.size_bytes;
        if (level.count == 0) self.levels.removePrepared(allocator, level) else self.levels.putPrepared(allocator, level);
        self.tree.removePrepared(allocator, entry);
        self.domains.removePrepared(allocator, entry);
        self.bounds.removePrepared(allocator, entry);
    }
    pub fn count(self: *const Directory) usize {
        return if (self.tree.root) |root| root.count else 0;
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
        return @sizeOf(Directory) + (self.tree.spare.capacity + self.domains.spare.capacity + self.bounds.spare.capacity + self.levels.spare.capacity) * @sizeOf(*Tree.Node) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.domains.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.bounds.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.levels.account) |account| account.chargeOnce(pass) else 0);
    }

    pub const PlanningOrder = struct { domain: []usize, bounds: []usize };

    /// Ordering is maintained incrementally at publication. The existing
    /// positional planner adapter only translates stable IDs; it never sorts
    /// the complete domain/range index after an unrelated publication.
    pub fn planningOrder(self: *const Directory, allocator: std.mem.Allocator) !PlanningOrder {
        const domain = try allocator.alloc(usize, self.count());
        errdefer allocator.free(domain);
        const ordered_bounds = try allocator.alloc(usize, self.count());
        errdefer allocator.free(ordered_bounds);
        if (self.count() == 0) return .{ .domain = domain, .bounds = ordered_bounds };
        const first = self.domains.root.?.at(0);
        const last = self.domains.root.?.at(self.count() - 1);
        const one_domain = state.compareNamespace(.{ .name = first.run.smallest_namespace_name }, .{ .name = last.run.smallest_namespace_name }) == .eq and std.mem.eql(u8, first.domain, last.domain);
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
        var by_level: Tree.Cursor = .{};
        for (0..self.count()) |i| positions.putAssumeCapacity(by_level.at(self.tree.root.?, i).run.id, i);
        var by_domain: DomainTree.Cursor = .{};
        var by_bounds: BoundsTree.Cursor = .{};
        for (0..self.count()) |i| {
            domain[i] = if (one_domain) i else positions.get(by_domain.at(self.domains.root.?, i).run.id).?;
            ordered_bounds[i] = if (one_sorted_level) i else positions.get(by_bounds.at(self.bounds.root.?, i).run.id).?;
        }
        return .{ .domain = domain, .bounds = ordered_bounds };
    }
};

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
