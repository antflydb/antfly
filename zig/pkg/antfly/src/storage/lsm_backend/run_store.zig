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

//! Writer ownership, ordered independently of contiguous positions. Candidate
//! roots share untouched payloads; adoption occurs only after every fallible
//! publication preparation. Mutable cache hints belong to these writer-owned
//! runs, not to the immutable reader directory.
const std = @import("std");
const Run = @import("repository.zig").Run;
const Directory = @import("run_directory.zig").Directory;
const Account = @import("memory_account.zig").Account;
const completion_allocation = @import("completion_allocator.zig");
const DestroyRun = *const fn (*Run, std.mem.Allocator) void;

/// Slice-backed planner fixtures remain an independent oracle. Production
/// uses the ordered owner; these adapters never materialize a hidden vector.
pub fn count(backend: anytype) usize {
    return if (@TypeOf(backend.runs) == Store) backend.runs.count() else backend.runs.items.len;
}
pub fn at(backend: anytype, rank: usize) *Run {
    return if (@TypeOf(backend.runs) == Store) backend.runs.at(rank) else &backend.runs.items[rank];
}
pub fn project(backend: anytype, allocator: std.mem.Allocator) ![]Run {
    return if (@TypeOf(backend.runs) == Store) backend.runs.project(allocator) else allocator.dupe(Run, backend.runs.items);
}
/// Stable plan handles address writer records across unrelated rank changes.
/// Positions remain only for slice-backed planner oracles.
pub fn planAt(backend: anytype, plan: anytype, i: usize) *Run {
    if (comptime @TypeOf(backend.runs) == Store) {
        if (plan.input_handles) |handles| {
            const offset = if (i < plan.source_len) plan.source_start + i else plan.target_start + i - plan.source_len;
            return backend.runs.find(handles[offset].run).?;
        }
    }
    return at(backend, if (i < plan.source_len) plan.sourceIndex(i) else plan.targetIndex(i - plan.source_len));
}

pub fn planGet(runs: anytype, plan: anytype, i: usize) Run {
    if (comptime @TypeOf(runs) == *Store or @TypeOf(runs) == *const Store) {
        if (plan.input_handles) |handles| {
            const offset = if (i < plan.source_len) plan.source_start + i else plan.target_start + i - plan.source_len;
            return runs.find(handles[offset].run).?.*;
        }
    }
    return get(runs, if (i < plan.source_len) plan.sourceIndex(i) else plan.targetIndex(i - plan.source_len));
}
/// The independent flat planner is exercised only by differential tests.
/// Production cannot silently fall back to materializing its entire run set.
pub fn oracleItems(backend: anytype) ![]const Run {
    if (comptime @TypeOf(backend.runs) != Store) return backend.runs.items;
    if (comptime @import("builtin").is_test) return backend.runs.testItems(backend.allocator);
    return error.TestOnlyPlanner;
}
pub fn len(runs: anytype) usize {
    if (comptime @TypeOf(runs) == *Store or @TypeOf(runs) == *const Store) return runs.count();
    if (comptime @TypeOf(runs) == *std.ArrayListUnmanaged(Run) or @TypeOf(runs) == *const std.ArrayListUnmanaged(Run)) return runs.items.len;
    return runs.len;
}
pub fn get(runs: anytype, rank: usize) Run {
    if (comptime @TypeOf(runs) == *Store or @TypeOf(runs) == *const Store) return runs.at(rank).*;
    if (comptime @TypeOf(runs) == *std.ArrayListUnmanaged(Run) or @TypeOf(runs) == *const std.ArrayListUnmanaged(Run)) return runs.items[rank];
    return runs[rank];
}

const Payload = struct {
    allocation_allocator: std.mem.Allocator,
    owner: @import("repository.zig").RunOwner,
    parent: ?*@import("repository.zig").RunOwner = null,
    run: Run,
    owned: bool = false,
    destroy_run: ?DestroyRun,
    account: *Account,
    bytes: u64,

    fn destroy(header: *@import("repository.zig").RunOwner, allocator: std.mem.Allocator) void {
        const payload: *Payload = @fieldParentPtr("owner", header);
        if (payload.parent) |parent| {
            if (payload.run.owns_bloom_filter) if (payload.run.bloom_filter) |*filter| filter.deinit(allocator);
            if (payload.run.table_index) |*index| index.deinit(allocator);
            if (payload.run.path != null) if (payload.run.state) |*state| state.deinit(allocator);
            parent.release(allocator);
        } else if (payload.owned) {
            payload.run.owner = null;
            if (payload.destroy_run) |destroy_run| destroy_run(&payload.run, allocator) else payload.run.deinit(allocator);
        }
        payload.account.discharge(payload.bytes);
        payload.allocation_allocator.destroy(payload);
    }
};

test "writer run store stages ownership atomically through allocation failures" {
    const Fixture = struct {
        fn make(allocator: std.mem.Allocator, id: u64) !Run {
            const key = try allocator.alloc(u8, 8);
            errdefer allocator.free(key);
            std.mem.writeInt(u64, key[0..8], id, .big);
            return .{ .id = id, .level = 1, .size_bytes = id, .path = null, .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = try allocator.dupe(u8, key), .entry_count = 1, .bloom_filter = null, .state = null };
        }
        fn check(allocator: std.mem.Allocator) !void {
            var live: Store = .{};
            defer live.deinit(allocator);
            for (0..8) |i| {
                var run = try make(allocator, i + 1);
                errdefer run.deinit(allocator);
                try live.append(allocator, run);
            }
            var output = try make(allocator, 20);
            var adopted = false;
            defer if (!adopted) output.deinit(allocator);
            var candidate = live.fork();
            defer candidate.deinit(allocator);
            const removed = live.at(3);
            try candidate.remove(allocator, removed);
            try candidate.stage(allocator, output);
            try std.testing.expectEqual(@as(u64, 4), live.at(3).id);
            try std.testing.expectEqual(@as(u64, 5), candidate.at(3).id);
            try std.testing.expect(live.at(0) == candidate.at(0));
            candidate.adopt(&output);
            adopted = true;
            std.mem.swap(Store, &live, &candidate);
            try std.testing.expectEqual(@as(usize, 8), live.count());
            try std.testing.expectEqual(@as(u64, 52), live.totalBytes());
            // The original root is still pinned and owns its removed input.
            try std.testing.expectEqual(@as(u64, 4), candidate.at(3).id);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}
const Entry = struct {
    run: *Run,
    payload: ?*Payload = null,
    pub const Summary = struct { run_bytes: u64 = 0, manifest_bytes: u64 = 0, l0_files: usize = 0, l0_bytes: u64 = 0 };
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        const run = entry.run;
        const wire: u64 = if (run.path) |path| 112 + path.len + run.smallest_key.len + run.largest_key.len +
            (if (run.smallest_namespace_name) |name| name.len else 0) + (if (run.largest_namespace_name) |name| name.len else 0) else 0;
        return .{ .run_bytes = left.run_bytes +| right.run_bytes +| run.size_bytes, .manifest_bytes = left.manifest_bytes +| right.manifest_bytes +| wire, .l0_files = left.l0_files + right.l0_files + @intFromBool(run.level == 0), .l0_bytes = left.l0_bytes +| right.l0_bytes +| if (run.level == 0) run.size_bytes else 0 };
    }
    pub fn retainShared(self: Entry) Entry {
        _ = self.payload.?.owner.retain();
        return self;
    }
    pub fn deinit(self: Entry, allocator: std.mem.Allocator) void {
        const payload = self.payload orelse return;
        payload.owner.release(allocator);
    }
    pub fn retainedBytes(_: Entry) usize {
        return 0;
    }
};
fn compare(a: Entry, b: Entry) std.math.Order {
    if (Directory.runLess(a.run, b.run)) return .lt;
    if (Directory.runLess(b.run, a.run)) return .gt;
    return .eq;
}

pub const Store = struct {
    pub const Tree = @import("ordered_index.zig").Index(Entry, compare);
    pub const empty: Store = .{};

    /// Empty output tree, owned payload headers and immutable path/bounds.
    /// Includes every allocation prefix, even for empty namespace slices.
    pub fn freshAllocationBound(count_: usize, bound_bytes: usize, path_bytes: usize) !usize {
        const footprint = completion_allocation.RecyclingScratch.allocationFootprint;
        const bounds = try std.math.mul(usize, 2, try std.math.add(usize, try footprint(bound_bytes, 1), try footprint(0, 1)));
        const per_run = try std.math.add(usize, try footprint(@sizeOf(Payload), @alignOf(Payload)), try std.math.add(usize, bounds, try footprint(path_bytes, 1)));
        return std.math.add(usize, try Tree.sequentialAllocationBound(count_, count_), try std.math.mul(usize, count_, per_run));
    }

    pub fn singleInsertAllocationBound(max_count: usize, bound_bytes: usize, path_bytes: usize) !usize {
        return std.math.add(usize, try freshAllocationBound(1, bound_bytes, path_bytes), try Tree.sequentialAllocationBound(max_count, 1));
    }
    tree: Tree = .{},
    destroy_run: ?DestroyRun = null,
    retired_next: ?*Store = null,
    // Explicit test/oracle projection. No production writer reads or maintains
    // this vector; structural edits invalidate it, like any borrowed cursor.
    test_projection: if (@import("builtin").is_test) ?[]Run else void = if (@import("builtin").is_test) null else {},

    pub fn count(self: *const Store) usize {
        return if (self.tree.root) |root| root.count else 0;
    }
    pub fn at(self: *const Store, rank: usize) *Run {
        return self.tree.root.?.at(rank).run;
    }
    pub fn find(self: *const Store, run: *const Run) ?*Run {
        return if (Tree.find(self.tree.root, .{ .run = @constCast(run) })) |node| node.entry.run else null;
    }
    pub fn rankOf(self: *const Store, run: *const Run) ?usize {
        if (self.find(run) == null) return null;
        return self.tree.root.?.lowerBound(.{ .run = @constCast(run) });
    }

    /// Only immutable fields are read from a pinned writer root. Mutable
    /// cache hints may be changing concurrently and belong to the new record.
    pub fn revision(source: *const Run, metadata: Run) Run {
        var run = metadata;
        run.owner = source.owner;
        run.path = source.path;
        run.smallest_namespace_name = source.smallest_namespace_name;
        run.smallest_key = source.smallest_key;
        run.largest_namespace_name = source.largest_namespace_name;
        run.largest_key = source.largest_key;
        run.state = if (source.path == null) source.state else null;
        run.bloom_filter = null;
        run.owns_bloom_filter = false;
        run.table_index = null;
        run.version_ref_pinned = false;
        return run;
    }
    pub fn levelStart(self: *const Store, level: u32) usize {
        var root = self.tree.root;
        var rank: usize = 0;
        while (root) |node| {
            if (node.entry.run.level < level) {
                rank += 1 + (if (node.left) |left| left.count else 0);
                root = node.right;
            } else root = node.left;
        }
        return rank;
    }
    pub fn countLevel(self: *const Store, level: u32) usize {
        return (if (level == std.math.maxInt(u32)) self.count() else self.levelStart(level + 1)) - self.levelStart(level);
    }
    pub fn maxLevel(self: *const Store) u32 {
        return if (self.count() == 0) 0 else self.at(self.count() - 1).level;
    }
    pub fn totalBytes(self: *const Store) u64 {
        return if (self.tree.root) |root| root.summary.run_bytes else 0;
    }
    pub fn l0Files(self: *const Store) usize {
        return if (self.tree.root) |root| root.summary.l0_files else 0;
    }
    pub fn l0Bytes(self: *const Store) u64 {
        return if (self.tree.root) |root| root.summary.l0_bytes else 0;
    }
    pub fn manifestBytes(self: *const Store) u64 {
        return if (self.tree.root) |root| root.summary.manifest_bytes else 0;
    }
    pub fn fork(self: *const Store) Store {
        return .{ .tree = self.tree.fork(), .destroy_run = self.destroy_run };
    }
    pub fn emptyLike(self: *const Store) Store {
        return .{ .tree = .{ .account = if (self.tree.account) |account| account.retain() else null }, .destroy_run = self.destroy_run };
    }
    pub fn deinit(self: *Store, allocator: std.mem.Allocator) void {
        self.invalidateProjection(allocator);
        self.tree.deinit(allocator);
        self.* = .{ .destroy_run = self.destroy_run };
    }
    pub const Reclaimer = struct {
        store: *Store,
        tree: Tree.Reclaimer,
        pub fn init(store: *Store) @This() {
            if (store.tree.account) |account| _ = account.retain();
            return .{ .store = store, .tree = .init(store.tree) };
        }
        pub fn step(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
            return self.tree.step(allocator, credits);
        }
        pub fn finish(self: *@This(), allocator: std.mem.Allocator) void {
            self.store.invalidateProjection(allocator);
            if (self.store.tree.account) |account| account.release();
            allocator.destroy(self.store);
        }
    };
    pub fn memoryBytes(self: *const Store, pass: u64) u64 {
        const prepaid_spares = if (self.tree.spare_allocator) |allocator| completion_allocation.isPrepaid(allocator) else false;
        return (if (prepaid_spares) @as(u64, 0) else self.tree.spare.capacity * @sizeOf(*Tree.Node)) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0);
    }
    pub const Cursor = struct {
        store: *const Store,
        index: Tree.Cursor = .{},
        rank: usize = 0,
        pub fn next(self: *@This()) ?*Run {
            if (self.rank == self.store.count()) return null;
            defer self.rank += 1;
            return self.index.at(self.store.tree.root.?, self.rank).run;
        }
    };
    pub fn cursor(self: *const Store) Cursor {
        return .{ .store = self };
    }

    pub fn project(self: *const Store, allocator: std.mem.Allocator) ![]Run {
        const runs = try allocator.alloc(Run, self.count());
        var iterator = self.cursor();
        for (runs) |*run| run.* = iterator.next().?.*;
        return runs;
    }
    pub fn testItems(self: *Store, allocator: std.mem.Allocator) ![]const Run {
        if (!@import("builtin").is_test) @compileError("writer projections are test/oracle only");
        if (self.test_projection == null) self.test_projection = try self.project(allocator);
        return self.test_projection.?;
    }
    fn invalidateProjection(self: *Store, allocator: std.mem.Allocator) void {
        if (@import("builtin").is_test) {
            if (self.test_projection) |projection| allocator.free(projection);
            self.test_projection = null;
        }
    }

    /// Staged payloads borrow incoming ownership. Aborting a prepared root
    /// frees its tree/payloads but leaves the caller's SST metadata intact.
    pub fn stage(self: *Store, allocator: std.mem.Allocator, run: Run) !void {
        return self.stageInternal(allocator, run, false);
    }
    pub fn stageRevision(self: *Store, allocator: std.mem.Allocator, run: Run) !void {
        return self.stageInternal(allocator, run, true);
    }
    fn stageInternal(self: *Store, allocator: std.mem.Allocator, run: Run, overwrite: bool) !void {
        var probe = run;
        if (!overwrite and Tree.find(self.tree.root, .{ .run = &probe }) != null) return error.DuplicateRun;
        try self.tree.prepare(allocator);
        const payload = try allocator.create(Payload);
        const header_bytes: usize = if (completion_allocation.isPrepaid(allocator)) 0 else @sizeOf(Payload);
        const bytes = header_bytes + if (run.owner != null or completion_allocation.isPrepaid(run.metadata_allocator orelse allocator)) @as(usize, 0) else run.smallest_key.len + run.largest_key.len +
            (if (run.path) |path| path.len else 0) +
            (if (run.smallest_namespace_name) |name| name.len else 0) +
            (if (run.largest_namespace_name) |name| name.len else 0) +
            (if (run.state) |present| present.estimatedMemoryBytes() else 0);
        self.tree.account.?.charge(bytes);
        const parent = if (run.owner) |owner| owner.raw.owner.?.retain() else null;
        payload.* = .{ .allocation_allocator = allocator, .owner = .{ .raw = if (parent) |owner| owner.raw else &payload.run, .destroy = Payload.destroy }, .parent = parent, .run = run, .destroy_run = self.destroy_run, .account = self.tree.account.?, .bytes = bytes };
        payload.run.owner = &payload.owner;
        if (parent != null) {
            payload.run.bloom_filter = null;
            payload.run.owns_bloom_filter = false;
            payload.run.table_index = null;
            payload.run.cached_state_index = null;
            payload.run.cached_index_index = null;
            payload.run.cached_table_index = null;
            payload.run.version_ref_pinned = false;
            if (payload.run.path != null) payload.run.state = null;
        }
        const entry = Entry{ .run = &payload.run, .payload = payload };
        defer entry.deinit(allocator);
        self.tree.putPrepared(allocator, entry);
        self.invalidateProjection(allocator);
    }
    pub fn adopt(self: *Store, run: *const Run) void {
        const payload = Tree.find(self.tree.root, .{ .run = @constCast(run) }).?.entry.payload.?;
        std.debug.assert(!payload.owned);
        payload.owned = true;
    }
    pub fn append(self: *Store, allocator: std.mem.Allocator, run: Run) !void {
        try self.stage(allocator, run);
        self.adopt(&run);
        if (run.owner) |owner| owner.release(allocator);
    }
    pub fn reindexForTest(self: *Store, allocator: std.mem.Allocator) !void {
        if (!@import("builtin").is_test) @compileError("fixture metadata edits require test mode");
        var rebuilt = self.emptyLike();
        errdefer rebuilt.deinit(allocator);
        var source = self.cursor();
        while (source.next()) |run| try rebuilt.stage(allocator, run.*);
        source = self.cursor();
        while (source.next()) |run| {
            rebuilt.adopt(run);
        }
        std.mem.swap(Store, self, &rebuilt);
        rebuilt.deinit(allocator);
    }
    pub fn remove(self: *Store, allocator: std.mem.Allocator, run: *const Run) !void {
        try self.tree.prepare(allocator);
        self.tree.removePrepared(allocator, .{ .run = @constCast(run) });
        self.invalidateProjection(allocator);
    }
    pub fn replace(self: *Store, allocator: std.mem.Allocator, old: *Run, replacement: Run) !void {
        var candidate = try self.prepareReplace(allocator, old, replacement);
        std.mem.swap(Store, self, &candidate);
        candidate.deinit(allocator);
    }
    pub fn prepareReplace(self: *const Store, allocator: std.mem.Allocator, old: *Run, replacement: Run) !Store {
        var candidate = self.fork();
        errdefer candidate.deinit(allocator);
        try candidate.remove(allocator, old);
        try candidate.stage(allocator, replacement);
        candidate.adopt(&replacement);
        return candidate;
    }
};

test "writer owner narrow publication scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const allocator = std.heap.smp_allocator;
    const clock = @import("antfly_platform").time;
    for ([_]usize{ 1000, 10000, 100000 }) |count_runs| {
        var live: Store = .{};
        defer live.deinit(allocator);
        const template = Run{ .id = 0, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null };
        for (0..count_runs) |i| {
            var run = template;
            run.id = i + 1;
            try live.append(allocator, run);
        }
        const baseline = try live.project(allocator);
        defer allocator.free(baseline);
        var tree_ns: u64 = 0;
        var array_ns: u64 = 0;
        for (0..31) |_| {
            const array_start = clock.monotonicNs();
            const array = try allocator.alloc(Run, count_runs - 3);
            @memcpy(array[0 .. count_runs - 4], baseline[4..]);
            var output = template;
            output.id = count_runs + 1;
            array[array.len - 1] = output;
            @import("compaction.zig").sortRuns(array);
            std.mem.doNotOptimizeAway(array[0].id);
            allocator.free(array);
            array_ns += clock.monotonicNs() - array_start;
            const tree_start = clock.monotonicNs();
            var candidate = live.fork();
            for (0..4) |i| try candidate.remove(allocator, live.at(i));
            try candidate.append(allocator, output);
            std.mem.doNotOptimizeAway(candidate.at(0).id);
            candidate.deinit(allocator);
            tree_ns += clock.monotonicNs() - tree_start;
        }
        std.debug.print("writer-owner runs={d} changed=5 array_mean_ns={d} tree_mean_ns={d} retained_bytes={d}\n", .{ count_runs, array_ns / 31, tree_ns / 31, live.memoryBytes(@import("memory_account.zig").nextPass()) });
    }
}

test "writer run store preserves prepaid metadata allocator through ordinary revisions and reader pins" {
    var prepaid = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var ordinary = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const reserved_alloc = prepaid.allocator();
    const ordinary_alloc = ordinary.allocator();
    var run: Run = .{
        .id = 1,
        .level = 0,
        .size_bytes = 1,
        .path = try reserved_alloc.dupe(u8, "reserved.sst"),
        .smallest_namespace_name = try reserved_alloc.dupe(u8, "docs"),
        .smallest_key = try reserved_alloc.dupe(u8, "a"),
        .largest_namespace_name = try reserved_alloc.dupe(u8, "docs"),
        .largest_key = try reserved_alloc.dupe(u8, "z"),
        .entry_count = 1,
        .bloom_filter = null,
        .state = null,
        .metadata_allocator = reserved_alloc,
    };
    var live: Store = .{};
    try live.append(reserved_alloc, run);
    var pinned = live.fork();
    // Mutable cache data is created later by ordinary reads. It must not be
    // retired through the allocator which owns the immutable run metadata.
    live.at(0).state = .{};
    try live.at(0).state.?.upsert(ordinary_alloc, .{}, "cache", "value", false);
    var metadata = live.at(0).*;
    metadata.level = 1;
    const changed = Store.revision(live.at(0), metadata);
    try live.remove(ordinary_alloc, live.at(0));
    try live.stageRevision(ordinary_alloc, changed);
    try std.testing.expectEqualStrings("reserved.sst", live.at(0).path.?);
    live.deinit(ordinary_alloc);
    try std.testing.expect(prepaid.freed_bytes < prepaid.allocated_bytes);
    try std.testing.expectEqualStrings("a", pinned.at(0).smallest_key);
    pinned.deinit(ordinary_alloc);
    try std.testing.expectEqual(prepaid.allocated_bytes, prepaid.freed_bytes);
    try std.testing.expectEqual(ordinary.allocated_bytes, ordinary.freed_bytes);
    // Ownership transferred to the payload; never retire this borrowed copy.
    run = undefined;
}

test "writer run store prepaid arena remains singly charged through directory and reader retention" {
    const resources = @import("../resource_manager.zig");
    const accounting = @import("memory_account.zig");
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    const ordinary = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = ordinary });
    defer manager.deinit(ordinary);
    const arena = try completion_allocation.Arena.create(ordinary, &manager, 256 * 1024);
    const allocation = arena.allocator();
    const slab_charge = manager.snapshot().memory.used_bytes;
    try std.testing.expect(slab_charge >= 256 * 1024);
    var live: Store = .{};
    const run: Run = .{
        .id = 1,
        .level = 0,
        .size_bytes = 1,
        .path = try allocation.dupe(u8, "reserved.sst"),
        .smallest_namespace_name = null,
        .smallest_key = try allocation.dupe(u8, "a"),
        .largest_namespace_name = null,
        .largest_key = try allocation.dupe(u8, "z"),
        .entry_count = 1,
        .bloom_filter = null,
        .state = null,
        .metadata_allocator = allocation,
    };
    try live.append(allocation, run);
    const directory = try Directory.create(allocation);
    var fixture: Fixture = .{ .allocator = allocation };
    try directory.put(&fixture, live.at(0).*);
    const pinned = directory.at(0).retain();
    var tracked: u64 = 0;
    const pass = accounting.nextPass();
    const extra = live.memoryBytes(pass) + directory.accountedMemoryBytes(pass);
    try std.testing.expectEqual(@as(u64, 0), extra);
    manager.observeUsage(.lsm_in_memory_state, &tracked, extra);
    try std.testing.expectEqual(slab_charge, manager.snapshot().memory.used_bytes);
    // Ordinary edits coexist with the prepaid generation; only their fresh
    // allocations enter the aggregate observer, never the prepaid slab again.
    var metadata = live.at(0).*;
    metadata.level = 1;
    const replacement = Store.revision(live.at(0), metadata);
    var old = live.fork();
    try live.remove(ordinary, live.at(0));
    try live.stageRevision(ordinary, replacement);
    const next_pass = accounting.nextPass();
    const ordinary_bytes = live.memoryBytes(next_pass) + old.memoryBytes(next_pass) + directory.accountedMemoryBytes(next_pass);
    try std.testing.expect(ordinary_bytes > 0);
    manager.observeUsage(.lsm_in_memory_state, &tracked, ordinary_bytes);
    try std.testing.expectEqual(slab_charge + ordinary_bytes, manager.snapshot().memory.used_bytes);
    arena.release();
    live.deinit(ordinary);
    old.deinit(ordinary);
    directory.destroy(ordinary);
    manager.observeUsage(.lsm_in_memory_state, &tracked, 0);
    try std.testing.expectEqual(slab_charge, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqualStrings("z", pinned.run.largest_key);
    pinned.release(ordinary);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}

test "workload admission completion recycling publication survives persistent run tree readers" {
    const resources = @import("../resource_manager.zig");
    const ordinary = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = ordinary });
    defer manager.deinit(ordinary);
    const publication = try completion_allocation.RecyclingScratch.create(ordinary, &manager, 256 * 1024);
    const reservation = try completion_allocation.PublicationReservation.create(publication, 128 * 1024);
    const allocator = reservation.allocator();
    var live: Store = .{};
    const run: Run = .{
        .id = 1,
        .level = 0,
        .size_bytes = 1,
        .path = try allocator.dupe(u8, "recycled.sst"),
        .smallest_namespace_name = null,
        .smallest_key = try allocator.dupe(u8, "a"),
        .largest_namespace_name = null,
        .largest_key = try allocator.dupe(u8, "z"),
        .entry_count = 1,
        .bloom_filter = null,
        .state = null,
        .metadata_allocator = allocator,
    };
    try live.append(allocator, run);
    var reader = live.fork();
    const charged = manager.snapshot().memory.used_bytes;
    reservation.finish();
    publication.retire();
    live.deinit(ordinary);
    try std.testing.expectEqual(charged, manager.snapshot().memory.used_bytes);
    try std.testing.expectEqualStrings("recycled.sst", reader.at(0).path.?);
    try std.testing.expectEqualStrings("z", reader.at(0).largest_key);
    reader.deinit(ordinary);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
}
