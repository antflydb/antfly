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
    tree: Tree = .{},
    retired_next: ?*Directory = null,

    pub fn create(allocator: std.mem.Allocator) !*Directory {
        const self = try allocator.create(Directory);
        self.* = .{};
        return self;
    }
    pub fn fork(self: *const Directory, allocator: std.mem.Allocator) !*Directory {
        const out = try create(allocator);
        out.tree = self.tree.fork();
        return out;
    }
    pub fn destroy(self: *Directory, allocator: std.mem.Allocator) void {
        self.tree.deinit(allocator);
        allocator.destroy(self);
    }
    pub fn put(self: *Directory, backend: anytype, run: Run) !void {
        const allocator = backend.allocator;
        try self.tree.prepare(allocator);
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
        const entry = Entry{ .run = &payload.run, .payload = payload };
        defer entry.deinit(allocator);
        self.tree.putPrepared(allocator, entry);
    }
    pub fn remove(self: *Directory, allocator: std.mem.Allocator, run: *const Run) !void {
        try self.tree.prepare(allocator);
        self.tree.removePrepared(allocator, .{ .run = run });
    }
    pub fn count(self: *const Directory) usize {
        return if (self.tree.root) |root| root.count else 0;
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
        return @sizeOf(Directory) + self.tree.spare.capacity * @sizeOf(*Tree.Node) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0);
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
