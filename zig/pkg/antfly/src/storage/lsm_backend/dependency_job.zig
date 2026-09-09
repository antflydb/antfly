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

//! Revalidate stable input identities and dependency coverage without an
//! unbounded locked walk or a resizing membership hash table.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const state = @import("state.zig");
const Member = struct {
    id: u64,
    pub fn retainShared(self: @This()) @This() {
        return self;
    }
    pub fn deinit(_: @This(), _: std.mem.Allocator) void {}
    pub fn retainedBytes(_: @This()) usize {
        return 0;
    }
    fn compare(a: @This(), b: @This()) std.math.Order {
        return std.math.order(a.id, b.id);
    }
};

test "dependency validation budgets identities overlaps and cleanup" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *@import("repository.zig").Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *@import("repository.zig").Run) void {}
        fn check(allocator: std.mem.Allocator, directory: *Directory, handles: []const Directory.Handle) !void {
            var job = Job.init(directory, .{ .input_handles = @as(?[]const Directory.Handle, handles), .source_level = @as(u32, 0), .output_level = @as(u32, 1), .tombstone_gc = false, .split_gc = false });
            defer job.deinit(allocator);
            try std.testing.expect(!try job.step(allocator, 1, 0));
            try std.testing.expect(job.indices == null);
            var slices: usize = 0;
            while (!try job.step(allocator, 1, std.math.maxInt(u64))) slices += 1;
            try std.testing.expect(slices > 2);
            try std.testing.expect(job.valid);
            try std.testing.expect(!job.covered);
            try std.testing.expect(try job.step(allocator, 0, 0));
            while (true) {
                var credits: usize = 1;
                if (job.deinitStep(allocator, &credits)) break;
            }
        }
    };
    const allocator = std.testing.allocator;
    var fixture = Fixture{ .allocator = allocator };
    const directory = try Directory.create(allocator);
    defer directory.destroy(allocator);
    for (0..3) |i| {
        try directory.put(&fixture, .{ .id = i + 1, .level = @intCast(i), .size_bytes = 1, .path = @constCast("dependency.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("c"), .entry_count = 1, .bloom_filter = null, .state = null });
    }
    const handles = [_]Directory.Handle{ directory.at(0), directory.at(1) };
    try std.testing.checkAllAllocationFailures(allocator, Fixture.check, .{ directory, &handles });
    const changed = try directory.fork(allocator);
    defer changed.destroy(allocator);
    var replacement = handles[1].run.*;
    replacement.gc_requested = true;
    try changed.put(&fixture, replacement);
    var stale = Job.init(changed, .{ .input_handles = @as(?[]const Directory.Handle, &handles), .source_level = @as(u32, 0), .output_level = @as(u32, 1), .tombstone_gc = false, .split_gc = false });
    defer stale.deinit(allocator);
    while (!try stale.step(allocator, 1, std.math.maxInt(u64))) {}
    try std.testing.expect(!stale.valid);
}
const Members = @import("ordered_index.zig").SummarizedIndex(Member, Member.compare, void);
test "dependency certificate rebases newer writes and rejects changed inputs or older dependencies" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *@import("repository.zig").Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *@import("repository.zig").Run) void {}
    };
    const allocator = std.testing.allocator;
    var fixture = Fixture{ .allocator = allocator };
    const base = try Directory.create(allocator);
    defer base.destroy(allocator);
    for (0..2) |i| try base.put(&fixture, .{ .id = i + 1, .level = @intCast(i), .size_bytes = 1, .path = @constCast("delta.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("c"), .entry_count = 1, .bloom_filter = null, .state = null });
    const handles = [_]Directory.Handle{ base.at(0), base.at(1) };
    for (0..5) |variant| {
        var job = Job.init(base, .{ .input_handles = @as(?[]const Directory.Handle, &handles), .source_level = @as(u32, 0), .output_level = @as(u32, 1), .tombstone_gc = variant == 4, .split_gc = false });
        defer job.deinit(allocator);
        while (!try job.step(allocator, 1, std.math.maxInt(u64))) {}
        try std.testing.expect(job.valid and job.covered);
        // Delta certification must still work after bounded scratch cleanup.
        job.deinit(allocator);
        const latest = try base.fork(allocator);
        defer latest.destroy(allocator);
        var change = handles[0].run.*;
        change.id = 10;
        if (variant == 1) {
            change = handles[1].run.*;
            change.gc_requested = true;
        } else if (variant == 2 or variant == 3) change.level = if (variant == 2) 2 else 1;
        try latest.put(&fixture, change);
        var cursor = Directory.ChangeCursor.init(base, latest);
        while (!cursor.done() and job.valid) {
            var credit: usize = 1;
            if (cursor.next(&credit)) |delta| _ = job.acceptChange(delta);
        }
        try std.testing.expectEqual(variant == 0 or variant == 2 or variant == 4, job.valid);
        if (variant == 2) try std.testing.expect(!job.covered);
    }
}

test "dependency certificate delta scaling benchmark" {
    if (@import("builtin").mode != .ReleaseFast) return error.SkipZigTest;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *@import("repository.zig").Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *@import("repository.zig").Run) void {}
    };
    const allocator = std.heap.smp_allocator;
    const clock = @import("antfly_platform").time;
    var fixture = Fixture{ .allocator = allocator };
    for ([_]usize{ 1000, 10000, 100000 }) |count| {
        const base = try Directory.create(allocator);
        defer base.destroy(allocator);
        for (0..count) |i| try base.put(&fixture, .{ .id = i + 1, .level = 0, .size_bytes = 1, .path = @constCast("bench.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("c"), .entry_count = 1, .bloom_filter = null, .state = null });
        const handles = try allocator.alloc(Directory.Handle, count);
        defer allocator.free(handles);
        var read = base.readCursor();
        for (handles) |*handle| handle.* = read.next().?;
        var job = Job.init(base, .{ .input_handles = @as(?[]const Directory.Handle, handles), .source_level = @as(u32, 0), .output_level = @as(u32, 1), .tombstone_gc = true, .split_gc = false });
        defer job.deinit(allocator);
        const started = clock.monotonicNs();
        var max_slice: u64 = 0;
        var slices: usize = 0;
        while (true) {
            const before = clock.monotonicNs();
            const done = try job.step(allocator, 2048, before +| 2 * std.time.ns_per_ms);
            max_slice = @max(max_slice, clock.monotonicNs() - before);
            slices += 1;
            if (done) break;
        }
        const scan_ns = clock.monotonicNs() - started;
        const latest = try base.fork(allocator);
        defer latest.destroy(allocator);
        var added = handles[0].run.*;
        added.id = count + 1;
        try latest.put(&fixture, added);
        var delta_ns: u64 = 0;
        var visits: usize = 0;
        for (0..31) |_| {
            var cursor = Directory.ChangeCursor.init(base, latest);
            const before = clock.monotonicNs();
            while (!cursor.done()) {
                var credit: usize = 64;
                if (cursor.next(&credit)) |change| {
                    const accepted = job.acceptChange(change);
                    std.mem.doNotOptimizeAway(accepted);
                    if (!accepted) return error.InvalidBenchmarkCertificate;
                }
                visits += 64 - credit;
            }
            std.mem.doNotOptimizeAway(job.covered);
            delta_ns += clock.monotonicNs() - before;
        }
        std.debug.print("dependency-delta inputs={d} initial_ns={d} slices={d} max_slice_ns={d} delta_mean_ns={d} delta_visits={d}\n", .{ count, scan_ns, slices, max_slice, delta_ns / 31, visits / 31 });
    }
}

pub const Job = struct {
    directory: *const Directory,
    handles: []const Directory.Handle,
    indices: ?[]usize = null,
    members: Members = .{},
    reclaimer: ?Members.Reclaimer = null,
    index: usize = 0,
    cursor: ?Directory.OverlapCursor = null,
    lower_ns: ?[]const u8 = null,
    lower: []const u8 = "",
    upper_ns: ?[]const u8 = null,
    upper: []const u8 = "",
    source_level: u32,
    output_level: u32,
    visibility: u64,
    full_gc: bool,
    split_gc: bool,
    valid: bool = true,
    covered: bool = true,
    done: bool = false,

    pub fn init(directory: *const Directory, plan: anytype) Job {
        const handles = plan.input_handles.?;
        const first = handles[0].run;
        return .{ .directory = directory, .handles = handles, .source_level = plan.source_level, .output_level = plan.output_level, .visibility = if (first.visibility_id == 0) first.id else first.visibility_id, .full_gc = plan.tombstone_gc, .split_gc = plan.split_gc };
    }
    fn compare(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8) std.math.Order {
        const order = state.compareNamespace(.{ .name = a_ns }, .{ .name = b_ns });
        return if (order == .eq) std.mem.order(u8, a, b) else order;
    }
    pub fn step(self: *Job, allocator: std.mem.Allocator, credits_arg: usize, deadline: u64) !bool {
        if (self.done) return true;
        var credits = credits_arg;
        while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
            if (self.indices == null) self.indices = try allocator.alloc(usize, self.handles.len);
            if (self.index < self.handles.len) {
                credits -= 1;
                const handle = self.handles[self.index];
                self.indices.?[self.index] = self.directory.resolve(handle) orelse {
                    self.valid = false;
                    self.done = true;
                    return true;
                };
                const run = handle.run;
                if (self.index == 0 or compare(run.smallest_namespace_name, run.smallest_key, self.lower_ns, self.lower) == .lt) {
                    self.lower_ns = run.smallest_namespace_name;
                    self.lower = run.smallest_key;
                }
                if (self.index == 0 or compare(run.largest_namespace_name, run.largest_key, self.upper_ns, self.upper) == .gt) {
                    self.upper_ns = run.largest_namespace_name;
                    self.upper = run.largest_key;
                }
                try self.members.prepare(allocator);
                self.members.putPrepared(allocator, .{ .id = run.id });
                self.index += 1;
                continue;
            }
            if (self.cursor == null) self.cursor = self.directory.overlaps(self.lower_ns, self.lower, self.upper_ns, self.upper);
            if (self.cursor.?.next(&credits)) |handle| {
                const run = handle.run;
                if (Members.find(self.members.root, .{ .id = run.id }) != null) continue;
                if (!self.acceptOutside(run)) {
                    self.done = true;
                    return true;
                }
            } else if (self.cursor.?.done()) {
                self.done = true;
                return true;
            }
        }
        return false;
    }
    fn acceptOutside(self: *Job, run: *const @import("repository.zig").Run) bool {
        const visibility = if (run.visibility_id == 0) run.id else run.visibility_id;
        const newer = run.level < self.source_level or (self.source_level == 0 and run.level == 0 and visibility > self.visibility);
        const older_l0 = self.source_level == 0 and run.level == 0 and !newer;
        if (!self.split_gc and ((self.full_gc and !newer) or older_l0 or run.level == self.output_level)) self.valid = false;
        if (!newer) self.covered = false;
        return self.valid;
    }

    /// Extend a completed dependency certificate through only changed paths.
    /// Input changes invalidate it; unrelated edits and genuinely newer L0
    /// writes neither restart the broad scan nor alter its stable addresses.
    pub fn acceptChange(self: *Job, change: Directory.ChangeCursor.Change) bool {
        const run = change.run;
        // Handles are emitted in read order. A comparator-key change emits a
        // removal of the old key first, so this also catches level/key moves.
        if (Directory.containsReadOrdered(self.handles, run)) {
            self.valid = false;
            return false;
        }
        if (change.kind == .remove) return true;
        if (compare(run.largest_namespace_name, run.largest_key, self.lower_ns, self.lower) == .lt or
            compare(run.smallest_namespace_name, run.smallest_key, self.upper_ns, self.upper) == .gt) return true;
        return self.acceptOutside(run);
    }

    pub fn deinitStep(self: *Job, allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.indices) |indices| allocator.free(indices);
        self.indices = null;
        if (self.reclaimer == null) {
            self.reclaimer = .init(self.members);
            self.members = .{};
        }
        return self.reclaimer.?.step(allocator, credits);
    }
    pub fn deinit(self: *Job, allocator: std.mem.Allocator) void {
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(self.deinitStep(allocator, &credits));
    }
};
