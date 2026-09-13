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

//! Resumable component discovery, density evaluation and bounded progress
//! planning. Select the first eligible component in a rotating read-order
//! sweep: a blocked hot component cannot monopolize collection indefinitely.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const Closure = @import("closure_job.zig").Job;
const ResourceManager = @import("../resource_manager.zig").ResourceManager;
const Seen = struct {
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

test "GC objective discovery and cleanup resume within bounded credits" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), _: *@import("repository.zig").Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *@import("repository.zig").Run) void {
            self.pins -= 1;
        }
        fn check(allocator: std.mem.Allocator, directory: *Directory, limit: u64) !void {
            const resources = @import("../resource_manager.zig");
            var manager = ResourceManager.init(.{});
            defer std.debug.assert(manager.sliceStats(.lsm_table_builder_working_set).used_bytes == 0);
            var scratch = resources.BudgetedAllocator.init(&manager, .lsm_table_builder_working_set, allocator, 1);
            defer scratch.deinit();
            var job = Job.init(directory, 0, 0, 0, 100, limit);
            job.scratch_allocator = scratch.allocator();
            job.output_manager = &manager;
            defer job.deinit(allocator);
            try std.testing.expect(!try job.step(allocator, 0, std.math.maxInt(u64)));
            try std.testing.expect(!try job.step(allocator, 17, 0));
            var slices: usize = 0;
            while (!try job.step(allocator, 17, std.math.maxInt(u64))) slices += 1;
            try std.testing.expect(slices > 3);
            try std.testing.expect(job.eligible);
            try std.testing.expectEqual(directory.count(), job.component.?.handles.?.len);
            slices = 0;
            while (true) {
                var credits: usize = 7;
                if (job.deinitStep(allocator, &credits)) break;
                slices += 1;
            }
            try std.testing.expect(slices > 3);
        }
    };
    const allocator = std.testing.allocator;
    var fixture = Fixture{ .allocator = allocator };
    const directory = try Directory.create(allocator);
    var live = true;
    defer if (live) directory.destroy(allocator);
    for (0..33) |i| {
        var key: [8]u8 = undefined;
        var upper: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        std.mem.writeInt(u64, &upper, if (i == 0) 33 else i, .big);
        try directory.put(&fixture, .{ .id = i + 1, .level = if (i == 0) 0 else 1, .size_bytes = 1, .path = @constCast("gc.sst"), .smallest_namespace_name = null, .smallest_key = &key, .largest_namespace_name = null, .largest_key = &upper, .entry_count = 1, .tombstone_count = if (i == 0) 1 else 0, .bloom_filter = null, .state = null });
    }
    for ([_]u64{ 0, 8 }) |limit| try std.testing.checkAllAllocationFailures(allocator, Fixture.check, .{ directory, limit });
    var reclaim = Directory.Reclaimer.init(directory);
    var slices: usize = 0;
    while (true) {
        var credits: usize = 7;
        if (reclaim.step(allocator, &credits)) break;
        slices += 1;
    }
    reclaim.finish(allocator);
    live = false;
    try std.testing.expect(slices > 3);
    try std.testing.expectEqual(@as(usize, 0), fixture.pins);
}
const SeenTree = @import("ordered_index.zig").SummarizedIndex(Seen, Seen.compare, void);

pub const Job = struct {
    scratch_allocator: ?std.mem.Allocator = null,
    output_manager: ?*ResourceManager = null,
    directory: *const Directory,
    cursor: Directory.TombstoneCursor,
    start_rank: usize,
    wrapped: bool = false,
    seen: SeenTree = .{},
    seen_reclaimer: ?SeenTree.Reclaimer = null,
    component: ?Closure = null,
    progress: ?Closure = null,
    anchor: Directory.Handle = undefined,
    oldest: Directory.Handle = undefined,
    index: usize = 0,
    deletes: u64 = 0,
    entries: u64 = 0,
    requested: bool = false,
    intent_runs: usize = 0,
    intent_wire_bytes: u64 = 128,
    intent_data_bytes: u64 = 0,
    eligible: bool = false,
    split: bool = false,
    retry_oldest: bool = false,
    age: u64,
    percent: u8,
    now: u64,
    limit: u64,
    valid_until: u64 = std.math.maxInt(u64),
    phase: enum { scan, component, measure, discard, prepare_progress, progress, retry, done } = .scan,

    pub fn init(directory: *const Directory, start: usize, age: u64, percent: u8, now: u64, limit: u64) Job {
        const rank = if (start < directory.count()) start else 0;
        return .{ .directory = directory, .cursor = .{ .directory = directory, .rank = rank }, .start_rank = rank, .age = age, .percent = @min(percent, 100), .now = now, .limit = limit };
    }
    pub fn step(self: *Job, allocator: std.mem.Allocator, credits_arg: usize, deadline: u64) !bool {
        var credits = credits_arg;
        while (credits != 0 and @import("antfly_platform").time.monotonicNs() < deadline) {
            if (self.phase == .done) return true;
            credits -= 1;
            switch (self.phase) {
                .scan => {
                    const anchor = self.cursor.next() orelse {
                        if (self.wrapped or self.start_rank == 0) {
                            self.phase = .done;
                            continue;
                        }
                        self.wrapped = true;
                        self.cursor.rank = 0;
                        continue;
                    };
                    if (self.wrapped and self.cursor.rank > self.start_rank) {
                        self.phase = .done;
                        continue;
                    }
                    if (SeenTree.find(self.seen.root, .{ .id = anchor.run.id }) != null) continue;
                    self.anchor = anchor;
                    self.oldest = anchor;
                    self.component = try self.initClosure(allocator, anchor, 0, true);
                    self.phase = .component;
                },
                .component, .progress => {
                    const closure = if (self.phase == .component) &self.component.? else &self.progress.?;
                    const quantum = @min(credits + 1, 64);
                    credits = credits + 1 - quantum;
                    if (!try closure.stepUntil(allocator, quantum, deadline)) continue;
                    if (self.phase == .component) {
                        self.phase = .measure;
                        self.index = 0;
                        self.deletes = 0;
                        self.entries = 0;
                        self.requested = false;
                        self.intent_runs = 0;
                        self.intent_wire_bytes = 128;
                        self.intent_data_bytes = 0;
                    } else if (closure.phase != .oversized) {
                        self.phase = .done;
                    } else if (!self.retry_oldest and self.anchor.run.level == 0 and self.oldest.run.id != self.anchor.run.id) {
                        self.retry_oldest = true;
                        self.phase = .retry;
                    } else {
                        self.split = self.anchor.run.entry_count > 1 and self.anchor.run.size_bytes <= self.limit;
                        self.phase = .done;
                    }
                },
                .measure => {
                    const handles = self.component.?.handles.?;
                    if (self.index != handles.len) {
                        const handle = handles[self.index];
                        self.index += 1;
                        const run = handle.run;
                        self.entries = @max(self.entries, run.entry_count);
                        if (run.level == 0 and Directory.readLess({}, self.oldest, handle)) self.oldest = handle;
                        const deletes = run.tombstone_count orelse 0;
                        if (deletes == 0) continue;
                        if (!run.gc_requested) {
                            self.intent_runs += 1;
                            const names = run.smallest_key.len + run.largest_key.len + (if (run.path) |path| path.len else 0) +
                                (if (run.smallest_namespace_name) |name| name.len else 0) + (if (run.largest_namespace_name) |name| name.len else 0);
                            self.intent_wire_bytes +|= 192 +| names;
                            self.intent_data_bytes +|= names +| (if (run.state) |*present| present.estimatedMemoryBytes() else 0);
                        }
                        try self.seen.prepare(self.scratch_allocator orelse allocator);
                        self.seen.putPrepared(self.scratch_allocator orelse allocator, .{ .id = run.id });
                        self.deletes +|= deletes;
                        const due = run.oldest_tombstone_unix_ns +| self.age;
                        const aged = self.age != 0 and (run.oldest_tombstone_unix_ns == 0 or run.oldest_tombstone_unix_ns > self.now or due <= self.now);
                        self.requested = self.requested or run.gc_requested or aged;
                        if (self.age != 0 and !aged) self.valid_until = @min(self.valid_until, due);
                        continue;
                    }
                    self.eligible = self.deletes != 0 and (self.requested or @as(u128, self.deletes) * 100 >= @as(u128, self.entries) * self.percent);
                    if (!self.eligible) {
                        self.phase = .discard;
                        continue;
                    }
                    if (self.limit == 0 or self.component.?.bytes <= self.limit) {
                        self.phase = .done;
                        continue;
                    }
                    self.phase = .prepare_progress;
                },
                .prepare_progress => {
                    var quantum: usize = @min(credits + 1, 64);
                    credits = credits + 1 - quantum;
                    if (!self.component.?.reclaimScratchStep(&quantum)) continue;
                    if (!self.reclaimSeenStep(allocator, &quantum)) continue;
                    self.progress = try self.initClosure(allocator, self.anchor, self.limit, false);
                    self.phase = .progress;
                },
                .discard => {
                    var quantum: usize = @min(credits + 1, 64);
                    credits = credits + 1 - quantum;
                    if (!self.component.?.deinitStep(allocator, &quantum)) continue;
                    self.component = null;
                    self.phase = .scan;
                },
                .retry => {
                    var quantum: usize = @min(credits + 1, 64);
                    credits = credits + 1 - quantum;
                    if (!self.progress.?.deinitStep(allocator, &quantum)) continue;
                    self.progress = null;
                    self.progress = try self.initClosure(allocator, self.oldest, self.limit, false);
                    self.phase = .progress;
                },
                .done => unreachable,
            }
        }
        return self.phase == .done;
    }
    fn initClosure(self: *Job, allocator: std.mem.Allocator, anchor: Directory.Handle, limit: u64, all_levels: bool) !Closure {
        var closure = try Closure.init(self.scratch_allocator orelse allocator, self.directory, &.{anchor}, limit, all_levels);
        closure.output_manager = self.output_manager;
        return closure;
    }

    fn reclaimSeenStep(self: *Job, allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.seen_reclaimer == null) {
            self.seen_reclaimer = .init(self.seen);
            self.seen = .{};
        }
        return self.seen_reclaimer.?.step(self.scratch_allocator orelse allocator, credits);
    }

    pub fn deinitStep(self: *Job, allocator: std.mem.Allocator, credits: *usize) bool {
        if (self.component) |*closure| {
            if (!closure.deinitStep(allocator, credits)) return false;
            self.component = null;
        }
        if (self.progress) |*closure| {
            if (!closure.deinitStep(allocator, credits)) return false;
            self.progress = null;
        }
        return self.reclaimSeenStep(allocator, credits);
    }
    pub fn deinit(self: *Job, allocator: std.mem.Allocator) void {
        var credits: usize = std.math.maxInt(usize);
        std.debug.assert(self.deinitStep(allocator, &credits));
    }
};
