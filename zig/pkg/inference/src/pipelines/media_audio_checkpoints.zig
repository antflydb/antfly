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

//! Request ownership for optional pause planning metadata. Retained snapshots
//! are borrowed only for lookup; new anchors use bounded inline storage.
//! Merging and durable publication run after the current PCM window retires.
const std = @import("std");
const plan = @import("media_audio_plan.zig");
const Cache = @import("media_index_cache.zig").Cache;
const Admitted = @import("../backends/admitted_allocator.zig").AdmittedAllocator;
const Bounded = @import("../runtime/bounded_allocator.zig").BoundedAllocator;

pub const Owner = struct {
    cache: *Cache,
    admission: Cache.Admission,
    identity: [32]u8,
    store: ?plan.Store.Context,
    allocation: Admitted,
    scratch: Bounded,
    ordinal: u64,
    restore: ?plan.Point = null,
    initial_tail: ?u64 = null,
    pending: [plan.max_points]plan.Point = undefined,
    count: usize = 0,
    finished: bool = false,

    /// Initialize only at the final address: scratch borrows this allocator.
    /// Lookup copies just two scalar markers, releasing both cache views and
    /// disk scratch before inference. Model pressure may reclaim the snapshot.
    pub fn init(self: *Owner, backing: std.mem.Allocator, cache: *Cache, admission: Cache.Admission, identity: [32]u8, store: ?plan.Store.Context, ordinal: u64) void {
        self.* = .{ .cache = cache, .admission = admission, .identity = identity, .store = store, .ordinal = ordinal, .allocation = .{ .backing = backing, .admission_context = admission.context, .admit = admission.acquire, .trim_on_free = true }, .scratch = undefined };
        self.scratch = .{ .backing = self.allocation.allocator(), .limit = 4 * plan.max_encoded_bytes };
        self.observeMemory();
        // Another inference owner can have persisted a newer tail than RAM.
        if (self.initial_tail) |tail| if (tail >= ordinal) return;
        const context = store orelse return;
        const saved = plan.Store.load(context, self.allocator(), identity) catch return;
        defer self.allocator().free(saved.points);
        self.observe(saved);
        // Promote disk-loaded unchanged repairs once, without rewriting disk.
        cache.remember(backing, identity, .{ .audio_plan = saved }, admission);
        self.observeMemory();
    }
    fn allocator(self: *Owner) std.mem.Allocator {
        return self.scratch.allocator();
    }
    fn observeMemory(self: *Owner) void {
        const hit = self.cache.get(self.identity) orelse return;
        defer self.cache.release(hit);
        if (hit.data == .audio_plan) self.observe(hit.data.audio_plan);
    }
    fn observe(self: *Owner, snapshot: plan.Snapshot) void {
        const tail = snapshot.points[snapshot.points.len - 1].ordinal;
        if (self.initial_tail) |old| if (tail <= old) return;
        self.initial_tail = tail;
        self.restore = snapshot.nearest(self.ordinal);
    }
    pub fn checkpoint(self: *const Owner) ?plan.Point {
        return self.restore;
    }
    pub fn append(self: *Owner, point: plan.Point) void {
        if (self.finished) return;
        if (self.initial_tail) |tail| if (point.ordinal <= tail) return;
        if (self.count != 0 and point.ordinal <= self.pending[self.count - 1].ordinal) return;
        // Fixed request-local storage cannot consume PCM admission or pin a
        // cache lease during inference. Long pages retain sparse new anchors.
        if (self.count == self.pending.len) self.count = plan.compact(&self.pending);
        self.pending[self.count] = point;
        self.count += 1;
    }
    fn seed(self: *Owner, merged: *plan.Builder, snapshot: plan.Snapshot) bool {
        const a = self.allocator();
        merged.points.clearRetainingCapacity();
        merged.points.ensureTotalCapacityPrecise(a, @min(plan.max_points, snapshot.points.len + self.count)) catch return false;
        for (snapshot.points) |point| merged.append(a, point) catch return false;
        return true;
    }
    pub fn finish(self: *Owner) void {
        if (self.finished) return;
        self.finished = true;
        if (self.count == 0) return;
        const a = self.allocator();
        var merged = plan.Builder{};
        defer merged.deinit(a);
        const through = self.pending[self.count - 1].ordinal;
        var base_tail: ?u64 = null;
        if (self.cache.get(self.identity)) |hit| {
            defer self.cache.release(hit);
            if (hit.data == .audio_plan) {
                const base = hit.data.audio_plan;
                base_tail = base.points[base.points.len - 1].ordinal;
                if (base_tail.? >= through) return;
                if (!self.seed(&merged, base)) return;
            }
        }
        // Reacquire after PCM retires. If pressure removed our initial view,
        // recover its durable prefix instead of publishing a thinner snapshot.
        if (base_tail == null or (self.initial_tail != null and base_tail.? < self.initial_tail.?)) if (self.store) |context| {
            if (plan.Store.load(context, a, self.identity)) |saved| {
                defer a.free(saved.points);
                const tail = saved.points[saved.points.len - 1].ordinal;
                if (tail >= through) {
                    self.cache.remember(self.allocation.backing, self.identity, .{ .audio_plan = saved }, self.admission);
                    return;
                }
                if (base_tail == null or tail > base_tail.?) {
                    if (!self.seed(&merged, saved)) return;
                    base_tail = tail;
                }
            } else |_| {}
        };
        if (base_tail == null) merged.points.ensureTotalCapacityPrecise(a, self.count) catch return;
        for (self.pending[0..self.count]) |point| merged.append(a, point) catch return;
        self.cache.remember(self.allocation.backing, self.identity, .{ .audio_plan = merged.snapshot() }, self.admission);
        if (self.store) |context| plan.Store.save(context, a, self.identity, merged.snapshot()) catch {};
    }
    pub fn deinit(self: *Owner) void {
        self.allocation.deinit();
    }
};

const Fixture = struct {
    controller: @import("../runtime/tier/memory.zig").AdmissionController = .{},
    requests: usize = 0,
    deny: bool = false,
    fn acquire(raw: *anyopaque, amount: usize) !@import("../runtime/tier/memory.zig").AdmissionLease {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.requests += 1;
        if (self.deny) return error.ResourceLimitExceeded;
        return self.controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = amount }, false);
    }
    fn admission(self: *Fixture) Cache.Admission {
        return .{ .context = self, .acquire = acquire };
    }
};

test "model aware media checkpoint owner borrows unchanged plans without allocation or persistence" {
    const Spy = struct {
        var base: std.Io = undefined;
        var reads: usize = 0;
        var creates: usize = 0;
        fn read(raw: ?*anyopaque, file: std.Io.File, data: []const []u8, offset: u64) std.Io.File.ReadPositionalError!usize {
            reads += 1;
            return base.vtable.fileReadPositional(raw, file, data, offset);
        }
        fn create(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.CreateFileOptions) std.Io.File.OpenError!std.Io.File {
            creates += 1;
            return base.vtable.dirCreateFile(raw, dir, path, options);
        }
    };
    const a = std.testing.allocator;
    var fixture = Fixture{};
    var cache = Cache{};
    defer cache.deinit();
    var points: [plan.max_points]plan.Point = undefined;
    for (&points, 0..) |*point, i| point.* = .{ .ordinal = i, .offset_ns = @intCast(i * 1000) };
    const identity: [32]u8 = @splat(9);
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var root_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buffer, ".zig-cache/tmp/{s}", .{temp.sub_path});
    var store = plan.Store{};
    try plan.Store.save(.{ .store = &store, .io = std.testing.io, .models_dir = root }, a, identity, .{ .points = &points });
    Spy.base = std.testing.io;
    Spy.reads = 0;
    Spy.creates = 0;
    var table = std.testing.io.vtable.*;
    table.fileReadPositional = Spy.read;
    table.dirCreateFile = Spy.create;
    const context = plan.Store.Context{ .store = &store, .io = .{ .userdata = std.testing.io.userdata, .vtable = &table }, .models_dir = root };
    // The first unchanged repair starts from disk and promotes it exactly once.
    {
        var owner: Owner = undefined;
        owner.init(a, &cache, fixture.admission(), identity, context, 2000);
        defer owner.deinit();
        try std.testing.expectEqual(@as(u64, 2000), owner.checkpoint().?.ordinal);
        try std.testing.expect(Spy.reads != 0);
        try std.testing.expectEqual(@as(usize, 1), cache.count);
        try std.testing.expectEqual(@as(usize, 0), owner.allocation.reserved_bytes);
        owner.append(points[2000]);
        owner.append(points[2001]);
        owner.finish();
        try std.testing.expectEqual(@as(usize, 0), Spy.creates);
    }
    Spy.reads = 0;
    Spy.creates = 0;
    const requests = fixture.requests;
    for (0..10) |_| {
        var owner: Owner = undefined;
        owner.init(a, &cache, fixture.admission(), identity, context, 2000);
        defer owner.deinit();
        try std.testing.expectEqual(@as(usize, 0), owner.count);
        try std.testing.expectEqual(@as(usize, 0), owner.allocation.reserved_bytes);
        owner.append(points[2000]);
        owner.append(points[2001]);
        owner.finish();
    }
    try std.testing.expectEqual(requests, fixture.requests);
    try std.testing.expectEqual(@as(usize, 0), Spy.reads);
    try std.testing.expectEqual(@as(usize, 0), Spy.creates);
    var lookup: Owner = undefined;
    lookup.init(a, &cache, fixture.admission(), identity, context, 2000);
    defer lookup.deinit();
    try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) != 0);
    try std.testing.expectEqual(@as(u64, 2000), lookup.checkpoint().?.ordinal);
    var model = try fixture.controller.tryAcquire(.cpu, .{ .host_limit_bytes = Cache.max_bytes }, .{ .host_scratch_bytes = Cache.max_bytes }, false);
    model.release();
    try std.testing.expectEqual(@as(usize, 0), fixture.controller.snapshot().host_scratch_bytes);
}

test "model aware media checkpoint owner isolates admission and allocation failures" {
    const a = std.testing.allocator;
    const points = [_]plan.Point{ .{ .ordinal = 0, .offset_ns = 0 }, .{ .ordinal = 1, .offset_ns = 1000 } };
    for (0..48) |fail_index| {
        var fixture = Fixture{};
        var cache = Cache{};
        const identity: [32]u8 = @splat(1);
        cache.remember(a, identity, .{ .audio_plan = .{ .points = &points } }, fixture.admission());
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        {
            var owner: Owner = undefined;
            owner.init(failing.allocator(), &cache, fixture.admission(), identity, null, 1);
            defer owner.deinit();
            owner.append(.{ .ordinal = 2, .offset_ns = 2000 });
            owner.finish();
        }
        cache.deinit();
        try std.testing.expectEqual(@as(usize, 0), fixture.controller.snapshot().host_scratch_bytes);
    }
    var denied = Fixture{ .deny = true };
    var cache = Cache{};
    defer cache.deinit();
    {
        var owner: Owner = undefined;
        owner.init(a, &cache, denied.admission(), @splat(2), null, 0);
        defer owner.deinit();
        owner.append(.{ .ordinal = 0, .offset_ns = 0 });
        try std.testing.expectEqual(@as(usize, 1), owner.count);
        owner.finish();
    }
    try std.testing.expectEqual(@as(usize, 0), cache.count);
    try std.testing.expectEqual(@as(usize, 0), denied.controller.snapshot().host_scratch_bytes);
}

test "model aware media checkpoint owner records dense pages without heap leases during inference" {
    const a = std.testing.allocator;
    var fixture = Fixture{};
    var cache = Cache{};
    defer cache.deinit();
    const identity: [32]u8 = @splat(7);
    {
        var owner: Owner = undefined;
        owner.init(a, &cache, fixture.admission(), identity, null, 0);
        defer owner.deinit();
        for (0..1000) |i| owner.append(.{ .ordinal = i, .offset_ns = @intCast(i * 1000) });
        try std.testing.expectEqual(@as(usize, 0), fixture.requests);
        try std.testing.expectEqual(@as(usize, 0), owner.allocation.reserved_bytes);
        owner.finish();
        const hit = cache.get(identity).?;
        defer cache.release(hit);
        try std.testing.expectEqual(@as(usize, 1000), hit.data.audio_plan.points.len);
        for (0..1000) |i| try std.testing.expectEqual(@as(u64, i), hit.data.audio_plan.nearest(i).?.ordinal);
    }
    try std.testing.expect(Cache.reclaim(&cache, Cache.max_bytes) != 0);
    try std.testing.expectEqual(@as(usize, 0), fixture.controller.snapshot().host_scratch_bytes);
}
