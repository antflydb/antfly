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

//! Query-owned selected-pack prefetch. All tasks join before publication state
//! is released. Saturation leaves required opens inline; pins retain their own
//! native cache/resource-manager admission, separately from task workspace.
const std = @import("std");
const local = @import("antfly_local_sources");
const scheduling = local.sql_parallel_scheduler;
const Pin = local.storage_db_query_search_exec.PinnedTextSource;
const Id = [32]u8;
const Self = @This();
pub const width = 4;
const workspace = 8 * 1024 * 1024;
pub const Source = struct { ptr: *anyopaque, open: *const fn (*anyopaque, Id) anyerror!?Pin };
const Job = struct {
    source: Source,
    id: Id,
    pin: ?Pin = null,
    attempted: bool = false,
    task: ?scheduling.Task(anyerror!void) = null,
    fn run(self: *@This()) anyerror!void {
        self.attempted = true;
        var pin = (self.source.open(self.source.ptr, self.id) catch return) orelse return;
        self.pin = pin.share(std.heap.page_allocator) catch {
            pin.deinit();
            return;
        };
    }
};
io: ?std.Io,
scheduler: *scheduling.Scheduler,
jobs: [width]Job = undefined,
count: usize,
reserved: bool = false,
pub fn init(source: Source, io: ?std.Io, ids: []const Id) Self {
    std.debug.assert(ids.len <= width);
    var self: Self = .{ .io = io, .scheduler = scheduling.global(), .count = ids.len };
    for (self.jobs[0..ids.len], ids) |*job, id| job.* = .{ .source = source, .id = id };
    return self;
}
/// Call only after the wave has reached its final address.
pub fn start(self: *Self) void {
    if (self.io == null or self.count < 2) return;
    self.reserved = self.scheduler.reservePull(0, workspace * self.count);
    if (!self.reserved) return;
    for (self.jobs[0..self.count]) |*job| job.task = self.scheduler.submitIo(self.io.?, workspace, Job.run, .{job});
}
pub fn get(self: *Self, index: usize) !?*const Pin {
    const job = &self.jobs[index];
    if (job.task) |*task| {
        task.await(self.io.?) catch |err| {
            job.task = null;
            return err;
        };
        job.task = null;
    } else if (!job.attempted) try job.run();
    return if (job.pin) |*pin| pin else null;
}
/// A joined consumer can release a completed pin before the next required open.
/// This restores cache headroom when speculative admission was saturated.
pub fn release(self: *Self, index: usize) void {
    const job = &self.jobs[index];
    std.debug.assert(job.task == null);
    if (job.pin) |*pin| pin.deinit();
    job.pin = null;
}
pub fn deinit(self: *Self) void {
    // A canceled job can have completed its pin. Join all jobs before freeing
    // either the result pins or their source callback's publication/context.
    for (self.jobs[0..self.count]) |*job| if (job.task) |*task| {
        task.cancel(self.io.?) catch {};
        job.task = null;
    };
    for (self.jobs[0..self.count]) |*job| if (job.pin) |*pin| pin.deinit();
    if (self.reserved) self.scheduler.releasePull(0, workspace * self.count);
}

test "external lake bounded window profiles selected pack waves overlap join cancel and preserve inline saturation" {
    const State = struct {
        live: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        released: std.atomic.Value(usize) = .init(0),
        created: std.atomic.Value(usize) = .init(0),
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.released.fetchAdd(1, .monotonic);
        }
        fn open(raw: *anyopaque, id: Id) !?Pin {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const live = self.live.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.live.fetchSub(1, .release);
            _ = self.peak.fetchMax(live, .monotonic);
            try std.testing.io.sleep(.fromMilliseconds(20), .awake);
            if (id[0] == 2) return error.InjectedPrefetchFailure;
            var writer = try local.index.IndexWriter.init(std.heap.page_allocator);
            defer writer.deinit();
            _ = self.created.fetchAdd(1, .monotonic);
            return .{ .snapshot = writer.acquireSnapshot(), .name = "pack", .text_analysis = .{}, .runtime_schema = null, .owner = self, .release_owner = release };
        }
    };
    for ([_]usize{ 0, width }) |lanes| {
        var scheduler: scheduling.Scheduler = .{ .max_io_workers = lanes };
        var state: State = .{};
        {
            var wave = init(.{ .ptr = &state, .open = State.open }, std.testing.io, &.{ @splat(0), @splat(1), @splat(2), @splat(3) });
            wave.scheduler = &scheduler;
            defer wave.deinit();
            wave.start();
            try std.testing.expect((try wave.get(0)) != null);
            try std.testing.expect((try wave.get(2)) == null);
            if (lanes == 0) {
                try std.testing.expect((try wave.get(1)) != null);
                try std.testing.expect((try wave.get(3)) != null);
            }
        }
        try std.testing.expectEqual(@as(usize, 0), state.live.load(.acquire));
        try std.testing.expectEqual(state.created.load(.acquire), state.released.load(.acquire));
        if (lanes == 0) try std.testing.expectEqual(@as(usize, 1), state.peak.load(.acquire)) else try std.testing.expect(state.peak.load(.acquire) > 1);
        try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
        try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    }
}
