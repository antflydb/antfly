// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Bounded, joined immutable artifact reads. Callers admit byte budgets before
//! submitting a wave; readers authenticate their own units before returning.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
pub const width = 8;
pub const Read = struct { ptr: *anyopaque, read: *const fn (*anyopaque, A, usize) anyerror![]u8 };
pub const Wave = struct {
    a: A,
    values: [width]?[]u8 = @splat(null),
    pub fn deinit(self: *Wave) void {
        for (&self.values) |*value| if (value.*) |bytes| {
            self.a.free(bytes);
            value.* = null;
        };
    }
    pub fn take(self: *Wave, index: usize) []u8 {
        const value = self.values[index].?;
        self.values[index] = null;
        return value;
    }
};
pub fn load(a: A, io: ?std.Io, first: usize, sizes: []const u64, reader: Read) !Wave {
    if (sizes.len > width) return error.InvalidNativeLakeTextCorpus;
    return loadScheduled(a, io, first, sizes, reader, local.sql_parallel_scheduler.global());
}
fn loadScheduled(a: A, io: ?std.Io, first: usize, sizes: []const u64, reader: Read, scheduler: *local.sql_parallel_scheduler.Scheduler) !Wave {
    var result: Wave = .{ .a = a };
    errdefer result.deinit();
    var retained: usize = 0;
    for (sizes) |bytes| retained +|= @as(usize, @intCast(bytes)) *| 2 +| 64 * 1024;
    const admitted = io != null and scheduler.reservePull(0, retained);
    defer if (admitted) scheduler.releasePull(0, retained);
    const Job = struct {
        reader: Read,
        a: A,
        index: usize,
        result: ?[]u8 = null,
        fn run(self: *@This()) anyerror!void {
            self.result = try self.reader.read(self.reader.ptr, self.a, self.index);
        }
    };
    var allocations: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = a };
    var jobs: [width]Job = undefined;
    for (jobs[0..sizes.len], 0..) |*job, i| job.* = .{ .reader = reader, .a = allocations.allocator(), .index = first + i };
    defer for (jobs[0..sizes.len]) |job| if (job.result) |bytes| a.free(bytes);
    var tasks: [width]?local.sql_parallel_scheduler.Task(anyerror!void) = @splat(null);
    defer if (io) |runtime| for (&tasks) |*task| if (task.*) |*held| {
        held.cancel(runtime) catch {};
    };
    var failure: ?anyerror = null;
    for (jobs[0..sizes.len], sizes, 0..) |*job, bytes, i| {
        if (failure != null) break;
        if (admitted and sizes.len > 1) tasks[i] = scheduler.submitIo(io.?, @as(usize, @intCast(bytes)) * 2 + 64 * 1024, Job.run, .{job});
        if (tasks[i] == null) job.run() catch |err| {
            failure = err;
        };
    }
    if (io) |runtime| for (&tasks) |*task| if (task.*) |*held| {
        held.await(runtime) catch |err| {
            if (failure == null) failure = err;
        };
        task.* = null;
    };
    if (failure) |err| return err;
    for (jobs[0..sizes.len], 0..) |*job, i| {
        result.values[i] = job.result;
        job.result = null;
    }
    return result;
}

test "external lake global frontier artifact waves preserve order and join failed reads" {
    const State = struct {
        calls: std.atomic.Value(usize) = .init(0),
        live: std.atomic.Value(usize) = .init(0),
        fail: ?usize = null,
        fn read(raw: *anyopaque, a: A, index: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            _ = self.live.fetchAdd(1, .monotonic);
            defer _ = self.live.fetchSub(1, .monotonic);
            if (self.fail == index) return error.InjectedFailure;
            return std.fmt.allocPrint(a, "page-{d}", .{index});
        }
    };
    for ([_]?std.Io{ null, std.testing.io }) |io| {
        var state: State = .{};
        var wave = try load(std.testing.allocator, io, 10, &@as([width]u64, @splat(32)), .{ .ptr = &state, .read = State.read });
        defer wave.deinit();
        for (0..width) |i| {
            const bytes = wave.take(i);
            defer std.testing.allocator.free(bytes);
            var expected: [32]u8 = undefined;
            try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "page-{d}", .{10 + i}), bytes);
        }
        try std.testing.expectEqual(width, state.calls.load(.acquire));
        state.calls.store(0, .release);
        state.fail = 13;
        try std.testing.expectError(error.InjectedFailure, load(std.testing.allocator, io, 10, &@as([width]u64, @splat(32)), .{ .ptr = &state, .read = State.read }));
        try std.testing.expectEqual(@as(usize, 0), state.live.load(.acquire));
    }
}

test "external lake global frontier artifact waves overlap bounded I O and fall back under saturation" {
    const State = struct {
        live: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        fn read(raw: *anyopaque, a: A, _: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const live = self.live.fetchAdd(1, .acq_rel) + 1;
            defer _ = self.live.fetchSub(1, .release);
            _ = self.peak.fetchMax(live, .monotonic);
            try std.testing.io.sleep(.fromMilliseconds(20), .awake);
            return a.dupe(u8, "page");
        }
    };
    for ([_]usize{ 0, width }) |lanes| {
        var scheduling: local.sql_parallel_scheduler.Scheduler = .{ .max_io_workers = lanes };
        var state: State = .{};
        var wave = try loadScheduled(std.testing.allocator, std.testing.io, 0, &@as([width]u64, @splat(4)), .{ .ptr = &state, .read = State.read }, &scheduling);
        defer wave.deinit();
        const peak = state.peak.load(.acquire);
        if (lanes == 0) try std.testing.expectEqual(@as(usize, 1), peak) else try std.testing.expect(peak > 1 and peak <= width);
        try std.testing.expectEqual(@as(usize, 0), scheduling.io_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduling.bytes);
        std.debug.print("Lineage wave: lanes={d} peak_reads={d} pages={d}\n", .{ lanes, peak, width });
    }
}
