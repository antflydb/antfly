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

//! One bounded metadata window may overlap current-window scoring. The owner
//! joins or cancels it before releasing source/publication state. Saturation
//! leaves required work on the caller and never creates a private thread pool.
const std = @import("std");
const scheduler = @import("antfly_local_sources").sql_parallel_scheduler;
const Window = @import("lake_index_text_directory.zig").RoutingCursor.Window;
const Id = [32]u8;
const Pin = @import("antfly_local_sources").storage_db_query_search_exec.PinnedTextSource;
const Position = @import("lake_index_text_directory.zig").Position;
const Self = @This();
pub const Source = struct {
    ptr: *anyopaque,
    next: *const fn (*anyopaque, []Id, ?f32) anyerror!?Window,
    next_ranked: ?*const fn (*anyopaque, []Id, ?f32, ?Position) anyerror!?Window = null,
    prepare: ?*const fn (*anyopaque, []const Id) anyerror!?Pin = null,
};
source: Source,
io: ?std.Io,
scheduling: *scheduler.Scheduler,
ids: [64]Id = undefined,
limit: usize = 1,
cutoff: ?f32 = null,
position: ?Position = null,
task: ?scheduler.Task(anyerror!?Window) = null,
prepared: ?Pin = null,
pub fn init(source: Source, io: ?std.Io) Self {
    return .{ .source = source, .io = io, .scheduling = scheduler.global() };
}
fn run(self: *Self) anyerror!?Window {
    const window = (if (self.source.next_ranked) |ranked| try ranked(self.source.ptr, self.ids[0..self.limit], self.cutoff, self.position) else try self.source.next(self.source.ptr, self.ids[0..self.limit], self.cutoff)) orelse return null;
    if (self.source.prepare) |prepare| {
        var pin = (prepare(self.source.ptr, window.identities) catch null) orelse return window;
        self.prepared = pin.share(std.heap.page_allocator) catch {
            pin.deinit();
            return window;
        };
    }
    return window;
}
pub fn next(self: *Self, limit: usize, cutoff: ?f32) !?Window {
    return self.nextRanked(limit, cutoff, null);
}
pub fn nextRanked(self: *Self, limit: usize, cutoff: ?f32, position: ?Position) !?Window {
    if (self.task) |task| {
        var pending = task;
        self.task = null;
        return pending.await(self.io.?);
    }
    self.limit = @min(self.ids.len, @max(1, limit));
    self.cutoff = cutoff;
    self.position = position;
    return self.run();
}
pub fn takePrepared(self: *Self) ?Pin {
    const pin = self.prepared;
    self.prepared = null;
    return pin;
}
pub fn prefetch(self: *Self, limit: usize, cutoff: ?f32) void {
    self.prefetchRanked(limit, cutoff, null);
}
pub fn prefetchRanked(self: *Self, limit: usize, cutoff: ?f32, position: ?Position) void {
    std.debug.assert(self.task == null and self.prepared == null);
    self.limit = @min(self.ids.len, @max(1, limit));
    self.cutoff = cutoff;
    self.position = position;
    if (self.io) |io| self.task = self.scheduling.submitIoTransient(io, 16 * 1024 * 1024, run, .{self});
}
pub fn deinit(self: *Self) void {
    if (self.task) |task| {
        var pending = task;
        self.task = null;
        _ = pending.cancel(self.io.?) catch {};
    }
    if (self.prepared) |*pin| pin.deinit();
    self.prepared = null;
}

test "external lake bounded window lookahead joins cancellation and yields under saturation" {
    const State = struct {
        io: std.Io,
        started: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        joined: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(usize) = .init(0),
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.released.fetchAdd(1, .monotonic);
        }
        fn prepare(raw: *anyopaque, _: []const Id) !?Pin {
            var writer = try @import("antfly_local_sources").index.IndexWriter.init(std.testing.allocator);
            defer writer.deinit();
            const pin: Pin = .{ .snapshot = writer.acquireSnapshot(), .name = "prepared", .text_analysis = .{}, .runtime_schema = null, .owner = raw, .release_owner = release };
            return pin;
        }
        fn next(raw: *anyopaque, ids: []Id, _: ?f32) !?Window {
            const self: *@This() = @ptrCast(@alignCast(raw));
            defer self.joined.store(true, .release);
            self.started.set(self.io);
            try self.gate.wait(self.io);
            ids[0] = @splat(7);
            return .{ .identities = ids[0..1], .doc_offset = 9 };
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |cancel| {
        var state: State = .{ .io = io };
        var scheduling: scheduler.Scheduler = .{};
        var ahead = Self.init(.{ .ptr = &state, .next = State.next, .prepare = State.prepare }, io);
        ahead.scheduling = &scheduling;
        defer ahead.deinit();
        ahead.prefetch(64, null);
        try std.testing.expect(ahead.task != null);
        try state.started.wait(io);
        if (cancel) {
            ahead.deinit();
        } else {
            state.gate.set(io);
            const window = (try ahead.next(64, null)).?;
            try std.testing.expectEqual(@as(u64, 9), window.doc_offset);
            try std.testing.expectEqual(@as(Id, @splat(7)), window.identities[0]);
            var pin = ahead.takePrepared().?;
            try std.testing.expectEqualStrings("prepared", pin.name);
            ahead.deinit();
            try std.testing.expectEqual(@as(usize, 0), state.released.load(.acquire));
            pin.deinit();
            try std.testing.expectEqual(@as(usize, 1), state.released.load(.acquire));
        }
        try std.testing.expect(state.joined.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduling.io_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduling.bytes);
    }
    var state: State = .{ .io = io };
    state.gate.set(io);
    var scheduling: scheduler.Scheduler = .{ .max_io_workers = 0 };
    var ahead = Self.init(.{ .ptr = &state, .next = State.next, .prepare = State.prepare }, io);
    ahead.scheduling = &scheduling;
    defer ahead.deinit();
    ahead.prefetch(1000, 1);
    try std.testing.expect(ahead.task == null);
    try std.testing.expectEqual(@as(usize, 64), ahead.limit);
    try std.testing.expect((try ahead.next(64, 2)) != null);
}
