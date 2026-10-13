// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! One bounded metadata window may overlap current-window scoring. The owner
//! joins or cancels it before releasing source/publication state. Saturation
//! leaves required work on the caller and never creates a private thread pool.
const std = @import("std");
const scheduler = @import("antfly_local_sources").sql_parallel_scheduler;
const Window = @import("lake_index_text_directory.zig").RoutingCursor.Window;
const Id = [32]u8;
const Self = @This();
pub const Source = struct {
    ptr: *anyopaque,
    next: *const fn (*anyopaque, []Id, ?f32) anyerror!?Window,
};
source: Source,
io: ?std.Io,
scheduling: *scheduler.Scheduler,
ids: [64]Id = undefined,
limit: usize = 1,
cutoff: ?f32 = null,
task: ?scheduler.Task(anyerror!?Window) = null,
pub fn init(source: Source, io: ?std.Io) Self {
    return .{ .source = source, .io = io, .scheduling = scheduler.global() };
}
fn run(self: *Self) anyerror!?Window {
    return self.source.next(self.source.ptr, self.ids[0..self.limit], self.cutoff);
}
pub fn next(self: *Self, limit: usize, cutoff: ?f32) !?Window {
    if (self.task) |task| {
        var pending = task;
        self.task = null;
        return pending.await(self.io.?);
    }
    self.limit = @min(self.ids.len, @max(1, limit));
    self.cutoff = cutoff;
    return self.run();
}
pub fn prefetch(self: *Self, limit: usize, cutoff: ?f32) void {
    std.debug.assert(self.task == null);
    self.limit = @min(self.ids.len, @max(1, limit));
    self.cutoff = cutoff;
    if (self.io) |io| self.task = self.scheduling.submitIoTransient(io, 8 * 1024 * 1024, run, .{self});
}
pub fn deinit(self: *Self) void {
    if (self.task) |task| {
        var pending = task;
        self.task = null;
        _ = pending.cancel(self.io.?) catch {};
    }
}

test "external lake bounded window lookahead joins cancellation and yields under saturation" {
    const State = struct {
        io: std.Io,
        started: std.Io.Event = .unset,
        gate: std.Io.Event = .unset,
        joined: std.atomic.Value(bool) = .init(false),
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
        var ahead = Self.init(.{ .ptr = &state, .next = State.next }, io);
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
        }
        try std.testing.expect(state.joined.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), scheduling.io_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduling.bytes);
    }
    var state: State = .{ .io = io };
    state.gate.set(io);
    var scheduling: scheduler.Scheduler = .{ .max_io_workers = 0 };
    var ahead = Self.init(.{ .ptr = &state, .next = State.next }, io);
    ahead.scheduling = &scheduling;
    defer ahead.deinit();
    ahead.prefetch(1000, 1);
    try std.testing.expect(ahead.task == null);
    try std.testing.expectEqual(@as(usize, 64), ahead.limit);
    try std.testing.expect((try ahead.next(64, 2)) != null);
}
