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

const std = @import("std");
const runtime = @import("../background_runtime.zig");
/// Supervision only. DB's run-one-pass port retains schema-version fencing and
/// durable building/failed/ready transitions. The durable owner lane must drain
/// before this owner or its borrowed operation context is destroyed.
pub const Owner = struct {
    state: std.atomic.Value(u8) = .init(0),
    stopping: std.atomic.Value(bool) = .init(false),
    bound: bool = false,
    port: Port = undefined,
    pub const Port = struct {
        ptr: *anyopaque,
        lane: runtime.DurableJobLane,
        owner_id: u64,
        stable_address: bool,
        closing: *const std.atomic.Value(bool),
        pass: *const fn (*anyopaque) void,
    };
    fn jobRun(ptr: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.run();
    }
    fn jobDeinit(_: *anyopaque) void {}
    pub fn schedule(self: *Owner, port: Port) void {
        if (self.stopping.load(.acquire) or port.closing.load(.acquire)) return;
        // Value-returning handles may move. Inline and movable handles retain
        // no borrowed context and complete reconciliation on the caller.
        if (!port.stable_address or port.lane.executesInline()) {
            port.pass(port.ptr);
            return;
        }
        while (true) {
            const state = self.state.load(.acquire);
            if (state == 2) return;
            if (state == 1) {
                if (self.state.cmpxchgWeak(1, 2, .acq_rel, .acquire) == null) return;
                continue;
            }
            if (self.state.cmpxchgWeak(0, 1, .acq_rel, .acquire) != null) continue;
            // Immutable across queued callbacks and successor flights.
            if (!self.bound) {
                self.port = port;
                self.bound = true;
            } else {
                std.debug.assert(self.port.ptr == port.ptr and self.port.owner_id == port.owner_id);
            }
            port.lane.submit(.{ .owner_id = port.owner_id, .class = .maintenance, .ptr = self, .run = jobRun, .deinit = jobDeinit }) catch |err| {
                std.log.warn("schema index reconciliation queue unavailable; using caller fallback err={s}", .{@errorName(err)});
                self.run();
            };
            return;
        }
    }
    pub fn stop(self: *Owner) void {
        self.stopping.store(true, .release);
    }
    fn run(self: *Owner) void {
        const port = self.port;
        while (true) {
            if (self.stopping.load(.acquire) or port.closing.load(.acquire)) {
                self.state.store(0, .release);
                return;
            }
            port.pass(port.ptr);
            if (self.state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return;
            self.state.store(1, .release);
        }
    }
};

const Fixture = struct {
    owner: Owner = .{},
    closing: std.atomic.Value(bool) = .init(false),
    queued: ?runtime.Job = null,
    refuse: bool = false,
    rerun: bool = true,
    inline_lane: bool = false,
    calls: usize = 0,
    submissions: usize = 0,
    fn submit(ptr: *anyopaque, job: runtime.Job) !void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.submissions += 1;
        if (self.refuse) return error.QueueRefused;
        self.queued = job;
    }
    fn noop(_: *anyopaque, _: u64) void {}
    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
    fn pass(ptr: *anyopaque) void {
        const self: *Fixture = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        if (self.rerun and self.calls == 1) self.owner.schedule(self.port());
    }
    fn port(self: *Fixture) Owner.Port {
        return .{ .ptr = self, .lane = .{ .ptr = self, .vtable = if (self.inline_lane) &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll, .executes_inline = true } else &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll } }, .owner_id = 1, .stable_address = true, .closing = &self.closing, .pass = pass };
    }
    fn drain(self: *Fixture) !void {
        const job = self.queued.?;
        self.queued = null;
        try job.run(job.ptr);
        job.deinit(job.ptr);
    }
};
test "schema reconciliation coalesces publication during a pass" {
    var f: Fixture = .{};
    f.owner.schedule(f.port());
    f.owner.schedule(f.port());
    try f.drain();
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.submissions);
    try std.testing.expectEqual(@as(u8, 0), f.owner.state.load(.acquire));
}
test "schema reconciliation falls back on rejected admission and never retains movable handles" {
    var f: Fixture = .{ .refuse = true };
    f.owner.schedule(f.port());
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(u8, 0), f.owner.state.load(.acquire));
    var movable: Fixture = .{ .rerun = false };
    var port = movable.port();
    port.stable_address = false;
    movable.owner.schedule(port);
    try std.testing.expectEqual(@as(usize, 1), movable.calls);
    try std.testing.expect(!movable.owner.bound);
    try std.testing.expectEqual(@as(usize, 0), movable.submissions);
}
test "schema reconciliation close rejects registration and cancels queued reruns" {
    var f: Fixture = .{};
    f.owner.schedule(f.port());
    f.owner.stop();
    try f.drain();
    f.owner.schedule(f.port());
    try std.testing.expectEqual(@as(usize, 0), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.submissions);
    try std.testing.expectEqual(@as(u8, 0), f.owner.state.load(.acquire));
}

test "schema reconciliation executes inline without retaining a callback" {
    var f: Fixture = .{ .inline_lane = true, .rerun = false };
    f.owner.schedule(f.port());
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    try std.testing.expect(!f.owner.bound);
    try std.testing.expectEqual(@as(usize, 0), f.submissions);
}
