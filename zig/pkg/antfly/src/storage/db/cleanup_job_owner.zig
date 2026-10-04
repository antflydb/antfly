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
const admission = @import("coalesced_job_admission.zig");
const builtin = @import("builtin");
const runtime = @import("../background_runtime.zig");

pub const Advance = enum { idle, progressed, busy };
/// Bounded cleanup supervision over borrowed storage operations. The admission
/// atomic is owned by IndexManager; its durable lane must drain before either
/// the manager or operation context is destroyed. Storage cursors and markers
/// remain authoritative after a job yields or admission fails.
pub const Port = struct {
    ptr: *anyopaque,
    state: *std.atomic.Value(u8),
    lane: runtime.DurableJobLane,
    owner_id: u64,
    io: ?std.Io,
    closing: ?*const std.atomic.Value(bool) = null,
    advance: *const fn (*anyopaque) anyerror!Advance,
    name: []const u8,
};
const Work = struct {
    port: Port,
    fn run(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try drain(self.port);
    }
    fn deinit(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        std.heap.page_allocator.destroy(self);
    }
};
pub fn schedule(port: Port) void {
    if (port.closing) |closing| if (closing.load(.acquire)) return;
    if (!admission.request(port.state)) return;
    const work = std.heap.page_allocator.create(Work) catch {
        port.state.store(0, .release);
        return;
    };
    work.* = .{ .port = port };
    port.lane.submit(.{ .owner_id = port.owner_id, .class = .cleanup, .ptr = work, .run = Work.run, .deinit = Work.deinit }) catch |err| {
        std.heap.page_allocator.destroy(work);
        port.state.store(0, .release);
        std.log.warn("{s} cleanup was not scheduled err={s}", .{ port.name, @errorName(err) });
    };
}
fn pause(io: std.Io, attempt: usize) void {
    const shift: u6 = @intCast(@min(attempt - 1, 5));
    const delay_ms: i64 = if (builtin.is_test) 1 else @min(@as(i64, 25) << shift, 1000);
    io.sleep(.fromMilliseconds(delay_ms), .awake) catch {};
}
fn yield(port: Port) void {
    port.state.store(0, .release);
    // Inline lanes have no queue to yield to: explicit polling or reopening
    // rediscover the durable marker. Never submit recursively on this stack.
    if (!port.lane.executesInline()) schedule(port);
}
pub fn drain(port: Port) !void {
    var pages: usize = 0;
    var retries: usize = 0;
    var contention_retries: usize = 0;
    while (true) {
        if (port.closing) |closing| if (closing.load(.acquire)) {
            port.state.store(0, .release);
            return;
        };
        const advance = port.advance(port.ptr) catch |err| {
            retries += 1;
            if (retries == 1 or std.math.isPowerOfTwo(retries)) std.log.warn("{s} cleanup retry attempt={} err={s}", .{ port.name, retries, @errorName(err) });
            if (port.io) |io| pause(io, retries) else {
                port.state.store(0, .release);
                return err;
            }
            if (retries >= 8) {
                yield(port);
                if (port.lane.executesInline()) return err;
                return;
            }
            continue;
        };
        retries = 0;
        if (advance == .busy) {
            const io = port.io orelse {
                port.state.store(0, .release);
                return;
            };
            contention_retries += 1;
            if (contention_retries < 8) {
                pause(io, contention_retries);
                continue;
            }
            yield(port);
            return;
        }
        contention_retries = 0;
        if (advance == .idle) {
            if (admission.settled(port.state)) return;
            pages = 0;
            continue;
        }
        pages += 1;
        if (pages < 8) continue;
        yield(port);
        return;
    }
}

test "cleanup job owner bounds inline error contention and progress slices" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(0),
        calls: usize = 0,
        depth: usize = 0,
        max_depth: usize = 0,
        outcome: ?Advance = null,
        fn submit(ptr: *anyopaque, job: runtime.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.depth += 1;
            defer self.depth -= 1;
            self.max_depth = @max(self.max_depth, self.depth);
            try job.run(job.ptr);
            job.deinit(job.ptr);
        }
        fn noop(_: *anyopaque, _: u64) void {}
        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return self.outcome orelse error.InjectedCleanupFailure;
        }
        fn port(self: *@This()) Port {
            return .{ .ptr = self, .state = &self.state, .lane = .{ .ptr = self, .vtable = &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll, .executes_inline = true } }, .owner_id = 1, .io = std.testing.io, .advance = advance, .name = "inline-test" };
        }
    };
    var f: F = .{};
    for ([_]?Advance{ null, .busy, .progressed }) |outcome| {
        f.outcome = outcome;
        const before = f.calls;
        schedule(f.port());
        try std.testing.expectEqual(@as(usize, 8), f.calls - before);
        try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    }
    f.outcome = .idle;
    schedule(f.port());
    try std.testing.expectEqual(@as(usize, 25), f.calls);
    try std.testing.expectEqual(@as(usize, 1), f.max_depth);
}

test "cleanup job owner coalesces a raced idle notification and respects closing" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(1),
        calls: usize = 0,
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.calls == 1) self.state.store(2, .release);
            return .idle;
        }
    };
    var f: F = .{};
    var closing: std.atomic.Value(bool) = .init(false);
    const port: Port = .{ .ptr = &f, .state = &f.state, .lane = undefined, .owner_id = 1, .io = null, .closing = &closing, .advance = F.advance, .name = "test" };
    try drain(port);
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    closing.store(true, .release);
    schedule(port);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    f.state.store(1, .release);
    try drain(port);
    try std.testing.expectEqual(@as(usize, 2), f.calls);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
}

test "cleanup job owner yields queued slices and releases rejected admission" {
    const F = struct {
        state: std.atomic.Value(u8) = .init(0),
        queued: ?runtime.Job = null,
        calls: usize = 0,
        rejecting: bool = false,
        fn submit(ptr: *anyopaque, job: runtime.Job) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.rejecting) return error.BackgroundOwnerClosed;
            std.debug.assert(self.queued == null);
            self.queued = job;
        }
        fn noop(_: *anyopaque, _: u64) void {}
        fn poll(_: *anyopaque, _: usize) !usize {
            return 0;
        }
        fn advance(ptr: *anyopaque) !Advance {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return if (self.calls <= 8) .progressed else .idle;
        }
        fn port(self: *@This()) Port {
            return .{ .ptr = self, .state = &self.state, .lane = .{ .ptr = self, .vtable = &.{ .submit = submit, .drain_owner = noop, .close_owner = noop, .poll = poll } }, .owner_id = 1, .io = null, .advance = advance, .name = "queued-test" };
        }
        fn next(self: *@This()) !void {
            const job = self.queued.?;
            self.queued = null;
            defer job.deinit(job.ptr);
            try job.run(job.ptr);
        }
    };
    var f: F = .{};
    schedule(f.port());
    try f.next();
    try std.testing.expectEqual(@as(usize, 8), f.calls);
    try std.testing.expect(f.queued != null);
    try f.next();
    try std.testing.expect(f.queued == null);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
    f.rejecting = true;
    schedule(f.port());
    try std.testing.expect(f.queued == null);
    try std.testing.expectEqual(@as(u8, 0), f.state.load(.acquire));
}
