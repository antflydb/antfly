// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const InferenceExecutionControl = @import("execution_control.zig").InferenceExecutionControl;

/// Process isolation boundary for backends whose driver/kernel calls cannot be
/// interrupted cooperatively. A canceled worker is killed only after the
/// caller-owned request has stopped sharing its address space with the model.
pub const Supervisor = struct {
    generation: u64 = 1,
    restart_count: u64 = 0,
    unhealthy: bool = false,
    cancellation_grace_ns: u64 = 250 * std.time.ns_per_ms,

    pub const Result = struct {
        term: std.process.Child.Term,
        generation: u64,
    };

    const WaitState = struct {
        child: *std.process.Child,
        io: std.Io,
        done: std.Io.Event = .unset,
        term: ?std.process.Child.Term = null,
        failed: bool = false,

        fn wait(self: *@This()) std.Io.Cancelable!void {
            self.term = self.child.wait(self.io) catch {
                self.failed = true;
                self.done.set(self.io);
                return;
            };
            self.done.set(self.io);
        }
    };

    /// Linux pidfds bind signals to a process identity rather than a reusable
    /// numeric PID. Other platforms stay fail-closed until they have an
    /// equivalent stable process handle implementation.
    const StableProcess = struct {
        fd: if (builtin.os.tag == .linux) std.posix.fd_t else void,

        fn open(id: std.process.Child.Id) !@This() {
            if (comptime builtin.os.tag != .linux) return error.StableProcessHandleUnsupported;
            const rc = std.os.linux.syscall2(.pidfd_open, @intCast(id), 0);
            return switch (std.posix.errno(rc)) {
                .SUCCESS => .{ .fd = @intCast(rc) },
                .NOSYS => error.StableProcessHandleUnsupported,
                else => error.StableProcessHandleUnavailable,
            };
        }

        fn close(self: *@This()) void {
            if (comptime builtin.os.tag == .linux) _ = std.posix.system.close(self.fd);
        }

        fn signal(self: *@This(), force: bool) void {
            if (comptime builtin.os.tag == .linux) {
                const signal_number: usize = @intFromEnum(if (force) std.posix.SIG.KILL else std.posix.SIG.TERM);
                _ = std.os.linux.syscall4(
                    .pidfd_send_signal,
                    @as(usize, @bitCast(@as(isize, self.fd))),
                    signal_number,
                    0,
                    0,
                );
            }
        }
    };

    fn stopAndReap(
        self: *Supervisor,
        io: std.Io,
        process: *StableProcess,
        state: *WaitState,
        group: *std.Io.Group,
    ) void {
        // The worker's IPC loop treats TERM as cooperative cancellation. Give
        // it a bounded interval to unwind its own resources, then force the
        // process down. Only WaitState.wait reaps and mutates Child.
        process.signal(false);
        if (!state.done.isSet()) {
            state.done.waitTimeout(io, .{
                .duration = .{
                    .raw = std.Io.Duration.fromNanoseconds(self.cancellation_grace_ns),
                    .clock = .awake,
                },
            }) catch process.signal(true);
        }
        if (!state.done.isSet()) state.done.waitUncancelable(io);
        group.await(io) catch {};
    }

    pub fn run(
        self: *Supervisor,
        io: std.Io,
        argv: []const []const u8,
        control: InferenceExecutionControl,
    ) !Result {
        try control.check();
        var child = try std.process.spawn(io, .{
            .argv = argv,
            .stdin = .ignore,
            .stdout = .ignore,
            .stderr = .inherit,
        });
        errdefer child.kill(io);
        var process = try StableProcess.open(child.id.?);
        defer process.close();
        var state = WaitState{ .child = &child, .io = io };
        var group = std.Io.Group.init;
        // A supervisor cannot use `Group.async`: that primitive may execute
        // eagerly when its pool is saturated, which would block here in
        // child.wait before the deadline loop gets a chance to terminate the
        // worker. `concurrent` either schedules independently or fails closed.
        try group.concurrent(io, WaitState.wait, .{&state});

        while (!state.done.isSet()) {
            control.check() catch |err| {
                // A process is the ownership boundary: killing it cannot race
                // model buffers retained by the parent. Wait for reaping before
                // advertising the replacement generation.
                self.stopAndReap(io, &process, &state, &group);
                self.unhealthy = true;
                self.restart_count +|= 1;
                self.generation +|= 1;
                return err;
            };
            state.done.waitTimeout(io, .{
                .duration = .{
                    .raw = std.Io.Duration.fromMilliseconds(10),
                    .clock = .awake,
                },
            }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => {
                    self.stopAndReap(io, &process, &state, &group);
                    self.unhealthy = true;
                    self.restart_count +|= 1;
                    self.generation +|= 1;
                    return error.Cancelled;
                },
            };
        }
        try group.await(io);
        if (state.failed) return error.WorkerWaitFailed;
        self.unhealthy = false;
        return .{ .term = state.term.?, .generation = self.generation };
    }
};

test "wedged worker is killed and the supervisor advances generation" {
    if (builtin.os.tag != .linux)
        return error.SkipZigTest;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var supervisor = Supervisor{ .cancellation_grace_ns = 10 * std.time.ns_per_ms };
    const control = InferenceExecutionControl{
        .deadline_ns = @import("antfly_platform").time.monotonicNs() + 10 * std.time.ns_per_ms,
    };
    try std.testing.expectError(
        error.Timeout,
        supervisor.run(io_impl.io(), &.{ "/bin/sh", "-c", "exec sleep 60" }, control),
    );
    try std.testing.expect(supervisor.unhealthy);
    try std.testing.expectEqual(@as(u64, 1), supervisor.restart_count);
    try std.testing.expectEqual(@as(u64, 2), supervisor.generation);
}
