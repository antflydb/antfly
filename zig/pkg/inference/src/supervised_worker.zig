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

    fn signalWithoutWaiting(id: std.process.Child.Id, force: bool) void {
        // Child.kill() also waits and cleans up the Child. The wait task below
        // owns that operation, so use the platform's signal primitive here to
        // avoid two concurrent waiters mutating the same Child.
        switch (builtin.os.tag) {
            .windows => {
                // Windows has no POSIX-style cooperative process signal. The
                // child protocol is expected to carry cancellation; once the
                // protocol grace expires this is the hard-stop fallback.
                if (!force) return;
                _ = std.os.windows.ntdll.NtTerminateProcess(id, @enumFromInt(1));
            },
            .wasi, .freestanding => unreachable,
            else => std.posix.kill(id, if (force) .KILL else .TERM) catch {},
        }
    }

    fn stopAndReap(
        self: *Supervisor,
        io: std.Io,
        child_id: std.process.Child.Id,
        state: *WaitState,
        group: *std.Io.Group,
    ) void {
        // The worker's IPC loop treats TERM as cooperative cancellation. Give
        // it a bounded interval to unwind its own resources, then force the
        // process down. Only WaitState.wait reaps and mutates Child.
        signalWithoutWaiting(child_id, false);
        if (!state.done.isSet()) {
            state.done.waitTimeout(io, .{
                .duration = .{
                    .raw = std.Io.Duration.fromNanoseconds(self.cancellation_grace_ns),
                    .clock = .awake,
                },
            }) catch signalWithoutWaiting(child_id, true);
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
        const child_id = child.id.?;
        var state = WaitState{ .child = &child, .io = io };
        var group = std.Io.Group.init;
        group.async(io, WaitState.wait, .{&state});

        while (!state.done.isSet()) {
            control.check() catch |err| {
                // A process is the ownership boundary: killing it cannot race
                // model buffers retained by the parent. Wait for reaping before
                // advertising the replacement generation.
                self.stopAndReap(io, child_id, &state, &group);
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
                    self.stopAndReap(io, child_id, &state, &group);
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
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding)
        return error.SkipZigTest;
    var supervisor = Supervisor{ .cancellation_grace_ns = 10 * std.time.ns_per_ms };
    const control = InferenceExecutionControl{
        .deadline_ns = @import("antfly_platform").time.monotonicNs() + 10 * std.time.ns_per_ms,
    };
    try std.testing.expectError(
        error.Timeout,
        supervisor.run(std.testing.io, &.{ "/bin/sh", "-c", "exec sleep 60" }, control),
    );
    try std.testing.expect(supervisor.unhealthy);
    try std.testing.expectEqual(@as(u64, 1), supervisor.restart_count);
    try std.testing.expectEqual(@as(u64, 2), supervisor.generation);
}
