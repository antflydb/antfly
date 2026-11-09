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

const std = @import("std");
const builtin = @import("builtin");
pub const worker_env = "ANTFLY_INFERENCE_SUPERVISED_WORKER";
const lifeline_env = "ANTFLY_INFERENCE_SUPERVISOR_LIFELINE";
/// Reserved exit code by which a worker asks its supervisor for a fresh
/// generation. Ordinary startup/configuration failures must use other codes.
pub const restart_exit_code: u8 = 86;

pub fn restartWorker() noreturn {
    @import("process.zig").exitImmediately(restart_exit_code);
}

/// The server command reserves stdin as a supervisor-owned lifeline (it does
/// not accept request input on stdin). EOF means the owner has gone away,
/// including SIGKILL, so stop without attempting potentially stuck teardown.
/// Install before model startup and keep alive through all worker teardown.
/// A concurrent task guarantees progress even while the main task is in a
/// blocking native call; all pipe I/O and cancellation are portable std.Io.
pub const WorkerLifetime = struct {
    group: std.Io.Group = .init,

    pub fn deinit(self: *WorkerLifetime, io: std.Io) void {
        self.group.cancel(io);
    }

    fn start(self: *WorkerLifetime, io: std.Io) !void {
        try self.group.concurrent(io, watchLifeline, .{io});
    }

    fn watchLifeline(io: std.Io) std.Io.Cancelable!void {
        var byte: [1]u8 = undefined;
        const count = std.Io.File.stdin().readStreaming(io, &.{&byte}) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.EndOfStream => @import("process.zig").exitImmediately(0),
            else => @import("process.zig").exitImmediately(1),
        };
        // No data is legal on this private channel. EOF is an intentional
        // owner-loss shutdown, not a watchdog restart request.
        @import("process.zig").exitImmediately(if (count == 0) 0 else 1);
    }
};

fn isHelp(value: []const u8) bool {
    return std.mem.eql(u8, value, "--help") or std.mem.eql(u8, value, "-h") or
        std.mem.eql(u8, value, "help");
}

fn isRunInvocation(argv: []const []const u8, command_index: usize) bool {
    if (command_index >= argv.len) return true;
    const command = argv[command_index];
    if (isHelp(command)) return false;
    if (std.mem.eql(u8, command, "run")) return true;
    // Run options are accepted without an explicit `run` command.
    return std.mem.startsWith(u8, command, "-");
}

fn restartDelayMs(restarts: u32) u64 {
    const shift: u5 = @intCast(@min(restarts, 6));
    return @min(@as(u64, 100) << shift, 5_000);
}

fn signalShutdownSupported() bool {
    return switch (builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
        else => false,
    };
}

var signals_in_use: std.atomic.Value(bool) = .init(false);
var stop_requested: std.atomic.Value(bool) = .init(false);

fn requestStop(_: std.posix.SIG) callconv(.c) void {
    // A listener/native driver may be blocked or hold runtime locks. The
    // handler only publishes intent; a concurrent task closes the lifeline.
    stop_requested.store(true, .release);
}

const StopSignals = struct {
    old_int: if (signalShutdownSupported()) std.posix.Sigaction else void,
    old_term: if (signalShutdownSupported()) std.posix.Sigaction else void,

    fn install() !StopSignals {
        if (comptime !signalShutdownSupported()) return .{ .old_int = {}, .old_term = {} };
        if (signals_in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) != null)
            return error.InferenceSupervisorAlreadyActive;
        stop_requested.store(false, .release);
        const action = std.posix.Sigaction{ .handler = .{ .handler = requestStop }, .mask = std.posix.sigemptyset(), .flags = 0 };
        var self: StopSignals = undefined;
        std.posix.sigaction(.INT, &action, &self.old_int);
        std.posix.sigaction(.TERM, &action, &self.old_term);
        return self;
    }

    fn deinit(self: *StopSignals) void {
        if (comptime !signalShutdownSupported()) return;
        std.posix.sigaction(.INT, &self.old_int, null);
        std.posix.sigaction(.TERM, &self.old_term, null);
        signals_in_use.store(false, .release);
    }
};

fn shouldRestart(term: std.process.Child.Term, stopping: bool) !bool {
    return switch (term) {
        .exited => |code| switch (code) {
            0 => false,
            restart_exit_code => !stopping,
            else => error.InferenceWorkerExited,
        },
        .signal => |number| if (!stopping) true else stopped: {
            if (comptime signalShutdownSupported()) {
                // A process-group operator signal may reach the worker before
                // its lifeline sees EOF. Other crashes remain failures on stop.
                if (number == .INT or number == .TERM)
                    break :stopped false;
            }
            break :stopped error.InferenceWorkerExited;
        },
        .stopped, .unknown => if (stopping) error.InferenceWorkerExited else true,
    };
}

fn watchStop(io: std.Io, lifeline: std.Io.File) std.Io.Cancelable!void {
    // This task is the sole owner. In particular Child.wait/kill may not
    // retain a copy that could close a reused descriptor behind this task.
    defer lifeline.close(io);
    while (!stop_requested.load(.acquire))
        try io.sleep(.fromMilliseconds(20), .awake);
}

fn waitChild(io: std.Io, child: *std.process.Child) !std.process.Child.Term {
    const child_id = child.id;
    return child.wait(io) catch |err| {
        // POSIX wait may clear id before a canceled waitpid reaps the child.
        // Retain ownership for the parent's deferred kill/reap on failure.
        if (comptime builtin.os.tag != .windows) child.id = child_id;
        return err;
    };
}

fn runWorker(init: std.process.Init, argv: []const []const u8, environment: *const std.process.Environ.Map) !std.process.Child.Term {
    var child = try std.process.spawn(init.io, .{
        .argv = argv,
        .environ_map = environment,
        .stdin = .pipe,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    defer child.kill(init.io);
    if (comptime signalShutdownSupported()) {
        const lifeline = child.stdin.?;
        child.stdin = null;
        var watcher = init.io.concurrent(watchStop, .{ init.io, lifeline }) catch |err| {
            lifeline.close(init.io);
            return err;
        };
        // Join the sole pipe owner before child cleanup on every path.
        defer _ = watcher.cancel(init.io) catch {};
        return waitChild(init.io, &child);
    }
    return waitChild(init.io, &child);
}

fn restartDelay(io: std.Io, milliseconds: u64) !void {
    if (comptime !signalShutdownSupported())
        return io.sleep(.fromMilliseconds(@intCast(milliseconds)), .awake);
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromMilliseconds(@intCast(milliseconds)));
    while (!stop_requested.load(.acquire)) {
        const remaining = std.Io.Clock.awake.now(io).durationTo(deadline).nanoseconds;
        if (remaining <= 0) return;
        try io.sleep(.fromNanoseconds(@min(remaining, 20 * std.time.ns_per_ms)), .awake);
    }
}

/// Turns the long-lived inference server command into a stable supervisor plus
/// a replaceable worker. The child receives the exact original argv and
/// environment, with only private lifecycle markers added.
///
/// Returns true in the parent after a clean worker exit; false in the worker or
/// for non-server inference commands.
pub fn runIfNeeded(init: std.process.Init, command_index: usize, lifetime: *WorkerLifetime) !bool {
    if (init.environ_map.get(worker_env)) |marker| {
        if (!std.mem.eql(u8, marker, "1") or
            !std.mem.eql(u8, init.environ_map.get(lifeline_env) orelse "", "stdin-v1"))
            return error.InvalidInferenceWorkerEnvironment;
        try lifetime.start(init.io);
        return false;
    }

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    var argv = std.ArrayListUnmanaged([]const u8).empty;
    defer argv.deinit(init.gpa);
    while (args.next()) |arg| try argv.append(init.gpa, arg);
    if (!isRunInvocation(argv.items, command_index)) return false;

    // Only the parent installs handlers. The worker keeps its existing
    // owner-loss EOF behavior, and non-server commands retain their policy.
    var signals = try StopSignals.install();
    defer signals.deinit();

    var child_environment = try init.environ_map.clone(init.gpa);
    defer child_environment.deinit();
    try child_environment.put(worker_env, "1");
    try child_environment.put(lifeline_env, "stdin-v1");

    var restarts: u32 = 0;
    while (true) {
        if (stop_requested.load(.acquire)) return true;
        const term = try runWorker(init, argv.items, &child_environment);
        // Argument/configuration/startup errors use an ordinary exit code and
        // must be returned to the operator, not hidden in an infinite restart
        // loop. The watchdog uses the reserved restart code; native crashes
        // arrive as signals on POSIX.
        if (!try shouldRestart(term, stop_requested.load(.acquire))) return true;
        restarts +|= 1;
        const delay_ms = restartDelayMs(restarts - 1);
        std.log.err(
            "inference worker stopped unexpectedly; restarting generation={d} delay_ms={d} term={any}",
            .{ restarts + 1, delay_ms, term },
        );
        try restartDelay(init.io, delay_ms);
    }
}

test "only inference server invocations are supervised" {
    try std.testing.expect(isRunInvocation(&.{"antfly-inference"}, 1));
    try std.testing.expect(isRunInvocation(&.{ "antfly", "inference", "run" }, 2));
    try std.testing.expect(isRunInvocation(&.{ "antfly", "inference", "--port", "0" }, 2));
    try std.testing.expect(!isRunInvocation(&.{ "antfly", "inference", "embed" }, 2));
    try std.testing.expect(!isRunInvocation(&.{ "antfly-inference", "--help" }, 1));
}

test "restart backoff is bounded" {
    try std.testing.expectEqual(@as(u64, 100), restartDelayMs(0));
    try std.testing.expectEqual(@as(u64, 5_000), restartDelayMs(20));
}

test "worker restart code is distinct from ordinary process failure" {
    try std.testing.expect(restart_exit_code != 0);
    try std.testing.expect(restart_exit_code != 1);
    try std.testing.expect(try shouldRestart(.{ .exited = restart_exit_code }, false));
    try std.testing.expect(!(try shouldRestart(.{ .exited = 0 }, false)));
    try std.testing.expectError(error.InferenceWorkerExited, shouldRestart(.{ .exited = 1 }, false));
}

test "operator stop suppresses restart but preserves failures" {
    try std.testing.expect(!(try shouldRestart(.{ .exited = 0 }, true)));
    try std.testing.expect(!(try shouldRestart(.{ .exited = restart_exit_code }, true)));
    try std.testing.expectError(error.InferenceWorkerExited, shouldRestart(.{ .exited = 1 }, true));
    try std.testing.expectError(error.InferenceWorkerExited, shouldRestart(.{ .unknown = 0 }, true));
    if (comptime signalShutdownSupported()) {
        for ([_]std.posix.SIG{ .INT, .TERM }) |sig|
            try std.testing.expect(!(try shouldRestart(.{ .signal = sig }, true)));
        for ([_]std.posix.SIG{ .KILL, .SEGV }) |sig| {
            try std.testing.expect(try shouldRestart(.{ .signal = sig }, false));
            try std.testing.expectError(error.InferenceWorkerExited, shouldRestart(.{ .signal = sig }, true));
        }
    }
}

test "supervisor signal ownership restores previous handlers" {
    if (comptime !signalShutdownSupported()) return error.SkipZigTest;
    const ignored = std.posix.Sigaction{ .handler = .{ .handler = std.posix.SIG.IGN }, .mask = std.posix.sigemptyset(), .flags = 0 };
    var previous_int: std.posix.Sigaction = undefined;
    var previous_term: std.posix.Sigaction = undefined;
    std.posix.sigaction(.INT, &ignored, &previous_int);
    defer std.posix.sigaction(.INT, &previous_int, null);
    std.posix.sigaction(.TERM, &ignored, &previous_term);
    defer std.posix.sigaction(.TERM, &previous_term, null);
    {
        var signals = try StopSignals.install();
        defer signals.deinit();
        try std.testing.expectError(error.InferenceSupervisorAlreadyActive, StopSignals.install());
        requestStop(.TERM);
        try std.testing.expect(stop_requested.load(.acquire));
    }
    var actual: std.posix.Sigaction = undefined;
    std.posix.sigaction(.INT, null, &actual);
    try std.testing.expectEqual(std.posix.SIG.IGN, actual.handler.handler);
    std.posix.sigaction(.TERM, null, &actual);
    try std.testing.expectEqual(std.posix.SIG.IGN, actual.handler.handler);
    var signals = try StopSignals.install();
    defer signals.deinit();
    try std.testing.expect(!stop_requested.load(.acquire));
}
