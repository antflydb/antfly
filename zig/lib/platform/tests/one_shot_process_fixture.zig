// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const supervisor = @import("platform").one_shot_process;
const inference_supervisor = @import("platform").inference_process_supervisor;
extern "c" fn atexit(handler: *const fn () callconv(.c) void) c_int;

fn blockedAtExit() callconv(.c) void {
    while (true) std.atomic.spinLoopHint();
}

fn fixtureSafetyLimit() void {
    // The intentionally pre-monitor stall has no production lifeline reader.
    // Bound this negative test even if the test parent itself is interrupted.
    var delay = std.posix.timespec{ .sec = 6, .nsec = 0 };
    while (std.posix.errno(std.posix.system.nanosleep(&delay, &delay)) == .INTR) {}
    @import("platform").process.exitImmediately(255);
}

fn parent(init: std.process.Init) !supervisor.Outcome {
    defer if (init.environ_map.get("FIXTURE_PARENT_ATEXIT") != null) {
        std.Io.File.stdout().writeStreamingAll(init.io, "parent-cleanup\n") catch {};
    };
    var arguments = std.ArrayListUnmanaged([]const u8).empty;
    defer arguments.deinit(init.gpa);
    var iterator = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer iterator.deinit();
    while (iterator.next()) |argument| try arguments.append(init.gpa, argument);
    const timeout_ms = try std.fmt.parseUnsigned(u64, init.environ_map.get("FIXTURE_TIMEOUT_MS") orelse "1000", 10);
    const grace_ms = try std.fmt.parseUnsigned(u64, init.environ_map.get("FIXTURE_GRACE_MS") orelse "200", 10);
    return try supervisor.runParent(init, arguments.items, .{ .timeout_ns = timeout_ms * std.time.ns_per_ms, .shutdown_grace_ns = grace_ms * std.time.ns_per_ms, .fingerprint = .{7} ** 32 });
}

fn announce(io: std.Io) !void {
    var bytes: [64]u8 = undefined;
    const line = try std.fmt.bufPrint(&bytes, "worker {d}\n", .{std.posix.system.getpid()});
    try std.Io.File.stdout().writeStreamingAll(io, line);
}

fn child(init: std.process.Init) !void {
    const mode = init.environ_map.get("FIXTURE_MODE") orelse "blocked";
    if (std.mem.endsWith(u8, mode, "_atexit")) {
        if (comptime @import("builtin").link_libc) {
            if (atexit(blockedAtExit) != 0) return error.FixtureAtExitFailed;
        }
    }
    if (std.mem.eql(u8, mode, "before_monitor")) {
        const ignored = std.posix.Sigaction{ .handler = .{ .handler = std.posix.SIG.IGN }, .mask = std.posix.sigemptyset(), .flags = 0 };
        std.posix.sigaction(.TERM, &ignored, null);
        (try std.Thread.spawn(.{}, fixtureSafetyLimit, .{})).detach();
        try announce(init.io);
        while (true) std.atomic.spinLoopHint();
    }
    const worker = try supervisor.Worker.create(init.gpa, init.io, init.environ_map);
    // Process exit keeps the independent monitor alive through all runtime
    // teardown. Pure allocation tests exercise explicit destroy separately.
    worker.verifyFingerprint(if (std.mem.eql(u8, mode, "wrong_fingerprint")) .{8} ** 32 else .{7} ** 32) catch worker.finish(9);
    try announce(init.io);
    if (std.mem.eql(u8, mode, "clean") or std.mem.eql(u8, mode, "clean_atexit")) worker.finish(0);
    if (std.mem.eql(u8, mode, "fail")) worker.finish(7);
    if (std.mem.eql(u8, mode, "fatal")) worker.finish(supervisor.fatal_exit_code);
    if (std.mem.eql(u8, mode, "restart_atexit")) inference_supervisor.restartWorker();
    if (std.mem.eql(u8, mode, "cooperative_timeout")) {
        const timeout_ms = try std.fmt.parseUnsigned(u64, init.environ_map.get("FIXTURE_TIMEOUT_MS").?, 10);
        try init.io.sleep(.fromMilliseconds(@intCast(timeout_ms)), .awake);
        worker.finish(supervisor.timeout_exit_code);
    }
    if (std.mem.eql(u8, mode, "pause")) {
        while (!supervisor.Worker.pauseRequested(worker)) std.atomic.spinLoopHint();
        try std.Io.File.stdout().writeStreamingAll(init.io, "paused\n");
        worker.finish(0);
    }
    if (std.mem.eql(u8, mode, "teardown")) {
        try std.Io.File.stdout().writeStreamingAll(init.io, "teardown\n");
    }
    while (true) std.atomic.spinLoopHint();
}

pub fn main(init: std.process.Init) !void {
    if (try supervisor.workerRequested(init.environ_map)) return child(init);
    if (init.environ_map.get("FIXTURE_PARENT_ATEXIT") != null) {
        // Only the parent installs this handler. Child-only exit coverage
        // cannot prove that the complete invocation terminates after reap.
        if (comptime @import("builtin").link_libc) {
            if (atexit(blockedAtExit) != 0) return error.FixtureAtExitFailed;
        }
    }
    if (init.environ_map.get("FIXTURE_CANCEL_PARENT") != null) {
        var future = try init.io.concurrent(parent, .{init});
        try init.io.sleep(.fromMilliseconds(200), .awake);
        const outcome = future.cancel(init.io) catch |err| {
            if (init.environ_map.get("FIXTURE_PARENT_ATEXIT") != null) {
                std.debug.print("fixture supervisor failed: {s}\n", .{@errorName(err)});
                supervisor.finishParent(.{ .term = .{ .exited = 1 } });
            }
            return;
        };
        if (init.environ_map.get("FIXTURE_PARENT_ATEXIT") != null) supervisor.finishParent(outcome);
        return;
    }
    supervisor.finishParent(try parent(init));
}
