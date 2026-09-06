// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const supervisor = @import("supervisor");

fn run(init: std.process.Init) !void {
    var lifetime = supervisor.WorkerLifetime{};
    defer lifetime.deinit(init.io);
    const index = try std.fmt.parseUnsigned(usize, init.environ_map.get("FIXTURE_COMMAND_INDEX") orelse "1", 10);
    if (try supervisor.runIfNeeded(init, index, &lifetime)) return;
    // Process integration tests run on POSIX hosts. The production supervisor
    // itself uses only std.Io pipes, including on Windows.
    var line: [64]u8 = undefined;
    const pid = if (@import("builtin").os.tag == .windows) 0 else std.posix.system.getpid();
    const message = try std.fmt.bufPrint(&line, "worker {d}\n", .{pid});
    try std.Io.File.stdout().writeStreamingAll(init.io, message);
    const mode = init.environ_map.get("FIXTURE_MODE") orelse "blocked";
    if (std.mem.eql(u8, mode, "clean")) return;
    if (std.mem.eql(u8, mode, "fail")) return error.FixtureStartupFailure;
    if (std.mem.eql(u8, mode, "restart")) supervisor.restartWorker();
    // Simulate an uncooperative native constructor: no Io or cancellation
    // points on the main task. The lifeline must still terminate this worker.
    while (true) std.atomic.spinLoopHint();
}

pub fn main(init: std.process.Init) !void {
    if (init.environ_map.get("FIXTURE_CANCEL_PARENT") != null and
        init.environ_map.get(supervisor.worker_env) == null)
    {
        var task = try init.io.concurrent(run, .{init});
        try init.io.sleep(.fromMilliseconds(500), .awake);
        _ = task.cancel(init.io) catch {};
        return;
    }
    try run(init);
}
