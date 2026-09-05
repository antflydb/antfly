// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license

const std = @import("std");

pub const worker_env = "ANTFLY_INFERENCE_SUPERVISED_WORKER";
pub const parent_pid_env = "ANTFLY_INFERENCE_SUPERVISOR_PID";

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

/// Turns the long-lived inference server command into a stable supervisor plus
/// a replaceable worker. The child receives the exact original argv and
/// environment, with only private lifecycle markers added.
///
/// Returns true in the parent after a clean worker exit; false in the worker or
/// for non-server inference commands.
pub fn runIfNeeded(init: std.process.Init, command_index: usize) !bool {
    if (init.environ_map.get(worker_env) != null) return false;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    var argv = std.ArrayListUnmanaged([]const u8).empty;
    defer argv.deinit(init.gpa);
    while (args.next()) |arg| try argv.append(init.gpa, arg);
    if (!isRunInvocation(argv.items, command_index)) return false;

    var child_environment = try init.environ_map.clone(init.gpa);
    defer child_environment.deinit();
    try child_environment.put(worker_env, "1");
    var parent_pid_buf: [32]u8 = undefined;
    const parent_pid = try std.fmt.bufPrint(&parent_pid_buf, "{d}", .{std.posix.system.getpid()});
    try child_environment.put(parent_pid_env, parent_pid);

    var restarts: u32 = 0;
    while (true) {
        var child = try std.process.spawn(init.io, .{
            .argv = argv.items,
            .environ_map = &child_environment,
            .stdin = .inherit,
            .stdout = .inherit,
            .stderr = .inherit,
        });
        const term = try child.wait(init.io);
        switch (term) {
            .exited => |code| {
                if (code == 0) return true;
                // Argument/configuration/startup errors use an ordinary exit
                // code and must be returned to the operator, not hidden in an
                // infinite restart loop. Hard watchdog and native crashes are
                // signals and get a fresh worker generation below.
                return error.InferenceWorkerExited;
            },
            .signal, .stopped, .unknown => {},
        }
        restarts +|= 1;
        const delay_ms = restartDelayMs(restarts - 1);
        std.log.err(
            "inference worker stopped unexpectedly; restarting generation={d} delay_ms={d} term={any}",
            .{ restarts + 1, delay_ms, term },
        );
        try init.io.sleep(std.Io.Duration.fromMilliseconds(delay_ms), .awake);
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
