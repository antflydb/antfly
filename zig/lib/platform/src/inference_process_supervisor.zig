// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const env = @import("env.zig");
const process = @import("process.zig");

pub const worker_env = "ANTFLY_INFERENCE_SUPERVISED_WORKER";
pub const parent_pid_env = "ANTFLY_INFERENCE_SUPERVISOR_PID";
/// Reserved exit code by which a worker asks its supervisor for a fresh
/// generation. Ordinary startup/configuration failures must use other codes.
pub const restart_exit_code: u8 = 86;

pub fn restartWorker() noreturn {
    std.process.exit(restart_exit_code);
}

pub fn supervisedWorkerParentAlive() bool {
    if (!env.getenvBool(worker_env)) return true;
    const raw = env.getenv(parent_pid_env) orelse return true;
    const expected = std.fmt.parseUnsigned(u32, raw, 10) catch return false;
    const actual = process.parentId() orelse return true;
    return actual == expected;
}

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

fn shouldRestart(term: std.process.Child.Term) !bool {
    return switch (term) {
        .exited => |code| switch (code) {
            0 => false,
            restart_exit_code => true,
            else => error.InferenceWorkerExited,
        },
        .signal, .stopped, .unknown => true,
    };
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
    if (process.currentId()) |pid| {
        const parent_pid = try std.fmt.bufPrint(&parent_pid_buf, "{d}", .{pid});
        try child_environment.put(parent_pid_env, parent_pid);
    }

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
        // Argument/configuration/startup errors use an ordinary exit code and
        // must be returned to the operator, not hidden in an infinite restart
        // loop. The watchdog uses the reserved restart code; native crashes
        // arrive as signals on POSIX.
        if (!try shouldRestart(term)) return true;
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

test "worker restart code is distinct from ordinary process failure" {
    try std.testing.expect(restart_exit_code != 0);
    try std.testing.expect(restart_exit_code != 1);
    try std.testing.expect(try shouldRestart(.{ .exited = restart_exit_code }));
    try std.testing.expect(!(try shouldRestart(.{ .exited = 0 })));
    try std.testing.expectError(error.InferenceWorkerExited, shouldRestart(.{ .exited = 1 }));
}
