// Copyright 2026 Antfly, Inc.
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
const structlog = @import("structlog");
const dispatch = @import("runtime_dispatch.zig");

extern fn antfly_runtime_lite(context: *const dispatch.Context) callconv(.c) c_int;
extern fn antfly_runtime_inference(context: *const dispatch.Context) callconv(.c) c_int;
const inference_process_supervisor = @import("antfly_platform").inference_process_supervisor;

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

pub fn main(init: std.process.Init) !void {
    structlog.init(.{ .formatter = .json, .level = .info });

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return;
    };

    if (std.mem.eql(u8, subcommand, "--help") or std.mem.eql(u8, subcommand, "-h") or std.mem.eql(u8, subcommand, "help")) {
        printUsage(argv0);
        return;
    }
    if (std.mem.eql(u8, subcommand, "--version") or std.mem.eql(u8, subcommand, "version")) {
        printVersion();
        return;
    }

    if (std.mem.eql(u8, subcommand, "lite")) {
        return try dispatch.run(antfly_runtime_lite, subcommand, init, &args);
    }

    // Embedded hosts can re-execute this binary as their inference worker.
    if (std.mem.eql(u8, subcommand, "inference")) {
        const worker_command = args.next() orelse return error.InvalidArguments;
        if (!std.mem.eql(u8, worker_command, "_worker")) return error.InvalidArguments;
        var worker_lifetime = inference_process_supervisor.WorkerLifetime{};
        defer worker_lifetime.deinit(init.io);
        if (try inference_process_supervisor.runIfNeeded(init, 2, &worker_lifetime)) return;
        var worker_args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
        defer worker_args.deinit();
        _ = worker_args.next();
        _ = worker_args.next();
        return try dispatch.run(antfly_runtime_inference, subcommand, init, &worker_args);
    }

    std.debug.print("unknown subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} <subcommand> [options]
        \\
        \\subcommands:
        \\  lite      Run Antfly Lite embedded database commands
        \\  version   Print version information
        \\
        \\Run `{s} lite help` for Lite commands.
        \\
    , .{ argv0, argv0 });
}

fn printVersion() void {
    std.debug.print("antfly lite\n", .{});
}

test "lite main compiles" {
    _ = main;
}
