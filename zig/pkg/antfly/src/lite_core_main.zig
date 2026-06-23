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
const structlog = @import("structlog");
const lite = @import("cmd/lite.zig");

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
        return try lite.runFromIterator(init, argv0, &args);
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
    std.debug.print("antfly lite-core\n", .{});
}

test "lite core main compiles" {
    _ = main;
}
