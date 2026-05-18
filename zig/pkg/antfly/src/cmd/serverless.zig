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
const api = @import("serverless_api.zig");
const query = @import("serverless_query.zig");
const maintenance = @import("serverless_maintenance.zig");
const swarm = @import("serverless_swarm.zig");

pub fn run(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly";
    if (args.next()) |subcommand| {
        return try dispatch(init, argv0, subcommand, &args);
    }
    printUsage(argv0);
}

pub fn runFromIterator(init: std.process.Init, argv0: []const u8, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return;
    };
    return try dispatch(init, argv0, subcommand, args);
}

fn dispatch(init: std.process.Init, argv0: []const u8, subcommand: []const u8, args: *std.process.Args.Iterator) !void {
    if (std.mem.eql(u8, subcommand, "--help") or std.mem.eql(u8, subcommand, "-h") or std.mem.eql(u8, subcommand, "help")) {
        printUsage(argv0);
        return;
    }

    var nested_argv0_buf: [128]u8 = undefined;
    const nested_argv0 = try std.fmt.bufPrint(&nested_argv0_buf, "{s} serverless {s}", .{ argv0, subcommand });

    if (std.mem.eql(u8, subcommand, "api")) return try api.runFromIterator(init, nested_argv0, args);
    if (std.mem.eql(u8, subcommand, "query")) return try query.runFromIterator(init, nested_argv0, args);
    if (std.mem.eql(u8, subcommand, "maintenance")) return try maintenance.runFromIterator(init, nested_argv0, args);
    if (std.mem.eql(u8, subcommand, "swarm")) return try swarm.runFromIterator(init, nested_argv0, args);

    std.debug.print("unknown serverless subcommand: {s}\n", .{subcommand});
    printUsage(argv0);
    return error.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} serverless <subcommand> [options]
        \\
        \\serverless subcommands:
        \\  api
        \\  query
        \\  maintenance
        \\  swarm
        \\
    , .{argv0});
}

test "serverless cmd compiles" {
    _ = run;
    _ = runFromIterator;
}
