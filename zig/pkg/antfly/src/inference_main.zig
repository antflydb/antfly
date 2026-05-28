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
const inference_cli = @import("inference_cli");
const platform = @import("antfly_platform");

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

pub fn main(init: std.process.Init) !void {
    structlog.init(.{ .formatter = .json, .level = .info });

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_buf: [64][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_iter.next()) |arg| {
        if (args_len < args_buf.len) {
            args_buf[args_len] = arg;
            args_len += 1;
        }
    }
    const args = args_buf[0..args_len];

    if (args.len < 2 or std.mem.eql(u8, args[1], "--help") or std.mem.eql(u8, args[1], "-h") or std.mem.eql(u8, args[1], "help")) {
        printUsage();
        return;
    }

    if (std.mem.eql(u8, args[1], "--version") or std.mem.eql(u8, args[1], "version")) {
        inference_cli.printVersion();
        return;
    }

    if (!std.mem.eql(u8, args[1], "inference")) {
        std.debug.print("unknown subcommand: {s}\n", .{args[1]});
        printUsage();
        return error.InvalidArguments;
    }

    return inference_cli.runFromArgs(init, platform.allocator.processAllocator(std.heap.smp_allocator), "antfly inference", args[2..]);
}

fn printUsage() void {
    std.debug.print(
        \\usage: antfly <subcommand> [options]
        \\
        \\subcommands:
        \\  inference    Run inference server and model commands
        \\  version      Print version information
        \\
        \\Run `antfly inference help` for inference commands.
        \\
    , .{});
}

test "inference main compiles" {
    _ = main;
}
