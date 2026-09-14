// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Focused build of the production resident HTTP server. Avoids compiling the
//! unrelated CLI commands when iterating on inference on a constrained host.
const std = @import("std");
const platform = @import("antfly_platform");
const cli = @import("main.zig");

pub const std_options = cli.std_options;

pub fn main(init: std.process.Init) !void {
    var worker_lifetime = platform.inference_process_supervisor.WorkerLifetime{};
    defer worker_lifetime.deinit(init.io);
    if (try platform.inference_process_supervisor.runIfNeeded(init, 1, &worker_lifetime)) return;
    const allocator = platform.allocator.processAllocator(std.heap.smp_allocator);
    var iterator = std.process.Args.Iterator.init(init.minimal.args);
    _ = iterator.next();
    const command = iterator.next() orelse return error.MissingCommand;
    if (!std.mem.eql(u8, command, "run")) return error.InvalidCommand;
    var arguments: [64][]const u8 = undefined;
    var count: usize = 0;
    while (iterator.next()) |argument| {
        if (count == arguments.len) return error.TooManyArguments;
        arguments[count] = argument;
        count += 1;
    }
    try cli.runServer(allocator, init.io, arguments[0..count]);
}
