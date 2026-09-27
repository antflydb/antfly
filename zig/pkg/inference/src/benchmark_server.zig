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
    var arguments: [64][]const u8 = undefined;
    var count: usize = 0;
    while (iterator.next()) |argument| {
        if (count == arguments.len) return error.TooManyArguments;
        arguments[count] = argument;
        count += 1;
    }
    if (std.mem.eql(u8, command, "run")) {
        try cli.runServer(allocator, init.io, arguments[0..count]);
    } else if (std.mem.eql(u8, command, "cuda-info")) {
        // Qualification needs the identity and capabilities of this exact
        // server binary, including when built on a memory-constrained host.
        try @import("inference").cuda_info.main(allocator, init.io, arguments[0..count]);
    } else return error.InvalidCommand;
}
