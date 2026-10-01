// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Independent inference executable; database server roles have a separate entrypoint.
const std = @import("std");
const structlog = @import("structlog");
const dispatch = @import("runtime_dispatch.zig");
const supervisor = @import("antfly_platform").inference_process_supervisor;
const one_shot = @import("antfly_platform").one_shot_process;

extern fn antfly_runtime_inference(context: *const dispatch.Context) callconv(.c) c_int;

pub const std_options: std.Options = .{ .logFn = structlog.logFn };

pub fn main(init: std.process.Init) void {
    run(init) catch |err| {
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly inference: {s}\n", .{message});
        std.process.exit(1);
    };
}

fn run(init: std.process.Init) anyerror!void {
    structlog.init(.{ .formatter = .json, .level = .info });
    var worker_lifetime = supervisor.WorkerLifetime{};
    defer worker_lifetime.deinit(init.io);
    if (try supervisor.runIfNeeded(init, 1, &worker_lifetime)) return;

    if (one_shot.isTrainingInvocation(init.minimal.args)) {
        // The runtime bridge substitutes argv; preserve the original training
        // invocation before dispatch, as the independent inference package does.
        const original = try one_shot.encodeOriginalArguments(init.gpa, init.minimal.args);
        defer init.gpa.free(original);
        try init.environ_map.put(one_shot.original_argv_env, original);
    }
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    try dispatch.run(antfly_runtime_inference, "inference", init, &args);
}
