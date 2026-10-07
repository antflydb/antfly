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

const std = @import("std");
const command = @import("cmd/lite.zig");
pub const antfly_sources = @import("source_owner_storage.zig");
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    const argv0 = args.next() orelse "antfly-lite";
    // Preserve the historical prefixed invocation without a server dispatcher.
    var prefixed_args = args;
    if (prefixed_args.next()) |name| {
        if (std.mem.eql(u8, name, "lite")) args = prefixed_args;
    }
    // The statically linked inference host re-executes its own image for
    // process isolation. This is an internal transport, not a Lite command.
    var worker_args = args;
    if (worker_args.next()) |command_name| {
        if (std.mem.eql(u8, command_name, "inference")) {
            if (worker_args.next()) |operation| {
                if (std.mem.eql(u8, operation, "_worker"))
                    return @import("antfly_inference_host").worker_module.runChild(init.gpa, init.io);
            }
        }
    }
    try command.runFromIterator(init, argv0, &args);
}

test "lite main compiles" {
    _ = main;
}
