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
const command = @import("cmd/lite.zig");
pub const antfly_sources = @import("source_owner_storage.zig");
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    const argv0 = args.next() orelse "antfly-lite";
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
