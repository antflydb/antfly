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
pub const antfly_sources = @import("source_owner_common.zig");
pub fn main(init: std.process.Init) void {
    run(init) catch |err| {
        if (err != error.CliDiagnosticReported) std.debug.print("error: {s}\n", .{@errorName(err)});
        std.process.exit(1);
    };
}
fn run(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    var invocation = try @import("cli_invocation.zig").parse(init.gpa, &args);
    defer invocation.deinit(init.gpa);
    args = std.process.Args.Iterator.init(.{ .vector = invocation.arguments.items });
    try @import("client_runtime.zig").runFromIterator(init, invocation.command orelse "", &args);
}

// Remote-client fixture: local backup operations require the storage runtime
// and fail explicitly here; their own suites exercise the real storage owner.
export fn antfly_restore_staging_create(_: *const anyopaque) callconv(.c) c_int {
    return 3;
}
export fn antfly_restore_staging_destroy(_: *anyopaque) callconv(.c) void {}
export fn antfly_backup_preflight(_: *const anyopaque) callconv(.c) c_int {
    return 3;
}
export fn antfly_backup_preflight_free(_: [*]u8, _: usize) callconv(.c) void {}
