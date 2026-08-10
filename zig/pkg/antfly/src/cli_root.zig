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

//! Focused facade for CLI code. Declarations referenced only by tests remain
//! lazy in production, keeping the remote command unit out of storage codegen.

const std = @import("std");
const platform = @import("antfly_platform");
const structlog = @import("structlog");
const bridge = @import("runtime_bridge.zig");
const cli_runtime = @import("cli_runtime.zig");

pub const build_options = @import("build_options");
pub const db = @import("storage/db/mod.zig");
pub const lite = @import("storage/lite/mod.zig");
pub const metadata = @import("metadata/mod.zig");
pub const metadata_api = @import("metadata/api.zig");
pub const platform_time = @import("antfly_platform").time;
pub const portable_backup = @import("storage/portable_backup.zig");
pub const public_api = @import("api/mod.zig");
pub const raft = @import("raft/mod.zig");
pub const schema = @import("storage/schema.zig");
pub const table_schema = @import("schema/mod.zig");

pub const std_options: std.Options = .{
    .logFn = structlog.logFn,
};

fn runtimeInit(init: std.process.Init) std.process.Init {
    return .{
        .minimal = init.minimal,
        .arena = init.arena,
        .gpa = platform.allocator.processAllocator(if (!@import("builtin").single_threaded) std.heap.smp_allocator else init.gpa),
        .io = init.io,
        .environ_map = init.environ_map,
        .preopens = init.preopens,
    };
}

fn cliEntry(context: *const bridge.Context) callconv(.c) c_int {
    const init: *const std.process.Init = @ptrCast(@alignCast(context.init));
    const args: *std.process.Args.Iterator = @ptrCast(@alignCast(context.args));
    const command = context.command_ptr[0..context.command_len];

    cli_runtime.runFromIterator(runtimeInit(init.*), command, args) catch |err| {
        const message = switch (err) {
            error.FileNotFound => "required file was not found; check the configured path",
            error.AddressInUse => "listen address is already in use",
            error.InvalidCharacter, error.InvalidArguments => "invalid command-line value; run with --help",
            else => "startup failed; see the preceding diagnostic for details",
        };
        std.debug.print("antfly cli: {s}\n", .{message});
        return 1;
    };
    return 0;
}

comptime {
    @export(&cliEntry, .{ .name = "antfly_runtime_cli", .visibility = .hidden });
}
