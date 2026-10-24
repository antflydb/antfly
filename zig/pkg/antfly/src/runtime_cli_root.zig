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

//! Compiled owner of the cli runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");
// The final native link owns one pthread signal stack. Keep crash handlers.
pub const std_options: std.Options = .{
    .signal_stack_size = if (@import("builtin").link_libc and
        (@import("builtin").os.tag == .macos or (@import("builtin").os.tag == .linux and @import("builtin").abi.isGnu()))) null else 1 << 18,
};

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

const cli_runtime = @import("cli_runtime.zig");

fn runCli(
    init: std.process.Init,
    command: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    return cli_runtime.runFromIterator(init, command, args);
}

fn cliEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "cli", runCli);
}

comptime {
    exportInternal(&cliEntry, "antfly_runtime_cli");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
