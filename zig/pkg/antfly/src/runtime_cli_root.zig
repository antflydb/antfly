// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the cli runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

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
