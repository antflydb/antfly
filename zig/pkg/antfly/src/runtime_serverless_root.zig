// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the serverless runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

pub const storage_backend_erased = @import("storage/backend_erased.zig");

pub const lsm_backend = @import("storage/lsm_backend/mod.zig");

const serverless_runtime = @import("cmd/serverless.zig");

fn runServerless(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return serverless_runtime.runFromIterator(init, "antfly", args);
}

fn serverlessEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "serverless", runServerless);
}

comptime {
    exportInternal(&serverlessEntry, "antfly_runtime_serverless");
}
