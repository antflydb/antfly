// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the inference runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

const runtimeAllocator = process.runtimeAllocator;

const inference_runtime = @import("inference_runtime/runtime.zig");

fn runInference(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return inference_runtime.runFromIterator(init, "antfly", args);
}

fn inferenceEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "inference", runInference);
}

comptime {
    _ = @import("antfly_inference_native_exports");
    exportInternal(&inferenceEntry, "antfly_runtime_inference");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
