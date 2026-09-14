// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the distributed runtime entry points.

pub const antfly_sources = @import("source_owner_control.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

pub const storage_backend_erased = @import("storage/backend_erased.zig");

pub const lsm_backend = @import("storage/lsm_backend/mod.zig");

const standby_runtime = @import("cmd/standby.zig");

const data_runtime = @import("data/runtime.zig");

const metadata_runtime = @import("metadata/runtime.zig");

const standalone_runtime = @import("standalone/runtime.zig");

fn runData(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return data_runtime.runFromIterator(init, "antfly", args);
}

fn runStandby(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return standby_runtime.runFromIterator(init, "antfly", args);
}

fn runMetadata(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return metadata_runtime.runFromIterator(init, "antfly", args);
}

fn runStandalone(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return standalone_runtime.runFromIterator(init, "antfly", args);
}

fn dataEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "data", runData);
}

fn standbyEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "standby", runStandby);
}

fn metadataEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "metadata", runMetadata);
}

fn standaloneEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "standalone", runStandalone);
}

fn runLiteServe(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    const subcommand = args.next() orelse return error.InvalidArguments;
    if (!std.mem.eql(u8, subcommand, "serve")) return error.InvalidArguments;
    return @import("cmd/lite_serve.zig").run(init, args);
}

fn standaloneLiteEntry(context: *const bridge.Context) callconv(.c) c_int {
    return runtimeEntry(context, "lite serve", runLiteServe);
}

comptime {
    exportInternal(&dataEntry, "antfly_runtime_data");
    exportInternal(&standbyEntry, "antfly_runtime_standby");
    exportInternal(&metadataEntry, "antfly_runtime_metadata");
    exportInternal(&standaloneEntry, "antfly_runtime_standalone");
    exportInternal(&standaloneLiteEntry, "antfly_runtime_standalone_lite");
}
