// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Compiled owner of the distributed runtime entry points.

pub const antfly_sources = @import("source_owner_control.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;

pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;

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

comptime {
    exportInternal(&dataEntry, "antfly_runtime_data");
    exportInternal(&standbyEntry, "antfly_runtime_standby");
    exportInternal(&metadataEntry, "antfly_runtime_metadata");
    exportInternal(&standaloneEntry, "antfly_runtime_standalone");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
