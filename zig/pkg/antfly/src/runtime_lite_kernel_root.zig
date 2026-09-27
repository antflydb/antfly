// Copyright 2026 Antfly, Inc.
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

//! Apache embedded engine owner; no database HTTP or distributed entry points.
pub const antfly_sources = @import("source_owner_lite.zig");
pub const runtime_impl = @import("lite_capi_root.zig");
const std = @import("std");
const bridge = @import("runtime_bridge.zig");
const process = @import("runtime_process.zig");
const capi = @import("capi/db.zig");
const query = @import("storage/local_query_provider.zig");
fn runLite(init: std.process.Init, _: []const u8, args: *std.process.Args.Iterator) !void {
    return @import("cmd/lite.zig").runFromIterator(init, "antfly", args);
}
fn liteEntry(context: *const bridge.Context) callconv(.c) c_int {
    return process.runtimeEntry(context, "lite", runLite);
}
comptime {
    _ = capi;
    process.exportInternal(&liteEntry, "antfly_runtime_lite");
    process.exportInternal(&query.execute, "antfly_local_query_execute");
    process.exportInternal(&query.bufferDestroy, "antfly_local_query_buffer_destroy");
}
