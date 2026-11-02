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

//! Compile actual standalone/data/metadata entrypoints without linking provider archives.
const std = @import("std");
pub const antfly_sources = if (@import("storage_source_options").control_only) @import("source_owner_control.zig") else @import("source_owner_physical.zig");
pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;
pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend;
pub const capi_dependencies = @import("capi_dependencies.zig");
comptime {
    if (@import("storage_source_options").control_only != @import("worker_memory_bootstrap_options").compiled_storage)
        @compileError("worker memory entry check storage profile mismatch");
}

export fn antfly_check_standalone_worker_memory_bootstrap(raw: *anyopaque) callconv(.c) u8 {
    const init: *std.process.Init = @ptrCast(@alignCast(raw));
    @import("standalone/runtime.zig").run(init.*) catch return 0;
    return 1;
}
export fn antfly_check_data_worker_memory_bootstrap(raw: *anyopaque) callconv(.c) u8 {
    const init: *std.process.Init = @ptrCast(@alignCast(raw));
    @import("data/runtime.zig").run(init.*) catch return 0;
    return 1;
}

export fn antfly_check_metadata_worker_memory_bootstrap(raw: *anyopaque) callconv(.c) u8 {
    const init: *std.process.Init = @ptrCast(@alignCast(raw));
    @import("metadata/runtime.zig").run(init.*) catch return 0;
    return 1;
}
