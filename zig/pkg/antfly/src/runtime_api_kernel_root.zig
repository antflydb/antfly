// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the api_kernel runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

pub const storage_backend_erased = @import("storage/backend_erased.zig");

pub const lsm_backend = @import("storage/lsm_backend/mod.zig");

const api_kernel_exports = @import("api/kernel_exports.zig");

comptime {
    exportInternal(&api_kernel_exports.getFunctionTable, "antfly_api_kernel_get_function_table");
}
