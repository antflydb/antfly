// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Compiled owner of the api_kernel runtime entry points.

pub const antfly_sources = @import("source_owner_common.zig");

const std = @import("std");

const bridge = @import("runtime_bridge.zig");

const process = @import("runtime_process.zig");

const runtimeEntry = process.runtimeEntry;

const exportInternal = process.exportInternal;

pub const storage_backend_erased = @import("antfly_local_sources").storage_backend_erased;

pub const lsm_backend = @import("antfly_local_sources").storage_lsm_backend_mod;

const api_kernel_exports = @import("api/kernel_exports.zig");

comptime {
    exportInternal(&api_kernel_exports.getFunctionTable, "antfly_api_kernel_get_function_table");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
