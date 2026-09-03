// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Lightweight, family-specific view of the embedded inference provider.
//!
//! Storage runtimes keep this borrowed descriptor without importing the full
//! inference provider graph. The executor restores the typed callback before
//! crossing the checked native boundary.

const std = @import("std");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");
const remote_capabilities = @import("../inference/remote_capabilities.zig");

pub const Provider = struct {
    ptr: *anyopaque,
    boundary_dispatch: runtime_callback_abi.CallbackDispatch,
    chunk_input_callback: *const anyopaque,
    remote_capability_cache: ?*remote_capabilities.Cache = null,
    io: ?std.Io = null,
};
