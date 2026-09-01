// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Lightweight, family-specific view of the embedded inference provider.
//!
//! Storage runtimes keep this borrowed descriptor without importing the full
//! inference provider graph. The executor restores the typed callback before
//! crossing the checked native boundary.

const runtime_callback_abi = @import("../runtime_callback_abi.zig");

pub const Provider = struct {
    ptr: *anyopaque,
    boundary_dispatch: runtime_callback_abi.CallbackDispatch,
    chunk_input_callback: *const anyopaque,
};
