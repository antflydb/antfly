// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Borrowed synchronous execution authority on ONE request-local backend header.
//! Never copy this pointer into a returned tensor/backend/cache/graph owner.
//! The first producer is the certified eager GPT host-output entry: it exports
//! owning host Tensors, never this header or a resident compute tensor.
pub const Context = struct {
    cleanup: @import("cleanup_context.zig").Context,
    native_weights: ?*const NativeWeights = null,
    workspace_uses: ?*@import("../backends/metal_workspace_ownership.zig").ReceiverUses = null,
    recurrent_state: ?*@import("recurrent_state_admission.zig").Budget = null,
};

/// Actual Source-owned synchronous slice boundary. This carries no cached
/// creation Epoch and is stripped with the containing borrowed Context.
pub const NativeWeights = struct {
    pub const Access = enum { get, acquire, embedding };
    ptr: *anyopaque,
    access: *const fn (*anyopaque, *const @import("../ops/ops.zig").ComputeBackend, []const u8, Access) anyerror!?@import("../ops/ops.zig").CT,
};
