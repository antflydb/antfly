// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-call cleanup capabilities. The boundary controls CPU quota retirement;
//! only an explicitly supplied live operation authorizes numerical/provider
//! mutation. Neither capability is saved as a publication-time permission.
const operation_mod = @import("backend_operation.zig");
const Epoch = @import("retirement_sink.zig").Epoch;
const ops = @import("../ops/ops.zig");

pub const Context = struct {
    boundary: ?*const Epoch = null,
    operation: ?operation_mod.Borrowed = null,
    /// Explicit raw outside-execution API contract. Absence of an epoch never
    /// implies this contract and this marker grants no numerical authority.
    outside_execution: bool = false,

    /// Validate against the actual target-owned table/provider. Metadata and
    /// absent scopes never confer numerical cleanup authority. A supplied B
    /// must be the exact epoch bound to this current host operation.
    pub fn operationFor(self: Context, backend: *const ops.ComputeBackend) !operation_mod.Borrowed {
        const token = self.operation orelse return error.BackendCleanupAuthorityRequired;
        if (token.mode == .metadata) return error.BackendCleanupNumericalAuthorityRequired;
        try token.validate(.{
            .implementation = .{ .context = backend.ptr, .table = backend.vtable },
            .program = null,
            .mode = .legacy_unknown_frame,
        });
        if (self.boundary) |boundary| {
            var bound = (try token.retainCleanupEpoch()) orelse return error.BackendOperationCleanupUnavailable;
            defer bound.release();
            if (bound.record != boundary.record or bound.nonce != boundary.nonce)
                return error.BackendOperationCleanupChanged;
        }
        return token;
    }
};
