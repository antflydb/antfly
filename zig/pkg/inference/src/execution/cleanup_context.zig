// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
    /// Borrowed only through a local explicit-outside cleanup function. The
    /// actual outside Owner seals its real B before this stack capability dies.
    synchronous_outside_raw: ?*@import("resource_retirement.zig").SynchronousOutsideRawLeases = null,

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
