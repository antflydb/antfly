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
    /// Caller-owned MAX span, borrowed only through this synchronous Source.
    matrix_scratch: ?[]f32 = null,
    prepare_dense: ?*const fn (*anyopaque, *const @import("../ops/ops.zig").ComputeBackend, *const @import("../ops/ops.zig").NativeLinearPreparation) anyerror!?@import("../ops/ops.zig").NativeLinearPrepared = null,
    /// Synchronous copied operation geometry; no tensors/loan escape callback.
    prepare_projection: ?*const fn (*anyopaque, *const @import("../ops/ops.zig").ComputeBackend, *const @import("../ops/ops.zig").LinearTripleInvocation) anyerror!bool = null,
    /// Exact copied request-local cache recipe; scope ends at the SAME
    /// producer return. No callback/CT/creation epoch enters a cache owner.
    prepare_cache: ?*const fn (*anyopaque, *const @import("../ops/ops.zig").ComputeBackend, @import("../ops/ops.zig").NativeWeightCachePreparation) anyerror!?@import("../ops/ops.zig").EmbeddingCacheScope = null,
    ptr: *anyopaque,
    access: *const fn (*anyopaque, *const @import("../ops/ops.zig").ComputeBackend, []const u8, Access) anyerror!?@import("../ops/ops.zig").CT,
};
