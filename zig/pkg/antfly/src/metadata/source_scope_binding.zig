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

//! One derivation for metadata capture and frozen destination planning. These
//! values bind identity; they prove no physical registration or legacy drainage.
const std = @import("std");
const control = @import("source_control.zig");
pub const native = @import("antfly_local_sources").storage_source_scope;
pub fn spec(state: control.State) !?native.Spec {
    try state.validate();
    if (!state.scope_fencing) return null;
    const source = state.scope orelse return error.SourceBindingConflict;
    const authority = state.checkpoint_authority orelse return error.SourceBindingConflict;
    const result: native.Spec = .{ .actor_table_id = source.table_id, .source_id = source.source_id, .generation = source.generation, .binding = source.identity(), .checkpoint_owner = try native.Basis.fromOwner(authority.owner) };
    try result.validate();
    return result;
}
pub fn reference(state: control.State, sequence: u64, token: u64) !?native.Reference {
    const binding = (try spec(state)) orelse return null;
    const result: native.Reference = .{ .domain = binding.domain(), .binding = binding.binding, .generation = binding.generation, .token = token, .sequence = sequence };
    try result.validate();
    return result;
}
/// The advertised scoped adapter must return the reference it actually froze,
/// together with the artifact token from the same planning grant.
pub fn requirePlan(expected: ?native.Reference, actual: ?native.Reference, artifact: ?control.artifact_control.Spec) !void {
    if (!std.meta.eql(expected, actual)) return error.SourceTransactionIdentityConflict;
    if (expected) |bound| {
        try bound.validate();
        const retained = artifact orelse return error.UnsupportedOperation;
        try retained.validate();
        if (!std.meta.eql(retained.binding, bound.binding) or retained.token() != bound.token) return error.SourceTransactionIdentityConflict;
    }
}
