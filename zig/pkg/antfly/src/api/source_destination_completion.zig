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

//! Completion recovery from the exact metadata acceptance receipt. A complete
//! result confirms participant resolution/publication, not source ACK or private
//! transaction/registry retirement. Those release obligations remain separate.
const std = @import("std");
const source = @import("../metadata/source_control.zig");
const receipts = @import("source_destination_receipt.zig");
const recovery = @import("completion_recovery.zig");
const metadata = @import("../metadata/api.zig");
const Context = @import("antfly_local_sources").api_operation.RequestContext;
pub const Outcome = enum { unproven, incomplete, complete, reclaimed };
pub const Completion = struct {
    recovery: recovery.Recovery,
    pub fn run(self: Completion, a: std.mem.Allocator, state: source.State, receipt: []const u8, fence: metadata.CatalogRouteFence, original: Context) !Outcome {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        var bound = try receipts.decode(a, state, receipt);
        defer bound.deinit();
        if (bound == .reclaimed) return .reclaimed;
        if (state.pending != null) return error.SourcePendingWork;
        const manifest = bound.retained.manifest.value;
        try @import("source_owner_scope.zig").requireFence(manifest.owner, fence);
        const result = try self.recovery.runSource(a, manifest.owner, fence, manifest.checkpoint_authority.table_name, manifest.transaction_id, .{ .admission_digest = manifest.admission_digest, .scope = manifest.source_scope, .purpose = manifest.checkpoint.purpose, .begin_timestamp = manifest.begin_timestamp, .commit_version = manifest.commit_version }, context);
        try context.ensureActive();
        return switch (result) {
            .unproven => .unproven,
            .incomplete => .incomplete,
            .complete => .complete,
            .pending => error.SourceOutcomeUnresolved,
            .aborted => error.DecisionConflict,
        };
    }
};
