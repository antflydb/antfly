// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Coarse owner-private online merge reads. Donor and receiver operations have
//! different authority: a receiver must never inherit a donor's source ledger.
const std = @import("std");
const source = @import("online_source_contract.zig");
const pages = @import("merge_page_contract.zig");
pub const Side = enum { donor, receiver };
pub const max_request_bytes = 8 * 1024 * 1024;
pub const max_response_bytes = 32 * 1024 * 1024;
pub const SnapshotRequest = struct { receipt: pages.Progress, certificate: @import("../source_snapshot.zig").Certificate };
pub const Request = struct {
    scope: source.Scope,
    operation: union(enum) {
        admission: Side,
        status: Side,
        tail: pages.Progress,
        snapshot: SnapshotRequest,
        integrity: SnapshotRequest,
        cleanup: pages.Progress,
        checkpoint: @import("types.zig").MergeReplicationCheckpoint,
        publication: void,
        revoke: void,
        artifact: @import("source_artifact_transfer.zig").Request,
        rewrite_tail: struct { after: u64, offset: u32 = 0, max_bytes: u32 = 64 * 1024 },
    },

    pub fn ownerGroup(self: Request) u64 {
        return switch (self.operation) {
            .admission, .status => |side| if (side == .donor) self.scope.fence.owner_group_id else self.scope.fence.peer_group_id,
            .tail, .snapshot, .integrity, .publication, .artifact, .revoke, .rewrite_tail => self.scope.fence.owner_group_id,
            .cleanup, .checkpoint => self.scope.fence.peer_group_id,
        };
    }
    pub fn validate(self: Request) !void {
        if (self.scope.fence.role == .rewrite_source) {
            // Reuse authenticated donor transport, never merge's receiver or
            // membership state machine. Rewrite effects use restore staging.
            switch (self.operation) {
                .admission, .status => |side| if (side != .donor) return error.InvalidOnlineSourceCommand,
                .publication, .artifact, .revoke, .rewrite_tail => {},
                else => return error.InvalidOnlineSourceCommand,
            }
        } else if (self.scope.fence.role != .merge_source or self.operation == .rewrite_tail) return error.InvalidOnlineSourceCommand;
        if (self.operation == .rewrite_tail) {
            const page = self.operation.rewrite_tail;
            if (page.max_bytes == 0 or page.max_bytes > 64 * 1024 or page.offset > 16 * 1024 * 1024) return error.InvalidOnlineSourceCommand;
        }
        if (self.operation == .admission) {
            // This is an identity-scoped observation, never a mutation grant.
            // Requiring explicit unbound fields prevents reuse as admission.
            if (self.scope.consumer_epoch != 0 or self.scope.copy_attempt.donor_term != 0 or self.scope.copy_attempt.sequence != 0 or
                self.scope.fence.attempt != 0 or self.scope.fence.admission_epoch != 0 or !std.mem.allEqual(u8, &self.scope.fence.catalog_digest, 0)) return error.InvalidOnlineSourceCommand;
            var identity_only = self.scope;
            identity_only.consumer_epoch = 1;
            identity_only.copy_attempt = .{ .donor_term = if (identity_only.authority == .raft) 1 else 0, .sequence = 1 };
            identity_only.fence.attempt = 1;
            identity_only.fence.admission_epoch = 1;
            return identity_only.validate();
        }
        try self.scope.validate();
        if (self.operation == .artifact and !std.meta.eql(self.scope, self.operation.artifact.scope())) return error.OnlineSourceScopeChanged;
        if (self.operation == .checkpoint) {
            const checkpoint = self.operation.checkpoint;
            if (checkpoint.transition_id != self.scope.fence.transition_id or checkpoint.donor_group_id != self.scope.fence.owner_group_id or checkpoint.receiver_group_id != self.scope.fence.peer_group_id or
                (checkpoint.kind != .accept and !std.meta.eql(checkpoint.copy_attempt, self.scope.copy_attempt)) or checkpoint.allow_doc_identity_reassignment or checkpoint.receiver_identity_reassignment_namespace != null) return error.MergeCopyFenced;
            for ([_][]const u8{ checkpoint.receiver_base_start, checkpoint.receiver_base_end, checkpoint.merged_start, checkpoint.merged_end }) |bound| if (bound.len > pages.max_cursor_bytes) return error.InvalidMergePage;
        }
        if (self.operation == .tail or self.operation == .snapshot or self.operation == .integrity or self.operation == .cleanup) {
            const receipt = switch (self.operation) {
                .tail => |value| value,
                .snapshot, .integrity => |value| value.receipt,
                .cleanup => |value| value,
                else => unreachable,
            };
            try validateReceiptScope(self.scope, receipt);
            if (self.operation == .tail and (receipt.phase != .tail or receipt.cursor.len != 0)) return error.InvalidMergePage;
            if (self.operation == .cleanup and ((receipt.phase != .cleanup and receipt.phase != .cleanup_integrity) or receipt.assembly != null)) return error.InvalidMergePage;
            if (self.operation == .snapshot or self.operation == .integrity) {
                const certificate = if (self.operation == .snapshot) self.operation.snapshot.certificate else self.operation.integrity.certificate;
                if (receipt.phase != .rows and receipt.phase != .artifacts) return error.InvalidMergePage;
                if (self.operation == .integrity and (receipt.phase != .artifacts or receipt.source.integrity == null)) return error.InvalidMergePage;
                if (self.operation == .snapshot and receipt.phase == .artifacts and receipt.source.integrity != null) return error.InvalidMergePage;
                if (!certificate.cut.namespace.eql(self.scope.fence.namespace) or certificate.cut.applied_index != receipt.source.applied_index or
                    !std.meta.eql(certificate.integrity, receipt.source.integrity) or
                    certificate.cut.retained_start != receipt.source.retention.?.after_sequence or !std.mem.eql(u8, &try certificate.digest(), &receipt.source.pin_digest)) return error.SourceSnapshotCutMismatch;
            }
        }
    }
    pub fn jsonStringify(self: Request, stream: anytype) !void {
        try @import("relational_integrity_json.zig").write(self, stream);
    }
};

/// Validate the actual durable phase, then bind its complete source/receiver
/// identity. Status must not manufacture another phase merely to reuse an RPC.
pub fn validateReceiptScope(scope: source.Scope, receipt: pages.Progress) !void {
    try scope.validate();
    if (scope.fence.role != .merge_source) return error.InvalidMergePage;
    try receipt.validate();
    if (!receipt.matches(.{ .transition_id = scope.fence.transition_id, .donor_group_id = scope.fence.owner_group_id, .receiver_group_id = scope.fence.peer_group_id, .identity_namespace = scope.receiver_namespace, .copy_attempt = scope.copy_attempt }) or
        !receipt.source.namespace.eql(scope.fence.namespace) or receipt.source.retention == null or receipt.source.retention.?.epoch != scope.consumer_epoch)
        return error.InvalidMergePage;
}

pub const AdmissionFacts = struct {
    authority: source.Authority = .raft,
    /// Rewrite-only immutable public mappings for every durable native epoch.
    /// The caller owns the decoded response, not a borrowed native DB view.
    source_schemas: []const []const u8 = &.{},
    namespace: @import("doc_identity_namespace.zig").Namespace,
    eligible: bool,
    catalog_digest: [32]u8,
    integrity: ?pages.IntegrityBinding = null,
    next_topology_epoch: u64,
    next_consumer_epoch: u64,
    /// Native marker term is only a lower bound. The leader-fenced service
    /// replaces this with its current Raft term, including an empty owner.
    donor_term: u64,
    next_copy_sequence: u64,
};

pub const SourceStatus = struct {
    ordinary_conflict: bool = false,
    ordinary_scope_conflict: bool = false,
    scope: source.Scope,
    certificate: ?@import("../source_snapshot.zig").Certificate = null,
    progress: ?@import("online_source.zig").Progress,
    retained_head: u64,
    retained_reclaimed: u64 = 0,
    retained_reclaimable: u64 = 0,
    fence: ?@import("relational_integrity_topology_contract.zig").Fence,
    next_epoch: u64,
    drained: bool,
    /// REF3 captures rows/timestamps/integrity, not independently authored
    /// graph/vector artifacts. A false value prevents online admission.
    row_derived_indexes: bool,
};
pub const ReceiverStatus = struct {
    scope: source.Scope,
    progress: ?pages.Progress,
    state: ?@import("merge_contract.zig").State,
};

pub const Prepared = struct {
    scope: source.Scope,
    /// Null means bounded recovery work remains (e.g. seeking a frame after
    /// failover), not end-of-stream and never permission to skip a receipt.
    request: ?@import("types.zig").BatchRequest = null,
};

test "relational index system online IO inline receipts enforce durable decode bounds" {
    const scope: source.Scope = .{
        .fence = .{ .transition_id = 9, .attempt = 1, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .catalog_digest = @splat(7) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 5 },
        .consumer_epoch = 6,
        .copy_attempt = .{ .donor_term = 8, .sequence = 1 },
    };
    const valid: pages.Progress = .{ .version = 2, .transition_id = 9, .donor_group_id = 2, .receiver_group_id = 3, .receiver_namespace = scope.receiver_namespace, .attempt = scope.copy_attempt, .source = .{ .namespace = scope.fence.namespace, .pin_digest = @splat(1), .applied_index = 19, .retention = .{ .epoch = 6, .after_sequence = 11 } }, .phase = .cleanup, .tail_sequence = 11 };
    try (Request{ .scope = scope, .operation = .{ .cleanup = valid } }).validate();
    const oversized = try std.testing.allocator.alloc(u8, pages.max_cursor_bytes + 1);
    defer std.testing.allocator.free(oversized);
    @memset(oversized, 'x');
    var invalid = valid;
    invalid.cursor = oversized;
    try std.testing.expectError(error.InvalidMergePage, (Request{ .scope = scope, .operation = .{ .cleanup = invalid } }).validate());
    invalid = valid;
    invalid.version = 99;
    try std.testing.expectError(error.InvalidMergePage, (Request{ .scope = scope, .operation = .{ .cleanup = invalid } }).validate());
    invalid = valid;
    invalid.phase = .tail;
    invalid.version = 4;
    invalid.assembly = .{ .transfer_digest = @splat(1), .last_digest = @splat(2), .next_offset = 1 };
    try std.testing.expectError(error.InvalidMergePage, (Request{ .scope = scope, .operation = .{ .tail = invalid } }).validate());
    invalid.assembly = .{ .transfer_digest = @splat(1), .last_digest = @splat(2), .next_offset = pages.chunk_bytes };
    try (Request{ .scope = scope, .operation = .{ .tail = invalid } }).validate();
    invalid.snapshot_position = .{ .object = 0, .offset = 4, .remaining = 0 };
    try std.testing.expectError(error.InvalidMergePage, (Request{ .scope = scope, .operation = .{ .tail = invalid } }).validate());
}
