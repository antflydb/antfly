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

//! Durable source actor authority. Provider offsets, admission receipts and
//! authoritative commit references are separate positions. All returned state
//! borrows its inputs plus the supplied attempt arena; encode before releasing
//! either. This module never treats acceptance as an index publication.
const std = @import("std");
const A = std.mem.Allocator;
const source_contract = @This();
pub const Digest = [32]u8;
pub const artifact_control = @import("source_artifact_control.zig");
pub const checkpoint_authority = @import("source_checkpoint_authority.zig");
pub const protocol_version: u16 = 90;
pub const closing_protocol_version: u16 = 104;
pub const manifest_protocol_version: u16 = 105;
pub const max_pending_bytes = 1024 * 1024;
pub const max_state_bytes = 8 * 1024 * 1024;
pub const max_outstanding = 256;
pub const max_outstanding_encoded_bytes = 1024 * 1024;
// JSON's worst expansion is six bytes per UTF-8 input byte. Reserve one
// complete receipt/checkpoint entry, including fixed digests and field names.
const acceptance_credit = 6 * (8192 + 1024) + 1024;
fn outstandingBytes(entries: []const Accepted) usize {
    var buffer: [256]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    std.json.Stringify.value(entries, .{}, &counter.writer) catch unreachable;
    return counter.fullCount();
}

pub fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn nonzero(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
fn text(value: []const u8, max: usize, empty: bool) bool {
    return (empty or value.len != 0) and value.len <= max and std.unicode.utf8ValidateSlice(value);
}
fn hashValue(value: anytype) Digest {
    const Output = struct {
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = std.Io.Writer.noopFlush }, .buffer = &.{} },
        hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            for (data[0 .. data.len - 1]) |bytes| self.hash.update(bytes);
            for (0..splat) |_| self.hash.update(data[data.len - 1]);
            return std.Io.Writer.countSplat(data, splat);
        }
    };
    var out: Output = .{};
    std.json.Stringify.value(value, .{}, &out.writer) catch unreachable;
    return out.hash.finalResult();
}
pub const Destination = enum { native, writable_iceberg };
pub const Scope = struct {
    table_id: u64,
    source_id: Digest,
    source_ordinal: u32,
    generation: u64,
    definition: Digest,
    provider: Digest,
    destination: Destination,
    destination_binding: Digest,
    pub fn validate(self: Scope) !void {
        if (self.table_id == 0 or self.generation == 0 or !nonzero(self.source_id) or !nonzero(self.definition) or !nonzero(self.provider) or !nonzero(self.destination_binding)) return error.InvalidSourceControl;
    }
    pub fn sameBinding(self: Scope, other: Scope) bool {
        var left = self;
        var right = other;
        // Ordinal is a catalog locator, never the source's identity.
        left.source_ordinal = 0;
        right.source_ordinal = 0;
        return std.meta.eql(left, right);
    }
    pub fn identity(self: Scope) Digest {
        return hashValue(.{
            .domain = "antfly:source-binding:v1",
            .table_id = self.table_id,
            .source_id = self.source_id,
            .generation = self.generation,
            .definition = self.definition,
            .provider = self.provider,
            .destination = self.destination,
            .destination_binding = self.destination_binding,
        });
    }
};
/// Logical source ownership is independent of a destination plan/upload attempt.
/// Including artifact or manifest digests here would create a circular identity
/// when the frozen PREPARE bytes themselves carry this purpose.
pub const LogicalInput = struct {
    source_sequence: u64,
    batch_id: []const u8,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    payload_digest: Digest,
    accepted_at_ns: u64,
};
pub fn inputFingerprint(scope: Scope, input: LogicalInput) !Digest {
    try scope.validate();
    if (input.source_sequence == 0 or input.accepted_at_ns == 0 or input.accepted_at_ns == std.math.maxInt(u64) or !text(input.batch_id, 256, false) or !text(input.checkpoint, 1024, false) or !nonzero(input.payload_digest)) return error.InvalidSourceControl;
    if (input.expected_checkpoint) |prior| if (!text(prior, 1024, false)) return error.InvalidSourceControl;
    return hashValue(.{ .domain = "antfly:logical-source-input:v1", .scope = scope.identity(), .input = input });
}
pub const Reservation = struct {
    sequence: u64,
    batch_id: []const u8,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    accepted_at_ns: u64,
    payload_digest: Digest,
    plan_digest: Digest,
    transaction_id: [16]u8,
    destination: Destination,
    /// Recoverable original routing/destination authority, not current routes.
    delivery_manifest: []const u8,
    previous_reservation_digest: Digest = @splat(0),
    replan_receipt: []const u8 = "",
    artifact: ?artifact_control.Spec = null,
    retain_manifest: bool = false,
    pub fn jsonStringify(self: Reservation, stream: anytype) @TypeOf(stream.*).Error!void {
        return writeArtifactFields(self, stream);
    }

    pub fn validate(self: Reservation) !void {
        if (self.sequence == 0 or self.accepted_at_ns == 0 or self.accepted_at_ns == std.math.maxInt(u64) or
            !text(self.batch_id, 256, false) or !text(self.checkpoint, 1024, false) or
            !text(self.delivery_manifest, max_pending_bytes, false) or
            !nonzero(self.payload_digest) or !nonzero(self.plan_digest) or !nonzero(self.transaction_id)) return error.InvalidSourceControl;
        if (self.artifact) |spec| try spec.validate();
        if (self.retain_manifest and (self.artifact == null or self.delivery_manifest.len > artifact_control.max_manifest_bytes)) return error.InvalidSourceControl;
        if (self.expected_checkpoint) |previous| if (!text(previous, 1024, false)) return error.InvalidSourceControl;
        if (!text(self.replan_receipt, 8192, true) or nonzero(self.previous_reservation_digest) != (self.replan_receipt.len != 0)) return error.InvalidSourceControl;
    }
    pub fn inputFingerprint(self: Reservation, scope: Scope) !Digest {
        return source_contract.inputFingerprint(scope, .{ .source_sequence = self.sequence, .batch_id = self.batch_id, .checkpoint = self.checkpoint, .expected_checkpoint = self.expected_checkpoint, .payload_digest = self.payload_digest, .accepted_at_ns = self.accepted_at_ns });
    }
    pub fn identity(self: Reservation, scope: Scope) Digest {
        const legacy = hashValue(.{
            .domain = "antfly:source-reservation:v1",
            .scope = scope.identity(),
            .sequence = self.sequence,
            .batch_id = self.batch_id,
            .checkpoint = self.checkpoint,
            .expected_checkpoint = self.expected_checkpoint,
            .accepted_at_ns = self.accepted_at_ns,
            .payload_digest = self.payload_digest,
            .plan_digest = self.plan_digest,
            .transaction_id = self.transaction_id,
            .destination = self.destination,
            .delivery_manifest = self.delivery_manifest,
            .previous_reservation_digest = self.previous_reservation_digest,
            .replan_receipt = self.replan_receipt,
        });
        const prior = if (self.artifact) |spec| hashValue(.{ .domain = "antfly:source-reservation:v2", .legacy = legacy, .artifact = spec }) else legacy;
        return if (self.retain_manifest) hashValue(.{ .domain = "antfly:source-reservation:v3", .prior = prior, .retain_manifest = true }) else prior;
    }
};
pub const Frontier = struct {
    sequence: u64 = 0,
    checkpoint: []const u8 = "",
    receipt_digest: Digest = @splat(0),
    reservation_digest: Digest = @splat(0),
    reference: []const u8 = "",
};
pub const Accepted = struct {
    sequence: u64,
    checkpoint: []const u8,
    reservation_digest: Digest,
    payload_digest: Digest,
    receipt: []const u8,
};
pub const State = struct {
    version: u32 = 1,
    revision: u64 = 0,
    scope: ?Scope = null,
    pending: ?Reservation = null,
    accepted: Frontier = .{},
    committed: Frontier = .{},
    outstanding: []const Accepted = &.{},
    retired: bool = false,
    closing: bool = false,
    scope_fencing: bool = false,
    manifest_retention: bool = false,
    retirement_receipt: []const u8 = "",
    retired_reservation_digest: Digest = @splat(0),
    artifact_control: ?@import("source_artifact_control.zig").State = null,
    checkpoint_authority: ?source_contract.checkpoint_authority.Spec = null,
    pub fn jsonStringify(self: State, stream: anytype) @TypeOf(stream.*).Error!void {
        return writeArtifactFields(self, stream);
    }

    pub fn validate(self: State) !void {
        if (self.version != 1 or self.outstanding.len > max_outstanding or !text(self.retirement_receipt, 8192, true)) return error.InvalidSourceControl;
        if (self.scope) |scope| try scope.validate() else {
            if (self.revision != 0 or self.pending != null or self.artifact_control != null or self.checkpoint_authority != null or self.accepted.sequence != 0 or self.committed.sequence != 0 or self.outstanding.len != 0 or self.retired or self.closing or self.scope_fencing) return error.InvalidSourceControl;
        }
        if (self.checkpoint_authority) |authority| {
            try authority.validate();
            if (self.scope == null or self.scope.?.destination != .native or self.artifact_control == null or !std.mem.eql(u8, &authority.binding, &self.scope.?.identity()) or (authority.retirement != null and !self.retired)) return error.InvalidSourceControl;
        }
        if (self.manifest_retention and (self.scope == null or self.artifact_control == null)) return error.InvalidSourceControl;
        if (self.scope_fencing and (self.checkpoint_authority == null or self.scope == null or self.scope.?.destination != .native)) return error.InvalidSourceControl;
        if (self.committed.sequence > self.accepted.sequence) return error.InvalidSourceControl;
        for ([_]Frontier{ self.accepted, self.committed }) |frontier| {
            if (!text(frontier.checkpoint, 1024, frontier.sequence == 0) or !text(frontier.reference, 8192, frontier.sequence == 0)) return error.InvalidSourceControl;
            if ((frontier.sequence == 0) != (frontier.checkpoint.len == 0)) return error.InvalidSourceControl;
            if (frontier.sequence == 0) {
                if (nonzero(frontier.receipt_digest) or nonzero(frontier.reservation_digest) or frontier.reference.len != 0) return error.InvalidSourceControl;
            } else if (!nonzero(frontier.receipt_digest) or !nonzero(frontier.reservation_digest)) return error.InvalidSourceControl;
        }
        if (self.pending) |pending| {
            try pending.validate();
            if (pending.retain_manifest != self.manifest_retention) return error.InvalidSourceControl;
            if (self.scope == null or pending.destination != self.scope.?.destination) return error.SourceBindingConflict;
            if (pending.sequence != (std.math.add(u64, self.accepted.sequence, 1) catch return error.InvalidSourceControl) or !predecessor(pending.expected_checkpoint, self.accepted)) return error.InvalidSourceControl;
        }
        var next = self.committed.sequence;
        for (self.outstanding) |entry| {
            next = std.math.add(u64, next, 1) catch return error.InvalidSourceControl;
            if (entry.sequence != next or !text(entry.checkpoint, 1024, false) or !text(entry.receipt, 8192, false) or !nonzero(entry.reservation_digest) or !nonzero(entry.payload_digest)) return error.InvalidSourceControl;
        }
        if (next != self.accepted.sequence) return error.InvalidSourceControl;
        const queued_bytes = outstandingBytes(self.outstanding);
        if (queued_bytes > max_outstanding_encoded_bytes or (self.pending != null and
            acceptance_credit > max_outstanding_encoded_bytes - queued_bytes)) return error.InvalidSourceControl;
        if (self.accepted.sequence != 0 and !std.mem.eql(u8, &self.accepted.receipt_digest, &digest(self.accepted.reference))) return error.InvalidSourceControl;
        if (self.outstanding.len != 0) {
            const last = self.outstanding[self.outstanding.len - 1];
            if (!std.mem.eql(u8, last.checkpoint, self.accepted.checkpoint) or
                !std.mem.eql(u8, &last.reservation_digest, &self.accepted.reservation_digest) or
                !std.mem.eql(u8, last.receipt, self.accepted.reference)) return error.InvalidSourceControl;
        } else if (self.accepted.sequence != 0 and
            (!std.mem.eql(u8, &self.accepted.receipt_digest, &self.committed.receipt_digest) or
                !std.mem.eql(u8, &self.accepted.reservation_digest, &self.committed.reservation_digest) or
                !std.mem.eql(u8, self.accepted.checkpoint, self.committed.checkpoint))) return error.InvalidSourceControl;
        if (self.artifact_control) |ledger| {
            try ledger.validate();
            if (ledger.last_reclaimed) |last| if (!std.mem.eql(u8, &last.spec.binding, &self.scope.?.identity())) return error.InvalidSourceControl;
            if (ledger.current) |record| {
                if (self.manifest_retention != (record.recovery != null)) return error.InvalidSourceControl;
                if (self.pending) |pending| {
                    if (pending.retain_manifest != self.manifest_retention) return error.InvalidSourceControl;
                    if (pending.retain_manifest) {
                        const retained = record.recovery orelse return error.InvalidSourceControl;
                        if (!std.mem.eql(u8, &retained.digest, &pending.plan_digest) or !std.mem.eql(u8, retained.bytes, pending.delivery_manifest)) return error.InvalidSourceControl;
                    }
                }
                if (!std.mem.eql(u8, &record.spec.binding, &self.scope.?.identity()) or self.retired) return error.InvalidSourceControl;
                switch (record.phase) {
                    .reserved, .sealed => {
                        const pending = self.pending orelse return error.InvalidSourceControl;
                        if (!std.mem.eql(u8, &record.reservation, &pending.identity(self.scope.?)) or pending.artifact == null or !std.meta.eql(record.spec, pending.artifact.?)) return error.InvalidSourceControl;
                    },
                    .accepted => if (self.pending != null or !std.mem.eql(u8, &record.reservation, &self.accepted.reservation_digest)) return error.InvalidSourceControl,
                    .garbage => {
                        if (self.pending) |pending| {
                            if (!std.mem.eql(u8, &record.reservation, &pending.identity(self.scope.?)) or pending.artifact == null or !std.meta.eql(record.spec, pending.artifact.?)) return error.InvalidSourceControl;
                        } else if (!std.mem.eql(u8, &record.reservation, &self.accepted.reservation_digest)) return error.InvalidSourceControl;
                    },
                }
            } else if (self.pending) |pending| {
                const reclaimed = ledger.last_reclaimed orelse return error.InvalidSourceControl;
                if (pending.artifact == null or !std.mem.eql(u8, &reclaimed.reservation, &pending.identity(self.scope.?)) or !std.meta.eql(reclaimed.spec, pending.artifact.?)) return error.InvalidSourceControl;
            }
        } else if (self.pending) |pending| if (pending.artifact != null) return error.InvalidSourceControl;
        if (self.retired and (self.pending != null or self.outstanding.len != 0 or self.retirement_receipt.len == 0)) return error.InvalidSourceControl;
        if (!self.retired and (self.retirement_receipt.len != 0 or nonzero(self.retired_reservation_digest))) return error.InvalidSourceControl;
    }
    fn advance(self: State) !State {
        var next = self;
        next.revision = std.math.add(u64, self.revision, 1) catch return error.InvalidSourceControl;
        return next;
    }
};
fn predecessor(previous: ?[]const u8, accepted: Frontier) bool {
    if (accepted.sequence == 0) return previous == null;
    return previous != null and std.mem.eql(u8, previous.?, accepted.checkpoint);
}
pub const Replan = struct {
    reservation_digest: Digest,
    plan_digest: Digest,
    transaction_id: [16]u8,
    delivery_manifest: []const u8,
    not_admitted_receipt: []const u8,
    artifact: ?artifact_control.Spec = null,
    retain_manifest: bool = false,
    pub fn jsonStringify(self: Replan, stream: anytype) @TypeOf(stream.*).Error!void {
        return writeArtifactFields(self, stream);
    }
};

pub const Mutation = union(enum) {
    /// Stop new grants while preserving original recovery and cleanup debt.
    close: void,
    enable_scope_fencing: void,
    configure: Scope,
    bind_checkpoint: checkpoint_authority.Spec,
    fence_checkpoint: struct { authority_digest: Digest, proof: Digest },
    reserve: Reservation,
    /// The adapter proves old routes cannot admit a delayed original attempt.
    /// This changes delivery authority, not logical source progress or payload.
    replan: Replan,
    seal_artifact: struct { reservation_digest: Digest, stamp: Digest },
    retire_artifact: struct { reservation_digest: Digest, proof: Digest, reason: enum { unsealed, not_admitted, committed } },
    reclaim_artifact: struct { reservation_digest: Digest, proof: Digest },
    accept: struct {
        reservation_digest: Digest,
        receipt: []const u8,
        /// Only a proven authoritative commit supplies this reference.
        commit_reference: ?[]const u8 = null,
    },
    commit: struct { sequence: u64, receipt_digest: Digest, reference: []const u8 },
    retire: struct { receipt: []const u8 },
    /// The adapter must prove the original attempt was rejected/fenced and
    /// source-owned retirement is complete. An ambiguous attempt cannot use
    /// this transition. The pending identity remains in the retired record.
    reject_and_retire: struct { reservation_digest: Digest, receipt: []const u8 },
    reseed: Scope,

    pub fn validate(self: Mutation) !void {
        switch (self) {
            .close, .enable_scope_fencing => {},
            .configure, .reseed => |scope| try scope.validate(),
            .bind_checkpoint => |authority| {
                try authority.validate();
                if (authority.retirement != null) return error.InvalidSourceControl;
            },
            .fence_checkpoint => |fence| if (!nonzero(fence.authority_digest) or !nonzero(fence.proof)) return error.InvalidSourceControl,
            .reserve => |reservation| try reservation.validate(),
            .replan => |plan| {
                if (plan.artifact) |spec| try spec.validate();
                if (plan.retain_manifest and (plan.artifact == null or plan.delivery_manifest.len > artifact_control.max_manifest_bytes)) return error.InvalidSourceControl;
                if (!nonzero(plan.reservation_digest) or !nonzero(plan.plan_digest) or !nonzero(plan.transaction_id) or
                    !text(plan.delivery_manifest, max_pending_bytes, false) or !text(plan.not_admitted_receipt, 8192, false)) return error.InvalidSourceControl;
            },
            .seal_artifact => |seal| if (!nonzero(seal.reservation_digest) or !nonzero(seal.stamp)) return error.InvalidSourceControl,
            .retire_artifact => |retirement| if (!nonzero(retirement.reservation_digest) or !nonzero(retirement.proof)) return error.InvalidSourceControl,
            .reclaim_artifact => |reclaimed| if (!nonzero(reclaimed.reservation_digest) or !nonzero(reclaimed.proof)) return error.InvalidSourceControl,
            .accept => |accepted| {
                if (!nonzero(accepted.reservation_digest) or !text(accepted.receipt, 8192, false)) return error.InvalidSourceControl;
                if (accepted.commit_reference) |ref| if (!text(ref, 8192, false)) return error.InvalidSourceControl;
            },
            .commit => |committed| if (committed.sequence == 0 or !nonzero(committed.receipt_digest) or !text(committed.reference, 8192, false)) return error.InvalidSourceControl,
            .retire => |retirement| if (!text(retirement.receipt, 8192, false)) return error.InvalidSourceControl,
            .reject_and_retire => |retirement| if (!nonzero(retirement.reservation_digest) or !text(retirement.receipt, 8192, false)) return error.InvalidSourceControl,
        }
    }
    pub fn apply(self: Mutation, a: A, state: State) !State {
        try self.validate();
        try state.validate();
        var next = try state.advance();
        switch (self) {
            .enable_scope_fencing => {
                if (state.scope_fencing) return state;
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (state.closing or state.retired or scope.destination != .native or state.checkpoint_authority == null) return error.SourceBindingConflict;
                if (state.pending != null or state.outstanding.len != 0 or (if (state.artifact_control) |ledger| ledger.current != null else false)) return error.SourcePendingWork;
                next.scope_fencing = true;
            },
            .close => {
                if (state.scope == null) return error.SourceBindingConflict;
                if (state.closing or state.retired) return state;
                next.closing = true;
            },
            .configure => |scope| {
                if (state.scope) |prior| if (!prior.sameBinding(scope) or state.retired) return error.SourceBindingConflict;
                next.scope = scope;
            },
            .bind_checkpoint => |authority| {
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (state.retired or scope.destination != .native or !std.mem.eql(u8, &scope.identity(), &authority.binding)) return error.SourceBindingConflict;
                if (state.checkpoint_authority) |bound| {
                    if (!bound.sameAuthority(authority) or bound.retirement != null) return error.SourceBindingConflict;
                    return state;
                }
                if (state.closing) return error.SourceBindingConflict;
                if (state.pending != null or state.accepted.sequence != 0 or state.outstanding.len != 0) return error.SourcePendingWork;
                if (state.artifact_control) |ledger| if (ledger.current != null) return error.SourcePendingWork;
                next.checkpoint_authority = authority;
                next.artifact_control = state.artifact_control orelse .{};
            },
            .fence_checkpoint => |fence| {
                const authority = state.checkpoint_authority orelse return error.SourceBindingConflict;
                if (!state.retired or !std.mem.eql(u8, &authority.digest(), &fence.authority_digest)) return error.SourceBindingConflict;
                if (authority.retirement) |prior| {
                    if (!std.mem.eql(u8, &prior, &fence.proof)) return error.SourcePayloadConflict;
                    return state;
                }
                if (state.pending != null or state.outstanding.len != 0) return error.SourcePendingWork;
                if (state.artifact_control) |ledger| if (ledger.current != null) return error.SourcePendingWork;
                next.checkpoint_authority.?.retirement = fence.proof;
            },
            .reseed => |scope| {
                const prior = state.scope orelse return error.SourceBindingConflict;
                if (!state.retired or scope.table_id != prior.table_id or !std.mem.eql(u8, &scope.source_id, &prior.source_id) or
                    scope.generation != (std.math.add(u64, prior.generation, 1) catch return error.SourceBindingConflict)) return error.SourceBindingConflict;
                if (state.artifact_control) |ledger| if (ledger.current != null) return error.SourcePendingWork;
                if (state.checkpoint_authority) |authority| if (authority.retirement == null) return error.SourcePendingWork;
                // Preserve the negotiated artifact contract for the same destination.
                // An explicit, fully drained destination migration renegotiates it.
                const retain_profile = state.artifact_control != null and scope.destination == prior.destination and std.mem.eql(u8, &scope.destination_binding, &prior.destination_binding);
                next = .{ .scope = scope, .revision = next.revision, .artifact_control = if (retain_profile) .{} else null, .manifest_retention = retain_profile and state.manifest_retention };
            },
            .reserve => |reservation| {
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (state.retired or reservation.destination != scope.destination) return error.SourceBindingConflict;
                if (state.pending) |pending| {
                    if (!std.mem.eql(u8, &pending.identity(scope), &reservation.identity(scope))) return error.SourcePayloadConflict;
                    return state;
                }
                if (state.closing) return error.SourceBindingConflict;
                if (state.manifest_retention and !reservation.retain_manifest) return error.UnsupportedOperation;
                if (nonzero(reservation.previous_reservation_digest) or reservation.replan_receipt.len != 0) return error.InvalidSourceControl;
                if (state.outstanding.len == max_outstanding or
                    acceptance_credit > max_outstanding_encoded_bytes - outstandingBytes(state.outstanding)) return error.SourceAdmissionBackpressure;
                if (reservation.sequence != (std.math.add(u64, state.accepted.sequence, 1) catch return error.SourceReceiptExpired)) return error.SourceReceiptExpired;
                if (!predecessor(reservation.expected_checkpoint, state.accepted)) return error.SourceCheckpointConflict;
                if (reservation.artifact) |spec| {
                    next.artifact_control = try (state.artifact_control orelse artifact_control.State{}).reserve(reservation.identity(scope), spec, scope.identity());
                } else if (state.artifact_control != null) return error.UnsupportedOperation;
                if (reservation.retain_manifest) {
                    next.artifact_control = try next.artifact_control.?.retainManifest(reservation.identity(scope), .{ .digest = reservation.plan_digest, .bytes = reservation.delivery_manifest });
                    next.manifest_retention = true;
                }
                next.pending = reservation;
            },
            .replan => |plan| {
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (state.retired) return error.SourceBindingConflict;
                const pending = state.pending orelse return error.SourceCheckpointConflict;
                if (std.mem.eql(u8, &pending.previous_reservation_digest, &plan.reservation_digest) and
                    std.mem.eql(u8, &pending.plan_digest, &plan.plan_digest) and
                    std.mem.eql(u8, &pending.transaction_id, &plan.transaction_id) and
                    std.mem.eql(u8, pending.delivery_manifest, plan.delivery_manifest) and
                    std.mem.eql(u8, pending.replan_receipt, plan.not_admitted_receipt) and std.meta.eql(pending.artifact, plan.artifact) and pending.retain_manifest == plan.retain_manifest) return state;
                if (state.closing) return error.SourceBindingConflict;
                if (state.manifest_retention and !plan.retain_manifest) return error.UnsupportedOperation;
                if (!std.mem.eql(u8, &pending.identity(scope), &plan.reservation_digest)) return error.SourcePayloadConflict;
                if (std.mem.eql(u8, &pending.transaction_id, &plan.transaction_id) or
                    std.mem.eql(u8, &pending.plan_digest, &plan.plan_digest) or
                    std.mem.eql(u8, pending.delivery_manifest, plan.delivery_manifest)) return error.InvalidSourceControl;
                if (pending.artifact != null) {
                    const ledger = state.artifact_control orelse return error.InvalidSourceControl;
                    const reclaimed = ledger.last_reclaimed orelse return error.SourcePendingWork;
                    if (ledger.current != null or !std.mem.eql(u8, &reclaimed.reservation, &plan.reservation_digest) or !std.mem.eql(u8, &reclaimed.retirement, &digest(plan.not_admitted_receipt)) or plan.artifact == null) return error.SourcePendingWork;
                } else if (state.artifact_control != null and plan.artifact == null) return error.UnsupportedOperation;
                next.pending.?.previous_reservation_digest = plan.reservation_digest;
                next.pending.?.plan_digest = plan.plan_digest;
                next.pending.?.transaction_id = plan.transaction_id;
                next.pending.?.delivery_manifest = plan.delivery_manifest;
                next.pending.?.replan_receipt = plan.not_admitted_receipt;
                next.pending.?.artifact = plan.artifact;
                next.pending.?.retain_manifest = plan.retain_manifest;
                if (plan.artifact) |spec| next.artifact_control = try (state.artifact_control orelse artifact_control.State{}).reserve(next.pending.?.identity(scope), spec, scope.identity());
                if (plan.retain_manifest) {
                    next.artifact_control = try next.artifact_control.?.retainManifest(next.pending.?.identity(scope), .{ .digest = plan.plan_digest, .bytes = plan.delivery_manifest });
                    next.manifest_retention = true;
                }
            },
            .seal_artifact => |seal| {
                const pending = state.pending orelse return error.SourcePendingWork;
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (pending.artifact == null or !std.mem.eql(u8, &pending.identity(scope), &seal.reservation_digest)) return error.SourcePayloadConflict;
                const ledger = state.artifact_control orelse return error.InvalidSourceControl;
                const sealed = try ledger.seal(seal.reservation_digest, seal.stamp);
                if (std.meta.eql(ledger, sealed)) return state;
                next.artifact_control = sealed;
            },
            .retire_artifact => |retirement| {
                const ledger = state.artifact_control orelse return error.SourcePendingWork;
                const record = ledger.current orelse return error.SourcePendingWork;
                switch (retirement.reason) {
                    .unsealed => {
                        const pending = state.pending orelse return error.SourcePendingWork;
                        if ((record.phase != .reserved and record.phase != .garbage) or record.stamp != null or !std.mem.eql(u8, &pending.identity(state.scope.?), &retirement.reservation_digest)) return error.SourcePendingWork;
                    },
                    .not_admitted => {
                        const pending = state.pending orelse return error.SourcePendingWork;
                        if (record.phase == .accepted or !std.mem.eql(u8, &pending.identity(state.scope.?), &retirement.reservation_digest)) return error.SourcePendingWork;
                    },
                    .committed => if (state.pending != null or !std.mem.eql(u8, &state.committed.reservation_digest, &retirement.reservation_digest) or (record.phase != .accepted and record.phase != .garbage)) return error.SourcePendingWork,
                }
                const retired = try ledger.retire(retirement.reservation_digest, retirement.proof);
                if (std.meta.eql(ledger, retired)) return state;
                next.artifact_control = retired;
            },
            .reclaim_artifact => |reclaimed| {
                const ledger = state.artifact_control orelse return error.SourcePendingWork;
                const released = try ledger.reclaim(reclaimed.reservation_digest, reclaimed.proof);
                if (std.meta.eql(ledger, released)) return state;
                next.artifact_control = released;
            },
            .accept => |accepted| {
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (state.retired) return error.SourceBindingConflict;
                const pending = state.pending orelse {
                    if (std.mem.eql(u8, &state.accepted.receipt_digest, &digest(accepted.receipt)) and std.mem.eql(u8, &state.accepted.reservation_digest, &accepted.reservation_digest)) {
                        if (accepted.commit_reference) |ref| return (Mutation{ .commit = .{ .sequence = state.accepted.sequence, .receipt_digest = state.accepted.receipt_digest, .reference = ref } }).apply(a, state);
                        return state;
                    }
                    return error.SourceCheckpointConflict;
                };
                const id = pending.identity(scope);
                if (!std.mem.eql(u8, &id, &accepted.reservation_digest)) return error.SourcePayloadConflict;
                if (pending.artifact != null) next.artifact_control = try (state.artifact_control orelse return error.InvalidSourceControl).accept(accepted.reservation_digest);
                const outstanding = try a.alloc(Accepted, state.outstanding.len + 1);
                @memcpy(outstanding[0..state.outstanding.len], state.outstanding);
                outstanding[state.outstanding.len] = .{
                    .sequence = pending.sequence,
                    .checkpoint = pending.checkpoint,
                    .reservation_digest = id,
                    .payload_digest = pending.payload_digest,
                    .receipt = accepted.receipt,
                };
                next.outstanding = outstanding;
                next.accepted = .{ .sequence = pending.sequence, .checkpoint = pending.checkpoint, .receipt_digest = digest(accepted.receipt), .reservation_digest = id, .reference = accepted.receipt };
                next.pending = null;
                if (accepted.commit_reference) |ref| {
                    const revision = next.revision;
                    next = try (Mutation{ .commit = .{ .sequence = pending.sequence, .receipt_digest = next.accepted.receipt_digest, .reference = ref } }).apply(a, next);
                    next.revision = revision;
                }
            },
            .commit => |committed| {
                if (committed.sequence == state.committed.sequence) {
                    if (!std.mem.eql(u8, &state.committed.receipt_digest, &committed.receipt_digest) or !std.mem.eql(u8, state.committed.reference, committed.reference)) return error.SourcePayloadConflict;
                    return state;
                }
                if (committed.sequence < state.committed.sequence) return error.SourceReceiptExpired;
                const index = for (state.outstanding, 0..) |entry, i| {
                    if (entry.sequence == committed.sequence) break i;
                } else return error.SourceCheckpointConflict;
                const accepted = state.outstanding[index];
                if (!std.mem.eql(u8, &digest(accepted.receipt), &committed.receipt_digest)) return error.SourcePayloadConflict;
                next.committed = .{ .sequence = accepted.sequence, .checkpoint = accepted.checkpoint, .receipt_digest = committed.receipt_digest, .reservation_digest = accepted.reservation_digest, .reference = committed.reference };
                next.outstanding = state.outstanding[index + 1 ..];
            },
            .reject_and_retire => |retirement| {
                if (state.scope_fencing and !state.closing) return error.SourcePendingWork;
                const scope = state.scope orelse return error.SourceBindingConflict;
                if (state.outstanding.len != 0) return error.SourcePendingWork;
                if (state.retired) {
                    if (!std.mem.eql(u8, &state.retired_reservation_digest, &retirement.reservation_digest) or
                        !std.mem.eql(u8, state.retirement_receipt, retirement.receipt)) return error.SourcePayloadConflict;
                    return state;
                }
                const pending = state.pending orelse return error.SourceCheckpointConflict;
                if (!std.mem.eql(u8, &pending.identity(scope), &retirement.reservation_digest)) return error.SourcePayloadConflict;
                if (pending.artifact != null) {
                    const ledger = state.artifact_control orelse return error.InvalidSourceControl;
                    const reclaimed = ledger.last_reclaimed orelse return error.SourcePendingWork;
                    if (ledger.current != null or !std.mem.eql(u8, &reclaimed.reservation, &retirement.reservation_digest) or !std.mem.eql(u8, &reclaimed.retirement, &digest(retirement.receipt))) return error.SourcePendingWork;
                }
                next.pending = null;
                next.retired = true;
                next.retired_reservation_digest = retirement.reservation_digest;
                next.retirement_receipt = retirement.receipt;
            },
            .retire => |retirement| {
                if (state.scope_fencing and !state.closing) return error.SourcePendingWork;
                if (state.pending != null or state.outstanding.len != 0 or (if (state.artifact_control) |ledger| ledger.current != null else false)) return error.SourcePendingWork;
                if (state.scope == null) return error.SourceBindingConflict;
                if (state.retired) {
                    if (!std.mem.eql(u8, state.retirement_receipt, retirement.receipt) or nonzero(state.retired_reservation_digest)) return error.SourcePayloadConflict;
                    return state;
                }
                next.retired = true;
                next.retirement_receipt = retirement.receipt;
            },
        }
        try next.validate();
        return next;
    }
};
pub const Write = struct {
    table_id: u64,
    source_id: Digest,
    expected_revision: u64,
    expected_binding: Digest,
    /// CAS against current catalog, in addition to immutable source identity.
    expected_catalog_digest: Digest,
    mutation: Mutation,
    pub fn validate(self: Write) !void {
        if (self.table_id == 0 or !nonzero(self.source_id) or !nonzero(self.expected_catalog_digest) or !nonzero(self.expected_binding)) return error.InvalidSourceControl;
        try self.mutation.validate();
        switch (self.mutation) {
            .configure, .reseed => |scope| if (scope.table_id != self.table_id or !std.mem.eql(u8, &scope.source_id, &self.source_id)) return error.SourceBindingConflict,
            else => {},
        }
    }
};
pub fn encode(a: A, state: State) ![]u8 {
    try state.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, state, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_state_bytes) return error.InvalidSourceControl;
    return bytes;
}
pub fn parse(a: A, bytes: []const u8) !std.json.Parsed(State) {
    if (bytes.len == 0 or bytes.len > max_state_bytes) return error.SourceControlCorrupt;
    var parsed = std.json.parseFromSlice(State, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.SourceControlCorrupt,
    };
    errdefer parsed.deinit();
    parsed.value.validate() catch return error.SourceControlCorrupt;
    return parsed;
}
pub fn encodeWrite(a: A, write: Write) ![]u8 {
    try write.validate();
    const bytes = try std.json.Stringify.valueAlloc(a, write, .{});
    errdefer a.free(bytes);
    if (bytes.len > max_state_bytes) return error.InvalidSourceControl;
    return bytes;
}
pub fn parseWrite(a: A, bytes: []const u8) !std.json.Parsed(Write) {
    if (bytes.len == 0 or bytes.len > max_state_bytes) return error.InvalidSourceControl;
    var parsed = std.json.parseFromSlice(Write, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidSourceControl,
    };
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

test "source control persists pending identity and separates acceptance from authoritative commitment" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .writable_iceberg, .destination_binding = @splat(7) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    var pending: Reservation = .{
        .sequence = 1,
        .batch_id = "one",
        .checkpoint = "same-opaque-value",
        .accepted_at_ns = 100,
        .payload_digest = @splat(4),
        .plan_digest = @splat(5),
        .transaction_id = @splat(6),
        .destination = .writable_iceberg,
        .delivery_manifest = "{\"catalog\":\"binding\"}",
    };
    state = try (Mutation{ .reserve = pending }).apply(ca, state);
    const saved = try encode(a, state);
    defer a.free(saved);
    var restored = try parse(a, saved);
    defer restored.deinit();
    try std.testing.expectEqual(@as(u64, 0), restored.value.accepted.sequence);
    var changed = pending;
    changed.payload_digest = @splat(9);
    try std.testing.expectError(error.SourcePayloadConflict, (Mutation{ .reserve = changed }).apply(ca, restored.value));
    state = try (Mutation{ .accept = .{ .reservation_digest = pending.identity(scope), .receipt = "wal-accepted-one" } }).apply(ca, restored.value);
    try std.testing.expectEqual(@as(u64, 1), state.accepted.sequence);
    try std.testing.expectEqual(@as(u64, 0), state.committed.sequence);
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .retire = .{ .receipt = "retired" } }).apply(ca, state));
    pending.sequence = 2;
    pending.batch_id = "two";
    pending.expected_checkpoint = "same-opaque-value";
    pending.accepted_at_ns = 101;
    state = try (Mutation{ .reserve = pending }).apply(ca, state);
    state = try (Mutation{ .accept = .{ .reservation_digest = pending.identity(scope), .receipt = "wal-accepted-two" } }).apply(ca, state);
    try std.testing.expectEqual(@as(usize, 2), state.outstanding.len);
    try std.testing.expectError(error.SourcePayloadConflict, (Mutation{ .commit = .{ .sequence = 2, .receipt_digest = digest("wrong"), .reference = "snapshot-9" } }).apply(ca, state));
    state = try (Mutation{ .commit = .{ .sequence = 1, .receipt_digest = digest("wal-accepted-one"), .reference = "snapshot-8" } }).apply(ca, state);
    try std.testing.expectEqual(@as(u64, 2), state.accepted.sequence);
    try std.testing.expectEqual(@as(u64, 1), state.committed.sequence);
    state = try (Mutation{ .commit = .{ .sequence = 2, .receipt_digest = digest("wal-accepted-two"), .reference = "snapshot-9" } }).apply(ca, state);
    try std.testing.expectEqual(@as(usize, 0), state.outstanding.len);
    try std.testing.expectError(error.SourceReceiptExpired, (Mutation{ .reserve = pending }).apply(ca, state));
    state = try (Mutation{ .retire = .{ .receipt = "destination-fenced" } }).apply(ca, state);
    try std.testing.expectError(error.SourceBindingConflict, (Mutation{ .reserve = pending }).apply(ca, state));
    var replacement = scope;
    replacement.generation = 2;
    replacement.provider = @splat(8);
    state = try (Mutation{ .reseed = replacement }).apply(ca, state);
    try std.testing.expectEqual(@as(u64, 0), state.accepted.sequence);
    try std.testing.expectEqual(@as(u64, 2), state.scope.?.generation);
}

test "source control bounds accepted backlog without weakening original pending reservations" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{ .table_id = 1, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .writable_iceberg, .destination_binding = @splat(7) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    for (0..max_outstanding) |i| {
        const reservation: Reservation = .{
            .sequence = i + 1,
            .batch_id = try std.fmt.allocPrint(ca, "b-{d}", .{i}),
            .checkpoint = "opaque",
            .expected_checkpoint = if (i == 0) null else "opaque",
            .accepted_at_ns = i + 1,
            .payload_digest = @splat(4),
            .plan_digest = @splat(5),
            .transaction_id = @splat(6),
            .destination = .writable_iceberg,
            .delivery_manifest = "{}",
        };
        state = try (Mutation{ .reserve = reservation }).apply(ca, state);
        state = try (Mutation{ .accept = .{ .reservation_digest = reservation.identity(scope), .receipt = try std.fmt.allocPrint(ca, "receipt-{d}", .{i}) } }).apply(ca, state);
    }
    const extra: Reservation = .{
        .sequence = max_outstanding + 1,
        .batch_id = "blocked",
        .checkpoint = "next",
        .expected_checkpoint = "opaque",
        .accepted_at_ns = 999,
        .payload_digest = @splat(4),
        .plan_digest = @splat(5),
        .transaction_id = @splat(6),
        .destination = .writable_iceberg,
        .delivery_manifest = "{}",
    };
    try std.testing.expectError(error.SourceAdmissionBackpressure, (Mutation{ .reserve = extra }).apply(ca, state));
    state = try (Mutation{ .commit = .{ .sequence = max_outstanding, .receipt_digest = digest("receipt-255"), .reference = "snapshot" } }).apply(ca, state);
    state = try (Mutation{ .reserve = extra }).apply(ca, state);
    try std.testing.expect(state.pending != null);
}

fn hashField(hash: *std.crypto.hash.sha2.Sha256, bytes: []const u8) void {
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, bytes.len, .little);
    hash.update(&length);
    hash.update(bytes);
}
fn hashJson(a: A, hash: *std.crypto.hash.sha2.Sha256, value: std.json.Value) anyerror!void {
    hashField(hash, @tagName(value));
    switch (value) {
        .object => |object| {
            const names = try a.dupe([]const u8, object.keys());
            defer a.free(names);
            std.mem.sort([]const u8, names, {}, struct {
                fn less(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.lessThan(u8, left, right);
                }
            }.less);
            for (names) |name| {
                hashField(hash, name);
                try hashJson(a, hash, object.get(name).?);
            }
        },
        .array => |array| for (array.items) |item| try hashJson(a, hash, item),
        else => {
            const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
            defer a.free(bytes);
            hashField(hash, bytes);
        },
    }
    hashField(hash, "end");
}
/// Formatting and object-field order do not change a logical source definition.
/// Ordered route/transform arrays and numeric spellings remain significant.
pub fn definitionDigest(a: A, sources_json: []const u8, ordinal: u32) !Digest {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, sources_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .array or ordinal >= parsed.value.array.items.len or parsed.value.array.items[ordinal] != .object) return error.InvalidSourceControl;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:source-definition:v1");
    try hashJson(a, &hash, parsed.value.array.items[ordinal]);
    return hash.finalResult();
}
pub fn hasWork(state: State) bool {
    return (state.closing and !state.retired) or (state.scope_fencing and if (state.checkpoint_authority) |authority| authority.retirement == null else true) or state.pending != null or state.outstanding.len != 0 or (if (state.artifact_control) |ledger| ledger.current != null else false) or (state.retired and if (state.checkpoint_authority) |authority| authority.retirement == null else false);
}
pub fn validateEncodedWrite(bytes: []const u8) !void {
    var parsed = try parseWrite(std.heap.page_allocator, bytes);
    defer parsed.deinit();
}
pub fn prefixForGroup(buffer: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:source_control:{d}:", .{group_id});
}
pub fn keyForGroup(buffer: []u8, group_id: u64, table_id: u64, source_id: Digest) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:source_control:{d}:state:{d}:{s}", .{ group_id, table_id, std.fmt.bytesToHex(source_id, .lower) });
}
pub fn workKey(buffer: []u8, group_id: u64, table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:source_control:{d}:work:{d}", .{ group_id, table_id });
}

pub fn readOnService(svc: anytype, a: A, table_id: u64, source_id: Digest, request: anytype) ![]u8 {
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.getSourceControl(a, svc.metadata_group_id, table_id, source_id);
}
pub fn mutateOnService(svc: anytype, a: A, write: Write, request: anytype) !void {
    try write.validate();
    try request.ensureActive();
    _ = try svc.ensureTableTopologyProtocolReadyWithContext(request, requiredProtocol(write.mutation));
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const bytes = try store.getSourceControl(a, svc.metadata_group_id, write.table_id, write.source_id);
    defer a.free(bytes);
    var before = try parse(a, bytes);
    defer before.deinit();
    if (before.value.revision != write.expected_revision) return error.SourceControlConflict;
    if (before.value.scope) |scope| {
        if (!std.mem.eql(u8, &scope.identity(), &write.expected_binding)) return error.SourceBindingConflict;
    } else {
        if (write.mutation != .configure or !std.mem.eql(u8, &write.mutation.configure.identity(), &write.expected_binding)) return error.SourceBindingConflict;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const next = try write.mutation.apply(arena.allocator(), before.value);
    const expected = try encode(arena.allocator(), next);
    const command = try encodeWrite(arena.allocator(), write);
    try request.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_source_control = command });
    svc.waitForTransitionAppliedWithContext(receipt, request) catch return error.MetadataMutationOutcomeUnknown;
    const observed = try store.getSourceControl(a, svc.metadata_group_id, write.table_id, write.source_id);
    defer a.free(observed);
    if (!std.mem.eql(u8, expected, observed)) return error.MetadataMutationOutcomeUnknown;
}

test "source control proposal acknowledgments never grant unobserved progress or bypass decoder fencing" {
    const a = std.testing.allocator;
    const Context = struct {
        pub fn ensureActive(_: @This()) !void {}
    };
    const Service = struct {
        a: A,
        value: []u8,
        metadata_group_id: u64 = 1,
        blocked: bool = true,
        lose_ack: bool = false,
        reject_proposal: bool = false,
        proposals: usize = 0,
        pub fn ensureTableTopologyProtocolReadyWithContext(self: *@This(), _: Context, required: u16) !u8 {
            try std.testing.expectEqual(protocol_version, required);
            if (self.blocked) return error.TableTopologyProtocolUpgradeRequired;
            return 0;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
        pub fn projectedStore(self: *@This()) ?*@This() {
            return self;
        }
        pub fn getSourceControl(self: *@This(), allocator: A, _: u64, _: u64, _: Digest) ![]u8 {
            return allocator.dupe(u8, self.value);
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
            self.proposals += 1;
            if (!self.reject_proposal) {
                var write = try parseWrite(self.a, command.apply_source_control);
                defer write.deinit();
                var current = try parse(self.a, self.value);
                defer current.deinit();
                var arena = std.heap.ArenaAllocator.init(self.a);
                defer arena.deinit();
                const output = try encode(self.a, try write.value.mutation.apply(arena.allocator(), current.value));
                self.a.free(self.value);
                self.value = output;
            }
            return self.proposals;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Context) !void {
            if (self.lose_ack) return error.TestLostAcknowledgment;
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scope: Scope = .{
        .table_id = 7,
        .source_id = @splat(1),
        .source_ordinal = 0,
        .generation = 1,
        .definition = @splat(2),
        .provider = @splat(3),
        .destination = .native,
        .destination_binding = @splat(7),
    };
    var service: Service = .{ .a = a, .value = try encode(a, try (Mutation{ .configure = scope }).apply(arena.allocator(), .{})) };
    defer a.free(service.value);
    const reservation: Reservation = .{
        .sequence = 1,
        .batch_id = "same-original",
        .checkpoint = "cp",
        .accepted_at_ns = 123,
        .payload_digest = @splat(4),
        .plan_digest = @splat(5),
        .transaction_id = @splat(6),
        .destination = .native,
        .delivery_manifest = "{}",
    };
    var write: Write = .{
        .table_id = 7,
        .source_id = scope.source_id,
        .expected_revision = 1,
        .expected_binding = scope.identity(),
        .expected_catalog_digest = @splat(9),
        .mutation = .{ .reserve = reservation },
    };
    try std.testing.expectError(error.TableTopologyProtocolUpgradeRequired, mutateOnService(&service, a, write, Context{}));
    try std.testing.expectEqual(@as(usize, 0), service.proposals);
    service.blocked = false;
    service.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, mutateOnService(&service, a, write, Context{}));
    const recovered_bytes = try readOnService(&service, a, 7, scope.source_id, Context{});
    defer a.free(recovered_bytes);
    var recovered = try parse(a, recovered_bytes);
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u64, 123), recovered.value.pending.?.accepted_at_ns);
    try std.testing.expectEqual(reservation.identity(scope), recovered.value.pending.?.identity(scope));
    service.lose_ack = false;
    write.expected_revision = recovered.value.revision;
    try mutateOnService(&service, a, write, Context{});
    service.reject_proposal = true;
    write.mutation = .{ .accept = .{ .reservation_digest = reservation.identity(scope), .receipt = "native-accepted", .commit_reference = "native-commit" } };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, mutateOnService(&service, a, write, Context{}));
    service.reject_proposal = false;
    try mutateOnService(&service, a, write, Context{});
    var committed = try parse(a, service.value);
    defer committed.deinit();
    try std.testing.expectEqual(@as(u64, 1), committed.value.accepted.sequence);
    try std.testing.expectEqual(@as(u64, 1), committed.value.committed.sequence);
}

test "source control canonical definitions and stored corruption cannot forge progress" {
    const a = std.testing.allocator;
    const first = try definitionDigest(a, "[{\"type\":\"postgres\",\"key_template\":\"id\"}]", 0);
    const reordered = try definitionDigest(a, "[ {\"key_template\":\"id\",\"type\":\"postgres\"} ]", 0);
    try std.testing.expectEqual(first, reordered);
    const changed = try definitionDigest(a, "[{\"key_template\":\"other\",\"type\":\"postgres\"}]", 0);
    try std.testing.expect(!std.mem.eql(u8, &first, &changed));
    try std.testing.expectError(error.SourceControlCorrupt, parse(a, "{\"revision\":4}"));
    try std.testing.expectError(error.SourceControlCorrupt, parse(a, "{"));
    try std.testing.expectError(error.InvalidSourceControl, parseWrite(a, "{"));
}

test "source control reserves worst case receipt bytes before admitting destination work" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{
        .table_id = 1,
        .source_id = @splat(1),
        .source_ordinal = 0,
        .generation = 1,
        .definition = @splat(2),
        .provider = @splat(3),
        .destination = .writable_iceberg,
        .destination_binding = @splat(7),
    };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const receipt: [8192]u8 = @splat(1);
    var accepted: usize = 0;
    while (true) {
        const reservation: Reservation = .{
            .sequence = accepted + 1,
            .batch_id = try std.fmt.allocPrint(ca, "b-{d}", .{accepted}),
            .checkpoint = "cp",
            .expected_checkpoint = if (accepted == 0) null else "cp",
            .accepted_at_ns = accepted + 1,
            .payload_digest = @splat(4),
            .plan_digest = @splat(5),
            .transaction_id = @splat(6),
            .destination = .writable_iceberg,
            .delivery_manifest = "{}",
        };
        const reserved = (Mutation{ .reserve = reservation }).apply(ca, state) catch |err| {
            try std.testing.expectEqual(error.SourceAdmissionBackpressure, err);
            break;
        };
        state = try (Mutation{ .accept = .{ .reservation_digest = reservation.identity(scope), .receipt = &receipt } }).apply(ca, reserved);
        const encoded = try encode(a, state);
        a.free(encoded);
        accepted += 1;
    }
    try std.testing.expect(accepted > 0 and accepted < max_outstanding);
    try std.testing.expect(state.pending == null);
    state = try (Mutation{ .commit = .{ .sequence = accepted, .receipt_digest = digest(&receipt), .reference = "catalog-committed" } }).apply(ca, state);
    try std.testing.expect(!hasWork(state));
}

test "source control retires rejected pending attempts only with their original identity" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{
        .table_id = 1,
        .source_id = @splat(1),
        .source_ordinal = 0,
        .generation = 1,
        .definition = @splat(2),
        .provider = @splat(3),
        .destination = .native,
        .destination_binding = @splat(7),
    };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const reservation: Reservation = .{
        .sequence = 1,
        .batch_id = "rejected",
        .checkpoint = "cp",
        .accepted_at_ns = 100,
        .payload_digest = @splat(4),
        .plan_digest = @splat(5),
        .transaction_id = @splat(6),
        .destination = .native,
        .delivery_manifest = "{\"original_route\":1}",
    };
    state = try (Mutation{ .reserve = reservation }).apply(ca, state);
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .retire = .{ .receipt = "not-enough" } }).apply(ca, state));
    try std.testing.expectError(error.SourcePayloadConflict, (Mutation{ .reject_and_retire = .{ .reservation_digest = @splat(9), .receipt = "fenced" } }).apply(ca, state));
    state = try (Mutation{ .reject_and_retire = .{ .reservation_digest = reservation.identity(scope), .receipt = "fenced-and-retired" } }).apply(ca, state);
    try std.testing.expect(!hasWork(state) and state.retired);
    try std.testing.expectEqual(reservation.identity(scope), state.retired_reservation_digest);
    try std.testing.expectEqual(@as(u64, 0), state.accepted.sequence);
    try std.testing.expectError(error.SourceBindingConflict, (Mutation{ .accept = .{ .reservation_digest = reservation.identity(scope), .receipt = "late-acceptance" } }).apply(ca, state));
}

test "source control replans only proven unadmitted delivery while retaining logical source identity" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{
        .table_id = 1,
        .source_id = @splat(1),
        .source_ordinal = 0,
        .generation = 1,
        .definition = @splat(2),
        .provider = @splat(3),
        .destination = .native,
        .destination_binding = @splat(7),
    };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const reservation: Reservation = .{
        .sequence = 1,
        .batch_id = "same-source-commit",
        .checkpoint = "opaque",
        .accepted_at_ns = 123,
        .payload_digest = @splat(4),
        .plan_digest = @splat(5),
        .transaction_id = @splat(6),
        .destination = .native,
        .delivery_manifest = "{\"epoch\":1}",
    };
    state = try (Mutation{ .reserve = reservation }).apply(ca, state);
    const changed: Mutation = .{ .replan = .{
        .reservation_digest = reservation.identity(scope),
        .plan_digest = @splat(8),
        .transaction_id = @splat(9),
        .delivery_manifest = "{\"epoch\":2}",
        .not_admitted_receipt = "old-epoch-fenced-and-no-admission",
    } };
    state = try changed.apply(ca, state);
    try std.testing.expectEqual(@as(u64, 0), state.accepted.sequence);
    try std.testing.expectEqual(reservation.payload_digest, state.pending.?.payload_digest);
    try std.testing.expectEqual(reservation.accepted_at_ns, state.pending.?.accepted_at_ns);
    try std.testing.expectEqualStrings(reservation.batch_id, state.pending.?.batch_id);
    try std.testing.expectEqualStrings(reservation.checkpoint, state.pending.?.checkpoint);
    try std.testing.expectEqual(state.revision, (try changed.apply(ca, state)).revision);
    try std.testing.expectError(error.SourcePayloadConflict, (Mutation{ .accept = .{ .reservation_digest = reservation.identity(scope), .receipt = "late-old-attempt" } }).apply(ca, state));
    const current = state.pending.?.identity(scope);
    state = try (Mutation{ .accept = .{ .reservation_digest = current, .receipt = "new-route-accepted", .commit_reference = "new-route-committed" } }).apply(ca, state);
    try std.testing.expectEqual(@as(u64, 1), state.committed.sequence);
}

fn writeArtifactFields(value: anytype, stream: anytype) @TypeOf(stream.*).Error!void {
    try stream.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |name| {
        const include = if (comptime std.mem.eql(u8, name, "closing") or std.mem.eql(u8, name, "scope_fencing") or std.mem.eql(u8, name, "manifest_retention") or std.mem.eql(u8, name, "retain_manifest")) @field(value, name) else if (comptime std.mem.eql(u8, name, "artifact") or std.mem.eql(u8, name, "artifact_control") or std.mem.eql(u8, name, "checkpoint_authority")) @field(value, name) != null else true;
        if (include) {
            try stream.objectField(name);
            try stream.write(@field(value, name));
        }
    }
    try stream.endObject();
}
pub fn requiredProtocol(mutation: Mutation) u16 {
    return switch (mutation) {
        .close, .enable_scope_fencing => closing_protocol_version,
        .bind_checkpoint, .fence_checkpoint => checkpoint_authority.protocol_version,
        .reserve => |reservation| if (reservation.retain_manifest) manifest_protocol_version else if (reservation.artifact != null) artifact_control.protocol_version else protocol_version,
        .replan => |plan| if (plan.retain_manifest) manifest_protocol_version else if (plan.artifact != null) artifact_control.protocol_version else protocol_version,
        .seal_artifact, .retire_artifact, .reclaim_artifact => artifact_control.protocol_version,
        else => protocol_version,
    };
}
pub fn requiredProtocolEncoded(bytes: []const u8) u16 {
    var write = parseWrite(std.heap.page_allocator, bytes) catch return manifest_protocol_version;
    defer write.deinit();
    switch (write.value.mutation) {
        .reserve, .replan => {
            // Presence is a decoder requirement even for an explicit false.
            // Parse field names so escaped keys cannot bypass negotiation.
            var json = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, bytes, .{ .duplicate_field_behavior = .@"error" }) catch return manifest_protocol_version;
            defer json.deinit();
            const mutation = json.value.object.get("mutation").?.object;
            const payload = mutation.get(@tagName(write.value.mutation)).?;
            if (payload.object.contains("retain_manifest")) return manifest_protocol_version;
        },
        else => {},
    }
    return requiredProtocol(write.value.mutation);
}

fn artifactTestSpec(scope: Scope, token: u64) artifact_control.Spec {
    var attempt: [16]u8 = @splat(1);
    std.mem.writeInt(u64, attempt[0..8], token, .big);
    return .{ .namespace = @splat(9), .binding = scope.identity(), .domain = artifact_control.domainFor(scope.identity()), .attempt = attempt, .bytes = 1024, .checksum = @splat(10), .plan_digest = @splat(11) };
}

test "source control artifact quota remains charged through acceptance and returns only after reclamation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const legacy = try encode(ca, state);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "artifact_control") == null);
    var reservation: Reservation = .{ .sequence = 1, .batch_id = "batch", .checkpoint = "cp1", .accepted_at_ns = 1, .payload_digest = @splat(5), .plan_digest = @splat(6), .transaction_id = @splat(7), .destination = .native, .delivery_manifest = "frozen-reference" };
    const legacy_id = reservation.identity(scope);
    const old_write: Write = .{ .table_id = 7, .source_id = scope.source_id, .expected_revision = state.revision, .expected_binding = scope.identity(), .expected_catalog_digest = @splat(8), .mutation = .{ .reserve = reservation } };
    const old_bytes = try encodeWrite(ca, old_write);
    try std.testing.expect(std.mem.indexOf(u8, old_bytes, "artifact") == null);
    try std.testing.expectEqual(protocol_version, requiredProtocolEncoded(old_bytes));
    reservation.artifact = artifactTestSpec(scope, 1);
    try std.testing.expect(!std.mem.eql(u8, &legacy_id, &reservation.identity(scope)));
    var modern_write = old_write;
    modern_write.mutation = .{ .reserve = reservation };
    const modern_bytes = try encodeWrite(ca, modern_write);
    try std.testing.expectEqual(artifact_control.protocol_version, requiredProtocolEncoded(modern_bytes));
    const escaped = try std.mem.replaceOwned(u8, ca, modern_bytes, "\"artifact\"", "\"arti\\u0066act\"");
    try std.testing.expectEqual(artifact_control.protocol_version, requiredProtocolEncoded(escaped));
    const duplicate = try std.mem.replaceOwned(u8, ca, modern_bytes, "\"artifact\":", "\"artifact\":null,\"artifact\":");
    try std.testing.expectError(error.InvalidSourceControl, parseWrite(ca, duplicate));
    state = try modern_write.mutation.apply(ca, state);
    const id = reservation.identity(scope);
    try std.testing.expectEqual(@as(u64, 1024), state.artifact_control.?.chargedBytes());
    try std.testing.expect(hasWork(state));
    const accept: Mutation = .{ .accept = .{ .reservation_digest = id, .receipt = "native-accepted", .commit_reference = "native-commit" } };
    try std.testing.expectError(error.SourcePendingWork, accept.apply(ca, state));
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .reclaim_artifact = .{ .reservation_digest = id, .proof = @splat(12) } }).apply(ca, state));
    const seal: Mutation = .{ .seal_artifact = .{ .reservation_digest = id, .stamp = @splat(13) } };
    state = try seal.apply(ca, state);
    try std.testing.expectEqual(state.revision, (try seal.apply(ca, state)).revision);
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .retire_artifact = .{ .reservation_digest = id, .proof = @splat(14), .reason = .unsealed } }).apply(ca, state));
    const saved = try encode(ca, state);
    var reopened = try parse(a, saved);
    defer reopened.deinit();
    try std.testing.expectEqual(.sealed, reopened.value.artifact_control.?.current.?.phase);
    try std.testing.expectEqual(@as(u64, 1024), reopened.value.artifact_control.?.chargedBytes());
    state = try accept.apply(ca, state);
    try std.testing.expectEqual(@as(u64, 1), state.committed.sequence);
    try std.testing.expectEqual(@as(usize, 0), state.outstanding.len);
    try std.testing.expect(hasWork(state));
    try std.testing.expectEqual(@as(u64, 1024), state.artifact_control.?.chargedBytes());
    var next_reservation = reservation;
    next_reservation.sequence = 2;
    next_reservation.batch_id = "batch2";
    next_reservation.expected_checkpoint = "cp1";
    next_reservation.checkpoint = "cp2";
    next_reservation.artifact = artifactTestSpec(scope, 2);
    try std.testing.expectError(error.SourceAdmissionBackpressure, (Mutation{ .reserve = next_reservation }).apply(ca, state));
    const retire: Mutation = .{ .retire_artifact = .{ .reservation_digest = id, .proof = @splat(14), .reason = .committed } };
    state = try retire.apply(ca, state);
    try std.testing.expectEqual(@as(u64, 1024), state.artifact_control.?.chargedBytes());
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .retire = .{ .receipt = "source-closed" } }).apply(ca, state));
    const reclaim: Mutation = .{ .reclaim_artifact = .{ .reservation_digest = id, .proof = @splat(15) } };
    state = try reclaim.apply(ca, state);
    try std.testing.expectEqual(@as(u64, 0), state.artifact_control.?.chargedBytes());
    try std.testing.expect(!hasWork(state));
    try std.testing.expectEqual(state.revision, (try reclaim.apply(ca, state)).revision);
    next_reservation.artifact = null;
    try std.testing.expectError(error.UnsupportedOperation, (Mutation{ .reserve = next_reservation }).apply(ca, state));
    next_reservation.artifact = artifactTestSpec(scope, 1);
    try std.testing.expectError(error.SourceReceiptExpired, (Mutation{ .reserve = next_reservation }).apply(ca, state));
    next_reservation.artifact = artifactTestSpec(scope, 2);
    state = try (Mutation{ .reserve = next_reservation }).apply(ca, state);
    // A late proof for the previous allocation cannot release current quota.
    state = try reclaim.apply(ca, state);
    try std.testing.expectEqual(@as(u64, 1024), state.artifact_control.?.chargedBytes());
    var corrupt = state;
    corrupt.artifact_control.?.current.?.spec.checksum[0] ^= 1;
    try std.testing.expectError(error.InvalidSourceControl, corrupt.validate());
}

test "source control artifact replanning requires fenced cleanup and never reuses upload authority" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const reservation: Reservation = .{ .sequence = 1, .batch_id = "batch", .checkpoint = "cp1", .accepted_at_ns = 1, .payload_digest = @splat(5), .plan_digest = @splat(6), .transaction_id = @splat(7), .destination = .native, .delivery_manifest = "original", .artifact = artifactTestSpec(scope, 1) };
    state = try (Mutation{ .reserve = reservation }).apply(ca, state);
    const id = reservation.identity(scope);
    const logical_input = try reservation.inputFingerprint(scope);
    const replacement: Mutation = .{ .replan = .{ .reservation_digest = id, .plan_digest = @splat(8), .transaction_id = @splat(9), .delivery_manifest = "replacement", .not_admitted_receipt = "fenced-original", .artifact = artifactTestSpec(scope, 2) } };
    try std.testing.expectError(error.SourcePendingWork, replacement.apply(ca, state));
    state = try (Mutation{ .retire_artifact = .{ .reservation_digest = id, .proof = digest("fenced-original"), .reason = .unsealed } }).apply(ca, state);
    try std.testing.expectError(error.SourcePendingWork, replacement.apply(ca, state));
    state = try (Mutation{ .reclaim_artifact = .{ .reservation_digest = id, .proof = @splat(10) } }).apply(ca, state);
    var wrong = replacement;
    wrong.replan.not_admitted_receipt = "different-proof";
    try std.testing.expectError(error.SourcePendingWork, wrong.apply(ca, state));
    state = try replacement.apply(ca, state);
    try std.testing.expectEqual(logical_input, try state.pending.?.inputFingerprint(scope));
    try std.testing.expectEqual(@as(u64, 1), state.pending.?.sequence);
    try std.testing.expectEqual(@as(u64, 2), state.artifact_control.?.current.?.spec.token());
    try std.testing.expectEqual(@as(u64, 0), state.accepted.sequence);
    try std.testing.expectError(error.SourcePayloadConflict, (Mutation{ .seal_artifact = .{ .reservation_digest = id, .stamp = @splat(13) } }).apply(ca, state));
    const replacement_id = state.pending.?.identity(scope);
    state = try (Mutation{ .retire_artifact = .{ .reservation_digest = replacement_id, .proof = digest("fenced-replacement"), .reason = .unsealed } }).apply(ca, state);
    state = try (Mutation{ .reclaim_artifact = .{ .reservation_digest = replacement_id, .proof = @splat(11) } }).apply(ca, state);
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .reject_and_retire = .{ .reservation_digest = replacement_id, .receipt = "different-proof" } }).apply(ca, state));
    state = try (Mutation{ .reject_and_retire = .{ .reservation_digest = replacement_id, .receipt = "fenced-replacement" } }).apply(ca, state);
    try std.testing.expect(state.retired and !hasWork(state));
    const retired = state;
    scope.generation += 1;
    state = try (Mutation{ .reseed = scope }).apply(ca, state);
    var fresh = reservation;
    fresh.artifact = null;
    try std.testing.expectError(error.UnsupportedOperation, (Mutation{ .reserve = fresh }).apply(ca, state));
    fresh.artifact = artifactTestSpec(scope, 1);
    state = try (Mutation{ .reserve = fresh }).apply(ca, state);
    try std.testing.expectEqual(@as(u64, 1), state.artifact_control.?.next_token);
    try std.testing.expect(!std.mem.eql(u8, &reservation.artifact.?.domain, &state.pending.?.artifact.?.domain));
    var lake_scope = scope;
    lake_scope.destination = .writable_iceberg;
    lake_scope.destination_binding = @splat(19);
    const migrated = try (Mutation{ .reseed = lake_scope }).apply(ca, retired);
    try std.testing.expect(migrated.artifact_control == null);
    var lake_reservation = reservation;
    lake_reservation.destination = .writable_iceberg;
    lake_reservation.artifact = null;
    _ = try (Mutation{ .reserve = lake_reservation }).apply(ca, migrated);
}

test "source checkpoint authority is immutable and retirement debt blocks reseeding" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const authority: checkpoint_authority.Spec = .{ .binding = scope.identity(), .table_name = "checkpoint-owner", .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(5), .root_generation = 0, .group_id = 17, .topology_epoch = 1 } };
    const bind: Mutation = .{ .bind_checkpoint = authority };
    var refreshed = authority.owner;
    refreshed.topology_epoch += 1;
    refreshed.table_binding = @splat(6);
    try authority.requireOwner(refreshed);
    refreshed.root_generation += 1;
    try std.testing.expectError(error.SourceBindingConflict, authority.requireOwner(refreshed));
    const legacy = try encode(a, state);
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "checkpoint_authority") == null);
    state = try bind.apply(ca, state);
    try std.testing.expect(state.artifact_control != null);
    try std.testing.expect(!hasWork(state));
    try std.testing.expectEqual(state.revision, (try bind.apply(ca, state)).revision);
    var replaced = authority;
    replaced.owner.group_id += 1;
    try std.testing.expectError(error.SourceBindingConflict, (Mutation{ .bind_checkpoint = replaced }).apply(ca, state));
    scope.source_ordinal = 9;
    state = try (Mutation{ .configure = scope }).apply(ca, state);
    try std.testing.expect(state.checkpoint_authority.?.sameAuthority(authority));
    const bytes = try encode(a, state);
    defer a.free(bytes);
    var reopened = try parse(a, bytes);
    defer reopened.deinit();
    try std.testing.expect(reopened.value.checkpoint_authority.?.sameAuthority(authority));
    const write = try encodeWrite(a, .{ .table_id = 7, .source_id = scope.source_id, .expected_revision = state.revision, .expected_binding = scope.identity(), .expected_catalog_digest = @splat(8), .mutation = bind });
    defer a.free(write);
    try std.testing.expectEqual(checkpoint_authority.protocol_version, requiredProtocolEncoded(write));
    const fence: Mutation = .{ .fence_checkpoint = .{ .authority_digest = authority.digest(), .proof = @splat(9) } };
    try std.testing.expectError(error.SourceBindingConflict, fence.apply(ca, state));
    state = try (Mutation{ .retire = .{ .receipt = "qualified-source-retirement" } }).apply(ca, state);
    try std.testing.expect(hasWork(state));
    var next_scope = scope;
    next_scope.generation += 1;
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .reseed = next_scope }).apply(ca, state));
    state = try fence.apply(ca, state);
    try std.testing.expect(!hasWork(state));
    try std.testing.expectEqual(state.revision, (try fence.apply(ca, state)).revision);
    state = try (Mutation{ .reseed = next_scope }).apply(ca, state);
    try std.testing.expect(state.checkpoint_authority == null);
    try std.testing.expect(state.artifact_control != null);
}

test "source control closing intent survives reopen blocks new grants and retains cleanup debt" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    const legacy = try encode(a, state);
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "closing") == null);
    const pending: Reservation = .{ .sequence = 1, .batch_id = "original", .checkpoint = "first", .accepted_at_ns = 10, .payload_digest = @splat(5), .plan_digest = @splat(6), .transaction_id = @splat(7), .destination = .native, .delivery_manifest = "original-plan" };
    state = try (Mutation{ .reserve = pending }).apply(ca, state);
    state = try (Mutation{ .close = {} }).apply(ca, state);
    try std.testing.expect(state.closing and !state.retired);
    try std.testing.expect(hasWork(state));
    try std.testing.expectEqual(state.revision, (try (Mutation{ .close = {} }).apply(ca, state)).revision);
    const bytes = try encode(a, state);
    defer a.free(bytes);
    var reopened = try parse(a, bytes);
    defer reopened.deinit();
    try std.testing.expect(reopened.value.closing);
    try std.testing.expectEqual(pending.identity(scope), reopened.value.pending.?.identity(scope));
    // Lost reservation acknowledgements can still confirm the exact old grant.
    try std.testing.expectEqual(state.revision, (try (Mutation{ .reserve = pending }).apply(ca, reopened.value)).revision);
    try std.testing.expectError(error.SourceBindingConflict, (Mutation{ .replan = .{ .reservation_digest = pending.identity(scope), .plan_digest = @splat(8), .transaction_id = @splat(9), .delivery_manifest = "replacement", .not_admitted_receipt = "proof" } }).apply(ca, state));
    // Positive original outcomes still drain while admission remains closed.
    state = try (Mutation{ .accept = .{ .reservation_digest = pending.identity(scope), .receipt = "accepted-original", .commit_reference = "committed-original" } }).apply(ca, state);
    var next = pending;
    next.sequence = 2;
    next.batch_id = "new";
    next.expected_checkpoint = "first";
    try std.testing.expectError(error.SourceBindingConflict, (Mutation{ .reserve = next }).apply(ca, state));
    try std.testing.expect(hasWork(state));
    state = try (Mutation{ .retire = .{ .receipt = "all-roots-fenced" } }).apply(ca, state);
    try std.testing.expect(!hasWork(state));
    var replacement = scope;
    replacement.generation += 1;
    state = try (Mutation{ .reseed = replacement }).apply(ca, state);
    try std.testing.expect(!state.closing and !state.retired);
    const close_write = try encodeWrite(a, .{ .table_id = 7, .source_id = scope.source_id, .expected_revision = 1, .expected_binding = scope.identity(), .expected_catalog_digest = @splat(10), .mutation = .{ .close = {} } });
    defer a.free(close_write);
    try std.testing.expectEqual(closing_protocol_version, requiredProtocolEncoded(close_write));
    try std.testing.expectEqual(manifest_protocol_version, requiredProtocolEncoded("{}"));
}

test "source control scope fencing pins idle bindings until closing retirement and native proof" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    try std.testing.expectError(error.SourceBindingConflict, (Mutation{ .enable_scope_fencing = {} }).apply(ca, state));
    const authority: checkpoint_authority.Spec = .{ .binding = scope.identity(), .table_name = "checkpoint", .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(5), .root_generation = 0, .group_id = 17, .topology_epoch = 1 } };
    state = try (Mutation{ .bind_checkpoint = authority }).apply(ca, state);
    try std.testing.expect(!hasWork(state));
    const legacy = try encode(a, state);
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "scope_fencing") == null);
    state = try (Mutation{ .enable_scope_fencing = {} }).apply(ca, state);
    try std.testing.expect(hasWork(state));
    try std.testing.expectEqual(state.revision, (try (Mutation{ .enable_scope_fencing = {} }).apply(ca, state)).revision);
    try std.testing.expectError(error.SourcePendingWork, (Mutation{ .retire = .{ .receipt = "too-early" } }).apply(ca, state));
    state = try (Mutation{ .close = {} }).apply(ca, state);
    state = try (Mutation{ .retire = .{ .receipt = "cohort-closed" } }).apply(ca, state);
    try std.testing.expect(hasWork(state));
    const saved = try encode(a, state);
    defer a.free(saved);
    var reopened = try parse(a, saved);
    defer reopened.deinit();
    try std.testing.expect(reopened.value.scope_fencing and reopened.value.closing);
    state = try (Mutation{ .fence_checkpoint = .{ .authority_digest = authority.digest(), .proof = @splat(6) } }).apply(ca, reopened.value);
    try std.testing.expect(!hasWork(state));
    try std.testing.expectEqual(closing_protocol_version, requiredProtocol(.{ .enable_scope_fencing = {} }));
}

test "source control retained manifests survive acceptance without profile downgrade" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const scope: Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var state = try (Mutation{ .configure = scope }).apply(ca, .{});
    var reservation: Reservation = .{ .sequence = 1, .batch_id = "one", .checkpoint = "cp1", .accepted_at_ns = 1, .payload_digest = @splat(5), .plan_digest = @splat(6), .transaction_id = @splat(7), .destination = .native, .delivery_manifest = "original-owner-and-artifact", .artifact = artifactTestSpec(scope, 1) };
    const old_identity = reservation.identity(scope);
    reservation.retain_manifest = true;
    try std.testing.expect(!std.mem.eql(u8, &old_identity, &reservation.identity(scope)));
    try std.testing.expectEqual(manifest_protocol_version, requiredProtocol(.{ .reserve = reservation }));
    const write = try encodeWrite(ca, .{ .table_id = 7, .source_id = scope.source_id, .expected_revision = state.revision, .expected_binding = scope.identity(), .expected_catalog_digest = @splat(8), .mutation = .{ .reserve = reservation } });
    try std.testing.expectEqual(manifest_protocol_version, requiredProtocolEncoded(write));
    const explicit_false = try std.mem.replaceOwned(u8, ca, write, "\"retain_manifest\":true", "\"retain_manifest\":false");
    try std.testing.expectEqual(manifest_protocol_version, requiredProtocolEncoded(explicit_false));
    const escaped_false = try std.mem.replaceOwned(u8, ca, explicit_false, "retain_manifest", "reta\\u0069n_manifest");
    try std.testing.expectEqual(manifest_protocol_version, requiredProtocolEncoded(escaped_false));
    state = try (Mutation{ .reserve = reservation }).apply(ca, state);
    const id = reservation.identity(scope);
    try std.testing.expect(state.manifest_retention);
    try std.testing.expectEqualStrings(reservation.delivery_manifest, state.artifact_control.?.current.?.recovery.?.bytes);
    state = try (Mutation{ .seal_artifact = .{ .reservation_digest = id, .stamp = @splat(9) } }).apply(ca, state);
    state = try (Mutation{ .accept = .{ .reservation_digest = id, .receipt = "accepted", .commit_reference = "committed" } }).apply(ca, state);
    try std.testing.expect(state.pending == null);
    const saved = try encode(ca, state);
    var reopened = try parse(a, saved);
    defer reopened.deinit();
    try std.testing.expect(reopened.value.manifest_retention);
    try std.testing.expectEqualStrings(reservation.delivery_manifest, reopened.value.artifact_control.?.current.?.recovery.?.bytes);
    var corrupt = reopened.value;
    corrupt.artifact_control.?.current.?.recovery = null;
    try std.testing.expectError(error.InvalidSourceControl, corrupt.validate());
    state = try (Mutation{ .retire_artifact = .{ .reservation_digest = id, .proof = @splat(10), .reason = .committed } }).apply(ca, state);
    try std.testing.expectEqualStrings(reservation.delivery_manifest, state.artifact_control.?.current.?.recovery.?.bytes);
    state = try (Mutation{ .reclaim_artifact = .{ .reservation_digest = id, .proof = @splat(11) } }).apply(ca, state);
    try std.testing.expect(state.artifact_control.?.current == null);
    try std.testing.expect(state.manifest_retention);
    reservation.sequence = 2;
    reservation.batch_id = "two";
    reservation.expected_checkpoint = "cp1";
    reservation.checkpoint = "cp2";
    reservation.artifact = artifactTestSpec(scope, 2);
    reservation.retain_manifest = false;
    try std.testing.expectError(error.UnsupportedOperation, (Mutation{ .reserve = reservation }).apply(ca, state));
    reservation.retain_manifest = true;
    state = try (Mutation{ .reserve = reservation }).apply(ca, state);
    try std.testing.expectEqual(@as(u64, 2), state.artifact_control.?.next_token);
    const old_attempt = state.pending.?.identity(scope);
    state = try (Mutation{ .retire_artifact = .{ .reservation_digest = old_attempt, .proof = digest("fenced"), .reason = .unsealed } }).apply(ca, state);
    state = try (Mutation{ .reclaim_artifact = .{ .reservation_digest = old_attempt, .proof = @splat(12) } }).apply(ca, state);
    var replacement: Replan = .{ .reservation_digest = old_attempt, .plan_digest = @splat(13), .transaction_id = @splat(14), .delivery_manifest = "replacement-owner-and-artifact", .not_admitted_receipt = "fenced", .artifact = artifactTestSpec(scope, 3) };
    try std.testing.expectError(error.UnsupportedOperation, (Mutation{ .replan = replacement }).apply(ca, state));
    replacement.retain_manifest = true;
    try std.testing.expectEqual(manifest_protocol_version, requiredProtocol(.{ .replan = replacement }));
    state = try (Mutation{ .replan = replacement }).apply(ca, state);
    try std.testing.expectEqualStrings(replacement.delivery_manifest, state.artifact_control.?.current.?.recovery.?.bytes);
    try std.testing.expectEqual(state.revision, (try (Mutation{ .replan = replacement }).apply(ca, state)).revision);
}
