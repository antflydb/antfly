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

//! Codec support is not readiness: file migration, required BEGIN, durable
//! effects and recovery must also be available before negotiation succeeds.
const std = @import("std");
const admission = @import("../common/read_admission.zig");
pub const Requirement = @import("../storage/db/prepared_transaction_effects.zig").Requirement;
pub const Mode = @import("../storage/db/prepared_transaction_effects.zig").Mode;
pub const wire_version: u16 = 1;
pub const Phase = enum(u8) { capabilities = 0, begin = 1, prepare = 2, seal = 3, sealed = 4, status = 5, decision = 6 };
pub const max_payload_bytes = 64 * 1024 * 1024 + 4096;
const header_bytes = 16;
const checksum_bytes = 32;
const quantum = 4096;

pub fn forSyncLevel(level: @import("../storage/db/types.zig").SyncLevel) Requirement {
    return .{ .mode = switch (level) {
        .propose, .write, .full_text => .deferred,
        .enrichments, .full_index => .synchronous,
    } };
}

pub const Capabilities = struct {
    protocol: u16 = 0,
    requirement_format: u16 = 0,
    effects_format: u16 = 0,
    minimum_reader: u32 = 0,
    permanent_reader: u32 = 0,
    catalog_envelope: u32 = 0,
    durable_begin: bool = false,
    durable_seal: bool = false,
    recovery_consumes_effects: bool = false,
    retained_contract: bool = false,
    bound_retained_contract: bool = false,
    catalog_migration_complete: bool = false,
    import_policy_fenced: bool = false,
    deferred: bool = false,
    synchronous: bool = false,

    pub fn require(self: @This(), requirement: Requirement) !void {
        if (self.protocol != wire_version or self.requirement_format != 1 or self.effects_format != 1 or
            self.minimum_reader < 7 or self.permanent_reader < 7 or self.catalog_envelope < 7 or
            !self.durable_begin or !self.durable_seal or !self.recovery_consumes_effects or !self.retained_contract or
            !self.bound_retained_contract or !self.catalog_migration_complete or !self.import_policy_fenced or
            !(if (requirement.mode == .deferred) self.deferred else self.synchronous))
            return error.PreparedTransactionProtocolUnavailable;
    }
};

/// A status observation contains actual durable metadata, including after
/// accepted frames retire. Expected routing is never copied into the evidence.
pub const Decision = struct {
    status: @import("../storage/db/types.zig").TxnStatus,
    requirement: ?Requirement,
    binding: ?@import("../storage/db/prepared_transaction_effects.zig").Binding = null,

    pub fn fromDurable(value: @import("../storage/transactions.zig").TxnManager.PreparationDecision) @This() {
        return .{ .status = value.status, .requirement = value.requirement, .binding = value.binding };
    }

    pub fn validate(self: @This(), requirement: Requirement, expected: @import("../storage/db/prepared_transaction_effects.zig").Binding) !@import("../storage/db/types.zig").TxnStatus {
        // Legacy aborts stay terminal after their sidecars retire. A bound
        // abort must still refer to the observed original participant.
        if (self.status != .aborted) {
            const accepted = self.requirement orelse return error.PreparationRequirementMismatch;
            if (accepted.mode != requirement.mode) return error.PreparationRequirementMismatch;
            if (self.binding == null) return error.PreparationContractConflict;
        }
        if (self.binding) |binding| if (!std.meta.eql(binding, expected)) return error.PreparationContractConflict;
        return self.status;
    }
};

pub const SealRequest = struct {
    txn_id: [16]u8,
    group_id: u64,
    topology_epoch: u64,
    /// Exact canonical participant locator, including hidden scope.
    participant_digest: [32]u8,
    route_digest: [32]u8,
    requirement: Requirement,
    pub fn binding(self: @This()) @import("../storage/db/prepared_transaction_effects.zig").Binding {
        return .{ .group_id = self.group_id, .topology_epoch = self.topology_epoch, .participant_digest = self.participant_digest, .route_digest = self.route_digest };
    }
};

/// Returned only after accepted effects and their requirement are durable.
/// Visibility and future replay-credit policy are separate from this receipt.
pub const SealReceipt = struct {
    request: SealRequest,
    intent_revision: u64,
    intent_digest: [32]u8,
    catalog_digest: [32]u8,
    frame_digest: [32]u8,
    pub fn fromDurable(value: @import("../storage/db/prepared_transaction_effects.zig").BoundReceipt) @This() {
        return .{ .request = .{ .txn_id = value.txn_id, .group_id = value.binding.group_id, .topology_epoch = value.binding.topology_epoch, .participant_digest = value.binding.participant_digest, .route_digest = value.binding.route_digest, .requirement = value.requirement }, .intent_revision = value.intent_revision, .intent_digest = value.intent_digest, .catalog_digest = value.catalog_digest, .frame_digest = value.frame_digest };
    }
    pub fn validate(self: @This(), expected: SealRequest) !void {
        if (!std.meta.eql(self.request, expected)) return error.PreparedEffectsMismatch;
    }
};

pub const Decoded = struct { phase: Phase, requirement: Requirement, payload: []const u8 };

/// Canonical fixed-width little-endian identity. Never hash process-local
/// admission fields or depend on a peer's JSON property ordering.
pub fn routeDigest(fence: @import("../metadata/catalog_route_contract.zig").CatalogRouteFence) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly-prepared-route-v1\x00");
    var protocol: [2]u8 = undefined;
    std.mem.writeInt(u16, &protocol, fence.protocol, .little);
    hash.update(&protocol);
    hash.update(&.{@intFromBool(fence.metadata_incarnation != null)});
    const incarnation = fence.metadata_incarnation orelse @as([32]u8, @splat(0));
    hash.update(&incarnation);
    for ([_]u64{ fence.metadata_group_id, fence.catalog_revision, fence.table_id, fence.topology_epoch, fence.route.group_id, fence.route.range_id, fence.route.identity_namespace.table_id, fence.route.identity_namespace.shard_id, fence.route.identity_namespace.range_id }) |value| {
        var encoded: [8]u8 = undefined;
        std.mem.writeInt(u64, &encoded, value, .little);
        hash.update(&encoded);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
fn update(hash: *std.crypto.hash.Blake3, bytes: []const u8, control: ?admission.Port) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        if (control) |owner| try owner.checkpoint();
        const n = @min(quantum, bytes.len - offset);
        hash.update(bytes[offset..][0..n]);
        offset += n;
    }
}

/// Binary outer framing makes old JSON ingress reject before mutation. Decode
/// borrows its input; retain the original request owner through payload use.
pub fn encode(alloc: std.mem.Allocator, phase: Phase, requirement: Requirement, payload: []const u8, control: ?admission.Port) ![]u8 {
    if (control) |owner| try owner.checkpoint();
    if (payload.len > max_payload_bytes) return error.TransactionTooLarge;
    const out = try alloc.alloc(u8, header_bytes + payload.len + checksum_bytes);
    errdefer alloc.free(out);
    @memcpy(out[0..4], "ATPW");
    std.mem.writeInt(u16, out[4..6], wire_version, .little);
    out[6] = @backingInt(phase);
    out[7] = @backingInt(requirement.mode);
    std.mem.writeInt(u32, out[8..12], @intCast(payload.len), .little);
    @memset(out[12..16], 0);
    var offset: usize = 0;
    while (offset < payload.len) {
        if (control) |owner| try owner.checkpoint();
        const n = @min(quantum, payload.len - offset);
        @memcpy(out[header_bytes + offset ..][0..n], payload[offset..][0..n]);
        offset += n;
    }
    var hash = std.crypto.hash.Blake3.init(.{});
    try update(&hash, out[0 .. header_bytes + payload.len], control);
    hash.final(out[header_bytes + payload.len ..][0..checksum_bytes]);
    if (control) |owner| try owner.checkpoint();
    return out;
}

pub fn decode(bytes: []const u8, expected: Phase, control: ?admission.Port) !Decoded {
    if (control) |owner| try owner.checkpoint();
    if (bytes.len < header_bytes + checksum_bytes or !std.mem.eql(u8, bytes[0..4], "ATPW")) return error.InvalidPreparationRequirement;
    if (std.mem.readInt(u16, bytes[4..6], .little) != wire_version) return error.UnsupportedVersion;
    const phase = std.enums.fromInt(Phase, bytes[6]) orelse return error.InvalidPreparationRequirement;
    if (phase != expected or !std.mem.allEqual(u8, bytes[12..16], 0)) return error.InvalidPreparationRequirement;
    const mode = std.enums.fromInt(Mode, bytes[7]) orelse return error.InvalidPreparationRequirement;
    const len = std.mem.readInt(u32, bytes[8..12], .little);
    if (len > max_payload_bytes or @as(u64, len) + header_bytes + checksum_bytes != bytes.len) return error.InvalidPreparationRequirement;
    var hash = std.crypto.hash.Blake3.init(.{});
    try update(&hash, bytes[0 .. header_bytes + len], control);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    if (!std.mem.eql(u8, &digest, bytes[header_bytes + len ..])) return error.InvalidPreparationRequirement;
    if (control) |owner| try owner.checkpoint();
    return .{ .phase = phase, .requirement = .{ .mode = mode }, .payload = bytes[header_bytes .. header_bytes + len] };
}
