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

//! Native owner-local private index admission. These transaction primitives do
//! not authenticate metadata, commit, sign receipts, or enable a public producer.
//! The ordered owner must first attest the metadata plan and local identity, then
//! stage its durable outbox with these effects and abort on every error.
const std = @import("std");
const topology = @import("relational_integrity_topology.zig");
const physical_catalog = @import("relational_index_catalog.zig");
const integrity_catalog = @import("relational_integrity_catalog.zig");
const candidate_mod = @import("relational_index_candidate.zig");
const schema = @import("../schema.zig");
const records = @import("relational_index_records.zig");
const transactions = @import("../transactions.zig");
const source_authority = @import("../source_authority.zig");
const positions = @import("receipt_position.zig");
const publication = @import("relational_index_publication_progress.zig");
const inputs_mod = @import("relational_index_inputs.zig");
const cancellation = @import("relational_index_cancellation_intent.zig");
const Digest = [32]u8;
pub const key = "\x00\x00__metadata__:relational_index_owner_job:v1";
pub const Phase = enum(u8) { fencing, reserved, installed };
pub const private_claims = @import("relational_index_private_claims.zig");
pub const cancellation_cleanup_key = key ++ ":cancel-gc";

/// One reusable cleanup-admission slot, not a publication acknowledgement.
/// Public retirement admission leaves a single reserved slot for the active
/// private job, so cancellation cannot deadlock behind a full churn queue.
pub const CancellationCleanup = struct { position: positions.Native, duplicate: bool };
pub const OrderedCancellationCleanup = struct { position: positions.Position, duplicate: bool };

/// Retain claim scope with queue admission, before the large immutable inputs
/// can be retired. The generation is derived from the table incarnation and
/// burned allocation, never from a request-selected constraint name or hash.
pub const CancellationCleanupRecord = struct {
    pub const encoded_len = 416;
    intent: cancellation.Intent,
    position: positions.Position,
    claim_allocation: u64,

    pub fn claimGeneration(self: CancellationCleanupRecord) !@import("relational_integrity.zig").Generation {
        return integrity_catalog.privateIndexGeneration(self.intent.fence.namespace.table_id, self.claim_allocation);
    }

    pub fn encode(self: CancellationCleanupRecord) ![encoded_len]u8 {
        _ = try self.claimGeneration();
        const admission: cancellation.Record = .{ .intent = self.intent, .position = self.position };
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..8], "AICG\x02\x00\x00\x00");
        @memcpy(bytes[8..376], &try admission.encode());
        std.mem.writeInt(u64, bytes[376..384], self.claim_allocation, .little);
        @memcpy(bytes[384..416], &digest(bytes[0..384]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !CancellationCleanupRecord {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICG\x02\x00\x00\x00") or
            !std.mem.eql(u8, bytes[384..416], &digest(bytes[0..384]))) return error.InvalidPrivateIndexCancellation;
        const admission = try cancellation.Record.decode(bytes[8..376]);
        const result: CancellationCleanupRecord = .{
            .intent = admission.intent,
            .position = admission.position,
            .claim_allocation = std.mem.readInt(u64, bytes[376..384], .little),
        };
        _ = result.claimGeneration() catch return error.InvalidPrivateIndexCancellation;
        return result;
    }
};

fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}
fn nonzero(bytes: []const u8) bool {
    return !std.mem.allEqual(u8, bytes, 0);
}
fn optional(txn: anytype, physical_key: []const u8) !?[]const u8 {
    return txn.get(physical_key) catch |err| {
        // In-memory/read-only adapters may expose only NotFound. An exhaustive
        // switch with an else prong does not compile for that valid contract.
        // Preserve every other native read failure instead of treating it as
        // absence of an owner job or its durable fencing state.
        if (err == error.NotFound) return null;
        return err;
    };
}

test "relational index candidate optional owner reads preserve narrow and fallible transaction contracts" {
    const Narrow = struct {
        pub fn get(_: @This(), _: []const u8) error{NotFound}![]const u8 {
            return error.NotFound;
        }
    };
    const Fallible = struct {
        result: error{ NotFound, ReadFailed }![]const u8,
        pub fn get(self: @This(), _: []const u8) error{ NotFound, ReadFailed }![]const u8 {
            return self.result;
        }
    };
    try std.testing.expectEqual(@as(?[]const u8, null), try optional(Narrow{}, key));
    try std.testing.expectEqual(@as(?[]const u8, null), try optional(Fallible{ .result = error.NotFound }, key));
    try std.testing.expectError(error.ReadFailed, optional(Fallible{ .result = error.ReadFailed }, key));
    try std.testing.expectEqual(@as(?cancellation.Record, null), try cancellation.load(Narrow{}));
    try std.testing.expectError(error.ReadFailed, cancellation.load(Fallible{ .result = error.ReadFailed }));
    const fence: topology.Fence = .{ .role = .index_build, .transition_id = 1, .attempt = 1, .owner_group_id = 2, .peer_group_id = 2, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .catalog_digest = @splat(1) };
    try std.testing.expectError(error.PrivateIndexJobRequired, requireActive(Narrow{}, fence));
    try std.testing.expectError(error.ReadFailed, requireActive(Fallible{ .result = error.ReadFailed }, fence));
    try std.testing.expectEqualStrings("", (try optional(Fallible{ .result = "" }, key)).?);
    try std.testing.expectEqualStrings("binding", (try optional(Fallible{ .result = "binding" }, key)).?);
}

/// One fixed-size binding per owner. Full job IDs are retained; the topology
/// transition number alone never authenticates retries. Hashes are identity,
/// not proof of metadata authority. Admission and reserve are independently CASed.
pub const Binding = struct {
    pub const encoded_len = 688;
    id: [16]u8,
    plan_digest: Digest,
    owner_digest: Digest,
    fence: topology.Fence,
    ordinal: u16,
    authority: publication.Authority,
    phase: Phase = .fencing,
    integrity_before: Digest,
    integrity_reserved: Digest,
    integrity_candidate: Digest,
    schema_before: Digest,
    physical_before: ?physical_catalog.Head,
    physical_reserved: physical_catalog.Head,
    physical_candidate: physical_catalog.Head,
    index: records.Id,
    inputs_digest: Digest,

    pub fn encode(self: Binding) ![encoded_len]u8 {
        if (!nonzero(&self.id) or !nonzero(&self.plan_digest) or !nonzero(&self.owner_digest) or !nonzero(&self.inputs_digest) or
            self.ordinal >= @import("relational_index_publication_progress.zig").max_owners or
            self.fence.role != .index_build or !nonzero(&self.integrity_before) or
            self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0 or
            !nonzero(&self.integrity_reserved) or !nonzero(&self.integrity_candidate) or !nonzero(&self.schema_before) or
            !std.mem.eql(u8, &self.integrity_before, &self.fence.catalog_digest) or
            self.physical_reserved.revision <= 1 or self.physical_candidate.revision != self.physical_reserved.revision or
            self.index.generation != self.physical_reserved.revision or
            self.physical_candidate.schema_version != (std.math.add(u32, self.physical_reserved.schema_version, 1) catch return error.InvalidPrivateIndexOwnerJob))
            return error.InvalidPrivateIndexOwnerJob;
        if (self.physical_before) |before| {
            if (before.revision == 0 or (std.math.add(u64, before.revision, 1) catch return error.InvalidPrivateIndexOwnerJob) != self.physical_reserved.revision or
                before.schema_version != self.physical_reserved.schema_version or
                !std.mem.eql(u8, &before.blob_digest, &self.physical_reserved.blob_digest)) return error.InvalidPrivateIndexOwnerJob;
        } else if (self.physical_reserved.revision != 2) return error.InvalidPrivateIndexOwnerJob;
        var out: [encoded_len]u8 = @splat(0);
        @memcpy(out[0..4], "AIOJ");
        std.mem.writeInt(u32, out[4..8], 4, .little);
        @memcpy(out[8..24], &self.id);
        @memcpy(out[24..56], &self.plan_digest);
        @memcpy(out[56..88], &self.owner_digest);
        @memcpy(out[88..224], &try self.fence.encode());
        std.mem.writeInt(u16, out[224..226], self.ordinal, .little);
        out[226] = @backingInt(self.phase);
        out[227] = @intFromBool(self.physical_before != null);
        out[228] = @backingInt(self.authority);
        @memcpy(out[232..264], &self.integrity_before);
        @memcpy(out[264..296], &self.integrity_reserved);
        @memcpy(out[296..328], &self.integrity_candidate);
        @memcpy(out[328..360], &self.schema_before);
        if (self.physical_before) |before| @memcpy(out[360..444], &before.encode());
        @memcpy(out[444..528], &self.physical_reserved.encode());
        @memcpy(out[528..612], &self.physical_candidate.encode());
        @memcpy(out[612..624], &self.index.encode());
        @memcpy(out[624..656], &self.inputs_digest);
        @memcpy(out[656..688], &digest(out[0..656]));
        return out;
    }

    pub fn decode(bytes: []const u8) !Binding {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "AIOJ") or
            std.mem.readInt(u32, bytes[4..8], .little) != 4 or bytes[227] > 1 or
            !std.mem.allEqual(u8, bytes[229..232], 0) or
            !std.mem.eql(u8, bytes[656..688], &digest(bytes[0..656]))) return error.InvalidPrivateIndexOwnerJob;
        if (bytes[227] == 0 and !std.mem.allEqual(u8, bytes[360..444], 0)) return error.InvalidPrivateIndexOwnerJob;
        const result: Binding = .{
            .id = bytes[8..24].*,
            .plan_digest = bytes[24..56].*,
            .owner_digest = bytes[56..88].*,
            .fence = try topology.Fence.decode(bytes[88..224]),
            .ordinal = std.mem.readInt(u16, bytes[224..226], .little),
            .authority = std.enums.fromInt(publication.Authority, bytes[228]) orelse return error.InvalidPrivateIndexOwnerJob,
            .phase = std.enums.fromInt(Phase, bytes[226]) orelse return error.InvalidPrivateIndexOwnerJob,
            .integrity_before = bytes[232..264].*,
            .integrity_reserved = bytes[264..296].*,
            .integrity_candidate = bytes[296..328].*,
            .schema_before = bytes[328..360].*,
            .physical_before = if (bytes[227] == 1) try physical_catalog.Head.decode(bytes[360..444]) else null,
            .physical_reserved = try physical_catalog.Head.decode(bytes[444..528]),
            .physical_candidate = try physical_catalog.Head.decode(bytes[528..612]),
            .index = try records.Id.decode(bytes[612..624]),
            .inputs_digest = bytes[624..656].*,
        };
        _ = try result.encode();
        return result;
    }
};

pub fn prepare(id: [16]u8, plan_digest: Digest, owner_digest: Digest, ordinal: u16, fence: topology.Fence, authority: publication.Authority, candidate: *const candidate_mod.Candidate, physical: *const physical_catalog.PrivateAddition) !Binding {
    if (!std.mem.eql(u8, &candidate.native_before_schema_digest, &physical.reservation.schema_digest)) return error.RelationalIndexCatalogSchemaMismatch;
    const before = candidate.reservation.expected orelse return error.InvalidPrivateIndexOwnerJob;
    const binding: Binding = .{
        .id = id,
        .plan_digest = plan_digest,
        .owner_digest = owner_digest,
        .ordinal = ordinal,
        .authority = authority,
        .fence = fence,
        .integrity_before = digest(before),
        .integrity_reserved = digest(candidate.reservation.value),
        .integrity_candidate = digest(candidate.next.value),
        .schema_before = physical.reservation.schema_digest,
        .physical_before = physical.reservation.expected,
        .physical_reserved = physical.reservation.head,
        .physical_candidate = physical.candidate.head,
        .index = physical.index,
        .inputs_digest = inputs_mod.digest(candidate.inputs.bytes),
    };
    const incarnation = try integrity_catalog.incarnationFromTableId(fence.namespace.table_id);
    if (!std.mem.eql(u8, &candidate.next.catalog.incarnation, &incarnation) or
        !std.mem.eql(u8, &candidate.definition_digest, &physical.definition_digest) or
        !std.mem.eql(u8, &candidate.reservation.catalog.schema_digest, &physical.reservation.schema_digest) or
        !std.mem.eql(u8, &candidate.next.catalog.schema_digest, &physical.candidate.schema_digest) or
        candidate.reservation.catalog.schema_version != physical.reservation.head.schema_version or
        candidate.next.catalog.schema_version != physical.candidate.head.schema_version)
        return error.InvalidPrivateIndexOwnerJob;
    _ = try binding.encode();
    return binding;
}

pub fn load(txn: anytype) !?Binding {
    return if (try optional(txn, key)) |bytes| try Binding.decode(bytes) else null;
}

/// Suppress descriptor reconciliation while the actual durable owner fence is
/// active. This is preservation, never execution or serving authority. Current
/// also verifies a never-visited cancellation's exact intent when no job exists;
/// that owner must still reopen to admit and finish its bounded cleanup.
pub fn protectsOwnerCatalog(txn: anytype, namespace: @import("doc_identity_namespace.zig").Namespace) !bool {
    const fence = (try topology.current(txn)) orelse return false;
    if (fence.role != .index_build) return false;
    if (!fence.namespace.eql(namespace)) return error.IdentityNamespaceMismatch;
    return true;
}

/// Cold preparation owns its input bytes before any further store read. This
/// compiler work belongs outside serialized apply, never on a build-page path.
pub fn reopenCandidate(alloc: std.mem.Allocator, txn: anytype, binding: Binding) !candidate_mod.Candidate {
    var inputs = try inputs_mod.Prepared.load(alloc, txn, binding.inputs_digest);
    defer inputs.deinit();
    const view = try inputs_mod.decode(inputs.bytes);
    var candidate = try candidate_mod.Candidate.prepare(alloc, binding.fence.namespace.table_id, view.before_json, view.after_json, view.predecessor, view.name);
    errdefer candidate.deinit();
    // API JSON and serialized native schema are different representations.
    // Match the independently rebuilt native identity, not the API byte hash.
    if (!std.mem.eql(u8, &candidate.reservation.catalog.schema_digest, &binding.schema_before) or
        !std.mem.eql(u8, &digest(view.predecessor), &binding.integrity_before) or
        !std.mem.eql(u8, &digest(candidate.reservation.value), &binding.integrity_reserved) or
        !std.mem.eql(u8, &digest(candidate.next.value), &binding.integrity_candidate)) return error.InvalidPrivateIndexInputs;
    return candidate;
}

fn requireNamespace(txn: anytype, binding: Binding) !void {
    const actual = (try @import("doc_identity.zig").loadNamespaceTxn(txn)) orelse return error.IdentityNamespaceMismatch;
    if (!actual.eql(binding.fence.namespace)) return error.IdentityNamespaceMismatch;
    _ = try source_authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
}

/// Three reusable slots, not a per-attempt history. Historical records for a
/// different immutable job are inert and replaced by its next admitted job.
/// The caller commits these records with the effect and its durable outbox;
/// returned staged receipts MUST NOT be signed or sent before commit succeeds.
pub const native_effect_prefix = key ++ ":native:";
pub const NativeEffect = enum(u8) { admitted = 0, fenced = 1, reserved = 2 };
const native_effect_len = 136;
const NativeCut = struct { position: positions.Native, evidence: Digest };

pub fn nativeEffectKey(action: NativeEffect) [native_effect_prefix.len + 1]u8 {
    var result: [native_effect_prefix.len + 1]u8 = undefined;
    @memcpy(result[0..native_effect_prefix.len], native_effect_prefix);
    result[native_effect_prefix.len] = @backingInt(action);
    return result;
}

fn nativeContext(binding: Binding) !Digest {
    var original = binding;
    original.phase = .fencing;
    return digest(&try original.encode());
}

fn nativeEvidence(binding: Binding, action: NativeEffect) !Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:index-owner-native-effect:v1");
    hash.update(&try nativeContext(binding));
    hash.update(&.{@backingInt(action)});
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

fn readNativeCut(txn: anytype, binding: Binding, action: NativeEffect) !?NativeCut {
    if (binding.authority != .native) return error.InvalidControlReceiptPosition;
    const state = try source_authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
    return readNativeCutAt(txn, binding, action, state);
}

fn readNativeCutAt(txn: anytype, binding: Binding, action: NativeEffect, state: source_authority.State) !?NativeCut {
    const bytes = (try optional(txn, &nativeEffectKey(action))) orelse return null;
    if (bytes.len != native_effect_len or !std.mem.eql(u8, bytes[0..4], "AIN1") or
        bytes[4] != @backingInt(action) or !std.mem.allEqual(u8, bytes[5..7], 0) or
        !std.mem.eql(u8, bytes[104..136], &digest(bytes[0..104]))) return error.InvalidPrivateIndexNativeReceipt;
    const position = positions.Position.decode(bytes[71..104]) catch return error.InvalidPrivateIndexNativeReceipt;
    if (position != .native) return error.InvalidPrivateIndexNativeReceipt;
    if (!std.mem.eql(u8, bytes[7..39], &try nativeContext(binding))) return null;
    try position.requireNamespace(binding.fence.namespace);
    if (position.native.sequence > state.sequence or
        !std.mem.eql(u8, bytes[39..71], &try nativeEvidence(binding, action))) return error.InvalidPrivateIndexNativeReceipt;
    return .{ .position = position.native, .evidence = bytes[39..71].* };
}

fn requireNativeCut(txn: anytype, binding: Binding, action: NativeEffect) !NativeCut {
    return (try readNativeCut(txn, binding, action)) orelse return error.InvalidPrivateIndexNativeReceipt;
}

pub fn nativeEffectPosition(txn: anytype, binding: Binding, action: NativeEffect) !?positions.Native {
    return if (try readNativeCut(txn, binding, action)) |cut| cut.position else null;
}

/// Bounded local status for a receiver which independently obtains current
/// metadata permission. Absence is not a cancellation fence or an ACK proof.
pub fn inspectNative(txn: anytype, expected: anytype) !?Binding {
    return inspectOwner(txn, expected, .native);
}

fn inspectOwner(txn: anytype, expected: anytype, comptime kind: source_authority.Kind) !?Binding {
    try expected.validate();
    _ = try source_authority.require(txn, kind, @import("online_source_contract.zig").namespaceBytes(expected.namespace));
    const binding = (try load(txn)) orelse return null;
    if ((binding.authority == .native) != (kind == .native) or !std.mem.eql(u8, &binding.id, &expected.id) or
        !std.mem.eql(u8, &binding.plan_digest, &expected.plan_digest) or
        !std.mem.eql(u8, &binding.owner_digest, &expected.owner_digest) or binding.ordinal != expected.ordinal or
        !binding.fence.namespace.eql(expected.namespace) or binding.fence.owner_group_id != expected.owner_group_id)
        return error.PrivateIndexOwnerJobChanged;
    _ = try requireCurrentJob(txn, binding);
    return binding;
}

/// One coherent, bounded inspection for cancellation dispatch. These cuts are
/// progress facts, never publication receipts. No manifest or schema compiler
/// is needed to choose the next effect after restart.
pub const CancellationState = struct {
    // An untouched native source legitimately has clock zero, unlike a cut.
    source: source_authority.State,
    intent: ?cancellation.Record = null,
    binding: ?Binding = null,
    admitted: ?positions.Native = null,
    fenced: ?positions.Native = null,
    reserved: ?positions.Native = null,
    cleanup: ?positions.Native = null,
    claim_allocation: ?u64 = null,
    artifacts_retired: bool = false,
    claims_retired: bool = false,
    completed: ?Canceled = null,

    /// Validate a decoded inspection before dispatching another owner effect.
    /// Structural validity is not metadata permission or root attestation.
    pub fn validate(self: CancellationState, expected: anytype) !void {
        try expected.validate();
        if (self.source.kind != .native or !std.mem.eql(u8, &self.source.namespace, &@import("online_source_contract.zig").namespaceBytes(expected.namespace)))
            return error.OnlineSourceScopeChanged;
        if (self.intent) |record| {
            if (record.position != .native) return error.InvalidPrivateIndexCancellation;
            try record.intent.validate();
            try requireCancellationIdentity(record.intent, expected);
        }
        for ([_]?positions.Native{ if (self.intent) |record| record.position.native else null, self.admitted, self.fenced, self.reserved, self.cleanup }) |maybe| {
            if (maybe) |cut| {
                try cut.validate();
                if (!cut.namespace.eql(expected.namespace) or cut.sequence > self.source.sequence) return error.InvalidPrivateIndexCancellation;
            }
        }
        if (self.binding) |binding| {
            _ = try binding.encode();
            if (binding.authority != .native or self.admitted == null) return error.InvalidPrivateIndexCancellation;
            try requireCancellationIdentity(cancellation.Intent.fromBinding(binding), expected);
            if (self.intent) |record| if (!record.intent.eql(cancellation.Intent.fromBinding(binding))) return error.PrivateIndexOwnerJobChanged;
            if ((binding.phase == .reserved) != (self.reserved != null)) return error.InvalidPrivateIndexCancellation;
        } else if (self.admitted != null or self.fenced != null or self.reserved != null or self.cleanup != null)
            return error.InvalidPrivateIndexCancellation;
        if (self.fenced) |cut| if (self.admitted == null or cut.sequence <= self.admitted.?.sequence) return error.InvalidPrivateIndexCancellation;
        if (self.reserved) |cut| if (self.fenced == null or cut.sequence <= self.fenced.?.sequence) return error.InvalidPrivateIndexCancellation;
        if (self.cleanup) |cut| {
            if (self.intent == null or self.reserved == null or self.claim_allocation == null or self.claim_allocation.? == 0 or
                cut.sequence <= self.intent.?.position.native.sequence or cut.sequence <= self.reserved.?.sequence)
                return error.InvalidPrivateIndexCancellation;
        } else if (self.claim_allocation != null) return error.InvalidPrivateIndexCancellation;
        if (self.artifacts_retired and self.cleanup == null) return error.InvalidPrivateIndexCancellation;
        if (self.claims_retired and self.cleanup == null) return error.InvalidPrivateIndexCancellation;
        if (self.completed) |record| {
            try record.validate();
            if (!self.artifacts_retired or !self.claims_retired or self.binding == null or self.intent == null or
                !std.meta.eql(record.binding, self.binding.?) or !std.meta.eql(record.intent_position, self.intent.?.position) or
                record.cleanup.position != .native or !std.meta.eql(record.cleanup.position.native, self.cleanup.?) or record.cleanup.claim_allocation != self.claim_allocation.? or
                record.position != .native or record.position.native.sequence > self.source.sequence) return error.InvalidPrivateIndexCancellation;
        }
    }
};

fn requireCancellationIdentity(intent: cancellation.Intent, expected: anytype) !void {
    if (!std.mem.eql(u8, &intent.id, &expected.id) or
        !std.mem.eql(u8, &intent.plan_digest, &expected.plan_digest) or
        !std.mem.eql(u8, &intent.owner_digest, &expected.owner_digest) or
        intent.ordinal != expected.ordinal or !intent.fence.namespace.eql(expected.namespace) or
        intent.fence.owner_group_id != expected.owner_group_id) return error.PrivateIndexOwnerJobChanged;
}

pub const RaftCancellationState = struct {
    source: source_authority.State,
    applied: ?positions.Position = null,
    intent: ?cancellation.Record = null,
    binding: ?Binding = null,
    admitted: ?positions.Position = null,
    fenced: ?positions.Position = null,
    reserved: ?positions.Position = null,
    cleanup: ?positions.Position = null,
    claim_allocation: ?u64 = null,
    artifacts_retired: bool = false,
    claims_retired: bool = false,
    completed: ?Canceled = null,

    fn requireCut(self: @This(), cut: positions.Position) !void {
        try cut.validate();
        const applied = self.applied orelse return error.InvalidPrivateIndexCancellation;
        if (cut != .raft or applied != .raft or cut.raft.index > applied.raft.index or cut.raft.term > applied.raft.term or
            (cut.raft.index == applied.raft.index and cut.raft.term != applied.raft.term)) return error.InvalidPrivateIndexCancellation;
    }

    pub fn validate(self: @This(), expected: anytype) !void {
        try expected.validate();
        if (self.source.kind != .raft or self.source.sequence != 0 or !std.mem.eql(u8, &self.source.namespace, &@import("online_source_contract.zig").namespaceBytes(expected.namespace))) return error.OnlineSourceScopeChanged;
        if (self.applied) |cut| try self.requireCut(cut);
        if (self.intent) |record| {
            try self.requireCut(record.position);
            try record.intent.validate();
            try requireCancellationIdentity(record.intent, expected);
        }
        for ([_]?positions.Position{ self.admitted, self.fenced, self.reserved, self.cleanup }) |cut| if (cut) |value| try self.requireCut(value);
        if (self.binding) |binding| {
            _ = try binding.encode();
            if (binding.authority != .raft or self.admitted == null or (binding.phase != .fencing and binding.phase != .reserved)) return error.InvalidPrivateIndexCancellation;
            try requireCancellationIdentity(cancellation.Intent.fromBinding(binding), expected);
            if (self.intent) |record| if (!record.intent.eql(cancellation.Intent.fromBinding(binding))) return error.PrivateIndexOwnerJobChanged;
            if ((binding.phase == .reserved) != (self.reserved != null)) return error.InvalidPrivateIndexCancellation;
        } else if (self.admitted != null or self.fenced != null or self.reserved != null or self.cleanup != null) return error.InvalidPrivateIndexCancellation;
        if (self.fenced) |cut| if (!cut.strictlyAfter(self.admitted orelse return error.InvalidPrivateIndexCancellation)) return error.InvalidPrivateIndexCancellation;
        if (self.reserved) |cut| if (!cut.strictlyAfter(self.fenced orelse return error.InvalidPrivateIndexCancellation)) return error.InvalidPrivateIndexCancellation;
        if (self.cleanup) |cut| {
            if (self.intent == null or self.claim_allocation == null or self.claim_allocation.? == 0 or
                !cut.strictlyAfter(self.intent.?.position) or !cut.strictlyAfter(self.reserved orelse return error.InvalidPrivateIndexCancellation)) return error.InvalidPrivateIndexCancellation;
        } else if (self.claim_allocation != null or self.artifacts_retired or self.claims_retired) return error.InvalidPrivateIndexCancellation;
        if (self.completed) |record| {
            try record.validate();
            try self.requireCut(record.position);
            if (!self.artifacts_retired or !self.claims_retired or self.binding == null or self.intent == null or
                !std.meta.eql(record.binding, self.binding.?) or !std.meta.eql(record.intent_position, self.intent.?.position) or
                !std.meta.eql(record.cleanup.position, self.cleanup orelse return error.InvalidPrivateIndexCancellation) or record.cleanup.claim_allocation != self.claim_allocation.?) return error.InvalidPrivateIndexCancellation;
        }
    }
};

pub fn inspectCancellation(txn: anytype, expected: anytype) !CancellationState {
    return inspectCancellationAuthority(txn, expected, .native);
}
pub fn inspectRaftCancellation(txn: anytype, expected: anytype) !RaftCancellationState {
    return inspectCancellationAuthority(txn, expected, .raft);
}
fn inspectCancellationAuthority(txn: anytype, expected: anytype, comptime kind: source_authority.Kind) !if (kind == .native) CancellationState else RaftCancellationState {
    const binding = try inspectOwner(txn, expected, kind);
    try cancellation.verifyRecovery(txn, binding);
    const authority = try source_authority.require(txn, kind, @import("online_source_contract.zig").namespaceBytes(expected.namespace));
    var result: if (kind == .native) CancellationState else RaftCancellationState = .{
        .source = authority,
        .intent = try cancellation.pending(txn),
        .binding = binding,
    };
    if (comptime kind == .raft) {
        const applied = try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn);
        result.applied = if (applied) |cut| .{ .raft = .{ .term = cut.term, .index = cut.index } } else null;
    }
    if (result.intent) |record| {
        try requireCancellationIdentity(record.intent, expected);
    }
    if (binding) |current| {
        inline for (.{ "admitted", "fenced", "reserved" }) |field| {
            @field(result, field) = if (comptime kind == .native) try nativeEffectPosition(txn, current, @field(NativeEffect, field)) else try @import("relational_index_raft_control.zig").retainedPosition(txn, current, @field(@import("relational_index_raft_control.zig").Effect, field));
        }
    }
    if (try loadCancellationCleanup(txn, binding)) |record| {
        const current = binding orelse return error.InvalidPrivateIndexCancellation;
        if ((record.position == .native) != (kind == .native)) return error.InvalidPrivateIndexCancellation;
        result.cleanup = if (comptime kind == .native) record.position.native else record.position;
        result.claim_allocation = record.claim_allocation;
        if (requireCancellationArtifactsRetired(txn, current)) |_| {
            result.artifacts_retired = true;
        } else |err| switch (err) {
            error.PrivateIndexCancellationPending => {},
            else => return err,
        }
        if (private_claims.requireRetired(txn, current)) |_| {
            result.claims_retired = true;
        } else |err| switch (err) {
            error.PrivateIndexCancellationPending => {},
            else => return err,
        }
    }
    if (try Canceled.load(txn)) |record| {
        if (binding) |current| {
            if (std.meta.eql(record.binding, current)) result.completed = record;
        }
    }
    try result.validate(expected);
    return result;
}

/// Shared by live page admission, receipt production and cold recovery. Read
/// the clock once, then validate only the three fixed slots; never scan history.
fn requireNativeCuts(txn: anytype, job: Binding) !void {
    if (job.authority == .raft) return @import("relational_index_raft_control.zig").requireCuts(txn, job);
    const state = try source_authority.require(txn, .native, @import("online_source_contract.zig").namespaceBytes(job.fence.namespace));
    const admitted = (try readNativeCutAt(txn, job, .admitted, state)) orelse return error.InvalidPrivateIndexNativeReceipt;
    const fenced = try readNativeCutAt(txn, job, .fenced, state);
    const reserved = try readNativeCutAt(txn, job, .reserved, state);
    if (fenced) |cut| if (cut.position.sequence <= admitted.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
    if (job.phase == .reserved or job.phase == .installed) {
        if (fenced == null or reserved == null or reserved.?.position.sequence <= fenced.?.position.sequence) return error.InvalidPrivateIndexNativeReceipt;
    } else if (reserved != null) return error.InvalidPrivateIndexNativeReceipt;
}

fn validateReplay(binding: Binding, replay: ?positions.Native) !void {
    if (replay) |position| {
        if (binding.authority != .native) return error.InvalidControlReceiptPosition;
        try position.validate();
        if (!position.namespace.eql(binding.fence.namespace)) return error.IdentityNamespaceMismatch;
    }
}

fn requireReplay(cut: NativeCut, replay: ?positions.Native) !void {
    if (replay) |position| if (!std.meta.eql(cut.position, position)) return error.InvalidControlReceiptPosition;
}

fn stageNativeCut(txn: anytype, binding: Binding, action: NativeEffect, replay: ?positions.Native) !NativeCut {
    try validateReplay(binding, replay);
    if (try readNativeCut(txn, binding, action)) |existing| {
        try requireReplay(existing, replay);
        return existing;
    }
    const position: positions.Native = .{ .namespace = binding.fence.namespace, .sequence = try source_authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), if (replay) |value| value.sequence else null) };
    const evidence = try nativeEvidence(binding, action);
    var bytes: [native_effect_len]u8 = @splat(0);
    @memcpy(bytes[0..4], "AIN1");
    bytes[4] = @backingInt(action);
    @memcpy(bytes[7..39], &try nativeContext(binding));
    @memcpy(bytes[39..71], &evidence);
    @memcpy(bytes[71..104], &try (positions.Position{ .native = position }).encode());
    @memcpy(bytes[104..136], &digest(bytes[0..104]));
    try txn.put(&nativeEffectKey(action), &bytes);
    return .{ .position = position, .evidence = evidence };
}

fn logicalReceipt(binding: Binding, action: NativeEffect, cut: NativeCut) !publication.Receipt {
    return .{
        .owner = .{ .plan_id = binding.id, .plan_digest = binding.plan_digest, .ordinal = binding.ordinal, .group_id = binding.fence.owner_group_id, .namespace = binding.fence.namespace, .authority = .native, .fence_digest = binding.owner_digest },
        .action = switch (action) {
            .fenced => .fenced,
            .reserved => .reserved,
            .admitted => return error.InvalidPrivateIndexNativeReceipt,
        },
        .position = .{ .native = cut.position },
        .candidate_digest = binding.integrity_candidate,
        .reservation_digest = binding.integrity_reserved,
        .evidence_digest = cut.evidence,
    };
}

/// Validate the logical proof against its exact physical cut. This establishes
/// internal consistency, not durable existence, enrollment or live authority.
pub fn validateNativeReceipt(binding: Binding, action: NativeEffect, position: positions.Native, receipt: publication.Receipt) !void {
    const expected = try logicalReceipt(binding, action, .{ .position = position, .evidence = try nativeEvidence(binding, action) });
    _ = receipt.digest() catch return error.InvalidPrivateIndexNativeReceipt;
    if (!std.meta.eql(expected, receipt)) return error.InvalidPrivateIndexNativeReceipt;
}

/// Fetch a previously committed native proof without inventing a new cut on
/// retry. Metadata admission/phase authority is independently checked by the
/// receiving worker; local state and a checksum alone cannot grant it.
pub fn nativeReceipt(txn: anytype, binding: Binding, action: NativeEffect) !publication.Receipt {
    const current = try requireCurrentJob(txn, binding);
    try inputs_mod.require(txn, current.inputs_digest);
    if (action == .reserved and current.phase != .reserved) return error.InvalidPrivateIndexNativeReceipt;
    return logicalReceipt(binding, action, try requireNativeCut(txn, binding, action));
}

fn requireCurrentJob(txn: anytype, binding: Binding) !Binding {
    const current = (try load(txn)) orelse return error.PrivateIndexJobRequired;
    if (!std.mem.eql(u8, &try nativeContext(current), &try nativeContext(binding))) return error.PrivateIndexOwnerJobChanged;
    try requireActive(txn, binding.fence);
    if (current.phase == .installed) return error.PrivateIndexPublicationRequired;
    if (current.phase == .reserved) {
        try requireReservedState(txn, binding);
    } else {
        try requireNativeCuts(txn, current);
        const integrity = (try optional(txn, integrity_catalog.key)) orelse return error.IntegrityCatalogChanged;
        if (!std.mem.eql(u8, &digest(integrity), &binding.integrity_before)) return error.IntegrityCatalogChanged;
        const physical = if (try optional(txn, physical_catalog.head_key)) |bytes| try physical_catalog.Head.decode(bytes) else null;
        if (binding.physical_before) |expected| {
            if (physical == null or !physical.?.eql(expected)) return error.PreparedGenerationChanged;
        } else if (physical != null) return error.PreparedGenerationChanged;
    }
    const schema_bytes = (try optional(txn, schema.schema_key)) orelse return error.RelationalIndexCatalogSchemaMismatch;
    if (!std.mem.eql(u8, &digest(schema_bytes), &binding.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
    return current;
}

/// Admission is not a drained-fence proof. Seal that proof in a separate
/// transaction only after every predecessor participant has resolved.
pub fn stageNativeFenced(txn: anytype, manager: *transactions.TxnManager, binding: Binding) !publication.Receipt {
    return stageNativeFencedWithReplay(txn, manager, binding, null);
}

/// Replay positions must originate in the authenticated standby frame, never
/// in a coordinator's effect request. A retry must match the original cut even
/// when the native clock has subsequently advanced.
pub fn stageNativeFencedWithReplay(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Native) !publication.Receipt {
    return stageNativeFencedImpl(txn, manager, binding, replay, false);
}

/// This proves only the drained fence, never complete cancellation/cleanup.
pub fn stageCanceledFencedWithReplay(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Native) !publication.Receipt {
    return stageNativeFencedImpl(txn, manager, binding, replay, true);
}

fn stageNativeFencedImpl(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Native, comptime canceling: bool) !publication.Receipt {
    try validateReplay(binding, replay);
    if (canceling) try cancellation.requireCanceled(txn, binding) else try cancellation.requireOpen(txn, binding);
    const current = try requireCurrentJob(txn, binding);
    // Integrity must fail before staging a new cut, not only when the caller
    // subsequently reconstructs its receipt after commit.
    try inputs_mod.require(txn, current.inputs_digest);
    _ = try requireNativeCut(txn, binding, .admitted);
    if (try readNativeCut(txn, binding, .fenced)) |existing| {
        try requireReplay(existing, replay);
        return logicalReceipt(binding, .fenced, existing);
    }
    if (current.phase != .fencing) return error.InvalidPrivateIndexNativeReceipt;
    try topology.requireDrained(txn, manager, binding.fence);
    return logicalReceipt(binding, .fenced, try stageNativeCut(txn, binding, .fenced, replay));
}

pub fn requireFencing(txn: anytype, fence: topology.Fence) !void {
    const binding = (try load(txn)) orelse return error.PrivateIndexJobRequired;
    if (binding.phase != .fencing or !binding.fence.eql(fence)) return error.PrivateIndexOwnerJobChanged;
    try requireNamespace(txn, binding);
}

pub fn requireActive(txn: anytype, fence: topology.Fence) !void {
    const binding = (try load(txn)) orelse {
        // The never-visited cancellation phase has a source fence but no
        // schema-sized worker binding yet. Only its exact durable native
        // intent can authorize that intermediate state.
        if (try cancellation.load(txn) == null) return error.PrivateIndexJobRequired;
        try cancellation.requirePendingFence(txn, fence);
        return;
    };
    if (!binding.fence.eql(fence)) return error.PrivateIndexOwnerJobChanged;
    try requireNamespace(txn, binding);
}

/// Page authority uses the durable fixed-size job/fence and current catalog
/// identities, not the public write snapshot. The owned candidate plan was
/// independently compiled/reloaded before page preparation.
pub fn requireReserved(txn: anytype, binding: Binding) !void {
    try cancellation.requireOpen(txn, binding);
    try requireReservedState(txn, binding);
}

pub fn requireCanceledReserved(txn: anytype, binding: Binding) !void {
    try cancellation.requireCanceled(txn, binding);
    try requireReservedState(txn, binding);
}

fn requireReservedState(txn: anytype, binding: Binding) !void {
    var expected = binding;
    expected.phase = .reserved;
    const current = (try load(txn)) orelse return error.PrivateIndexJobRequired;
    if (!std.mem.eql(u8, &try current.encode(), &try expected.encode())) return error.PrivateIndexOwnerJobChanged;
    try requireNamespace(txn, binding);
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
    const raw = (try optional(txn, integrity_catalog.key)) orelse return error.IntegrityCatalogChanged;
    if (!std.mem.eql(u8, &digest(raw), &binding.integrity_reserved)) return error.IntegrityCatalogChanged;
    const physical = try physical_catalog.Head.decode(try txn.get(physical_catalog.head_key));
    if (!physical.eql(binding.physical_reserved)) return error.PreparedGenerationChanged;
    const schema_bytes = (try optional(txn, schema.schema_key)) orelse return error.RelationalIndexCatalogSchemaMismatch;
    if (!std.mem.eql(u8, &digest(schema_bytes), &binding.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
    try requireNativeCuts(txn, current);
    _ = try @import("relational_index_private_claims.zig").requireRegistered(txn, binding);
}

/// Cold startup/restore validation in the SAME snapshot as the public catalog.
/// Structural recovery is not metadata authentication or permission to publish.
/// Work is bounded by catalog size, never the number of primary rows.
pub fn verifyRecovery(alloc: std.mem.Allocator, txn: anytype, active: ?*const physical_catalog.Loaded) !void {
    try @import("relational_index_release.zig").Record.verifyRecovery(alloc, txn);
    try @import("relational_index_release.zig").CancellationRecord.verifyRecovery(txn);
    const binding = try load(txn);
    try cancellation.verifyRecovery(txn, binding);
    _ = try loadCancellationCleanup(txn, binding);
    try Canceled.verifyRecovery(txn, binding);
    const fence = if (try optional(txn, topology.fence_key)) |bytes| try topology.Fence.decode(bytes) else null;
    if (binding == null) {
        if (fence) |value| if (value.role == .index_build) {
            if (try cancellation.load(txn) == null) return error.PrivateIndexJobRequired;
            try cancellation.requirePendingFence(txn, value);
        };
        if (try optional(txn, inputs_mod.key) != null) return error.InvalidPrivateIndexInputs;
        return;
    }
    const job = binding.?;
    try requireNamespace(txn, job);
    if (fence == null) return error.IntegrityTopologyFenceMissing;
    if (!fence.?.eql(job.fence)) return error.IntegrityTopologyChanged;
    try requireNativeCuts(txn, job);
    const installation = @import("relational_index_validation.zig").Installed;
    if (job.phase == .installed) {
        try installation.verifyRecovery(alloc, txn, job, active);
        return;
    }
    if (try installation.load(alloc, txn, job) != null) return error.InvalidPrivateIndexOwnerJob;
    try verifyUninstalledRecovery(alloc, txn, job, active);
}

/// Read-only proof verification against an explicitly retained predecessor.
/// Never grants page execution and cannot recursively select installed state.
pub fn verifyUninstalledRecovery(alloc: std.mem.Allocator, txn: anytype, job: Binding, active: ?*const physical_catalog.Loaded) !void {
    if (job.phase == .installed) return error.InvalidPrivateIndexOwnerJob;
    try requireNamespace(txn, job);
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(job.fence)) return error.IntegrityTopologyChanged;
    try requireNativeCuts(txn, job);
    var prepared_candidate = try reopenCandidate(alloc, txn, job);
    defer prepared_candidate.deinit();
    if (try topology.completed(txn)) |completed| {
        if (completed.admission_epoch >= job.fence.admission_epoch) return error.PrivateIndexPublicationRequired;
    }
    const schema_bytes = (try optional(txn, schema.schema_key)) orelse return error.RelationalIndexCatalogSchemaMismatch;
    if (!std.mem.eql(u8, &digest(schema_bytes), &job.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
    const integrity = (try optional(txn, integrity_catalog.key)) orelse return error.IntegrityCatalogChanged;
    const expected_integrity = if (job.phase == .fencing) job.integrity_before else job.integrity_reserved;
    if (!std.mem.eql(u8, &digest(integrity), &expected_integrity)) return error.IntegrityCatalogChanged;
    switch (job.phase) {
        .installed => unreachable, // Verified against the real successor above.
        .fencing => {
            if (job.physical_before) |before| {
                if (active == null or !active.?.head.eql(before)) return error.PreparedGenerationChanged;
            } else if (active != null) return error.PreparedGenerationChanged;
        },
        .reserved => {
            _ = try @import("relational_index_private_claims.zig").requireRegistered(txn, job);
            const current = active orelse return error.PreparedGenerationChanged;
            if (!current.head.eql(job.physical_reserved)) return error.PreparedGenerationChanged;
            const blob = (try optional(txn, &physical_catalog.blobKey(job.physical_candidate.blob_digest))) orelse return error.MissingRelationalIndexDefinitions;
            var candidate = try physical_catalog.decode(alloc, job.physical_candidate, blob);
            defer candidate.deinit();
            if (candidate.entries().len != current.entries().len + 1) return error.PrivateIndexOwnerJobChanged;
            var old_offset: usize = 0;
            var found = false;
            for (candidate.entries()) |entry| {
                if (entry.id().mapKey() == job.index.mapKey()) {
                    if (found) return error.PrivateIndexOwnerJobChanged;
                    found = true;
                    continue;
                }
                if (old_offset >= current.entries().len) return error.PrivateIndexOwnerJobChanged;
                const previous = current.entries()[old_offset];
                if (entry.generation >= job.physical_reserved.revision or
                    entry.id().mapKey() != previous.id().mapKey() or
                    !std.meta.eql(entry.comparison, previous.comparison) or
                    !std.mem.eql(u8, &try @import("../relational_index.zig").definitionDigest(entry.definition), &try @import("../relational_index.zig").definitionDigest(previous.definition))) return error.PrivateIndexOwnerJobChanged;
                old_offset += 1;
            }
            if (!found or old_offset != current.entries().len) return error.PrivateIndexOwnerJobChanged;
        },
    }
    const pages = @import("relational_index_jobs.zig");
    try @import("relational_index_owner_control.zig").PreparedBuild.verifyRecovery(txn, job);
    try private_claims.InventoryValidation.verifyRecovery(alloc, txn, job);
    try @import("relational_index_validation.zig").verifyRecovery(alloc, txn, job);
    // Decode borrowed progress last: another store read may invalidate it.
    const owner = try pages.ownership(txn);
    if (try optional(txn, &pages.progressKey(job.index))) |bytes| {
        if (job.phase == .fencing) return error.InvalidRelationalIndexProgress;
        const progress = try pages.Progress.decode(bytes);
        if (progress.id.mapKey() != job.index.mapKey() or progress.phase != .primary or
            !std.mem.eql(u8, &progress.owner, &owner)) return error.InvalidRelationalIndexProgress;
    }
}

/// Cold owner recovery retains the originally absent/existing predecessor from
/// the durable binding. An empty bootstrap catalog must not be mistaken for a
/// catalog that existed before the job's admission.
pub fn reopenPhysical(alloc: std.mem.Allocator, txn: anytype, before: @import("schema_registry.zig").SchemaView, after: @import("schema_registry.zig").SchemaView, name: []const u8) !physical_catalog.PrivateAddition {
    const binding = (try load(txn)) orelse return error.PrivateIndexJobRequired;
    if (binding.phase != .reserved) return error.PrivateIndexOwnerJobChanged;
    const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
    const raw = (try optional(txn, integrity_catalog.key)) orelse return error.IntegrityCatalogChanged;
    if (!std.mem.eql(u8, &digest(raw), &binding.integrity_reserved)) return error.IntegrityCatalogChanged;
    var physical = try physical_catalog.PrivateAddition.reopen(alloc, txn, before, after, binding.physical_reserved, binding.physical_candidate, name);
    errdefer physical.deinit();
    if (physical.index.mapKey() != binding.index.mapKey()) return error.PrivateIndexOwnerJobChanged;
    physical.reservation.expected = binding.physical_before;
    return physical;
}

/// Metadata authority and local range identity must already be independently
/// attested by the ordered owner. Fence and binding commit atomically; old
/// participants may finish but no new participants may enter after this commit.
pub fn stageBegin(txn: anytype, binding: Binding, inputs: *const inputs_mod.Prepared) !bool {
    return stageBeginWithReplay(txn, binding, inputs, null);
}

/// The ordered owner stages the forwarded clock and admitted job in the same
/// transaction. Errors require aborting that transaction, including when the
/// forwarded clock is not the exact successor of the local durable clock.
pub fn stageBeginWithReplay(txn: anytype, binding: Binding, inputs: *const inputs_mod.Prepared, replay: ?positions.Native) !bool {
    return stageBeginImpl(txn, binding, inputs, replay, false);
}

/// A never-visited cancellation still needs an exact admitted fence and burns.
/// Its durable intent must precede this operation and closes normal retries.
pub fn stageCanceledBeginWithReplay(txn: anytype, binding: Binding, inputs: *const inputs_mod.Prepared, replay: ?positions.Native) !bool {
    return stageBeginImpl(txn, binding, inputs, replay, true);
}

fn stageBeginImpl(txn: anytype, binding: Binding, inputs: *const inputs_mod.Prepared, replay: ?positions.Native, comptime canceling: bool) !bool {
    try validateReplay(binding, replay);
    if (binding.phase != .fencing) return error.InvalidPrivateIndexOwnerJob;
    const encoded = try binding.encode();
    _ = try inputs_mod.decode(inputs.bytes);
    if (!std.mem.eql(u8, &inputs_mod.digest(inputs.bytes), &binding.inputs_digest)) return error.InvalidPrivateIndexInputs;
    try requireNamespace(txn, binding);
    if (canceling) try cancellation.requireCanceled(txn, binding) else try cancellation.requireOpen(txn, binding);
    if (try load(txn)) |existing| {
        var original = existing;
        original.phase = .fencing;
        if (!std.mem.eql(u8, &try original.encode(), &encoded)) return error.PrivateIndexOwnerJobChanged;
        const stored_inputs = (try optional(txn, inputs_mod.key)) orelse return error.InvalidPrivateIndexInputs;
        if (!std.mem.eql(u8, stored_inputs, inputs.bytes)) return error.InvalidPrivateIndexInputs;
        const fence = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
        if (!fence.eql(binding.fence)) return error.IntegrityTopologyChanged;
        try requireNativeCuts(txn, existing);
        if (replay != null) try requireReplay(try requireNativeCut(txn, binding, .admitted), replay);
        return false;
    }
    try @import("relational_index_release.zig").Record.requireRetirementClear(txn);
    const integrity = (try optional(txn, integrity_catalog.key)) orelse return error.IntegrityCatalogChanged;
    if (!std.mem.eql(u8, &digest(integrity), &binding.integrity_before)) return error.IntegrityCatalogChanged;
    const physical = if (try optional(txn, physical_catalog.head_key)) |bytes| try physical_catalog.Head.decode(bytes) else null;
    if (binding.physical_before) |expected| {
        if (physical == null or !physical.?.eql(expected)) return error.PreparedGenerationChanged;
    } else if (physical != null) return error.PreparedGenerationChanged;
    const schema_bytes = (try optional(txn, schema.schema_key)) orelse return error.RelationalIndexCatalogSchemaMismatch;
    if (!std.mem.eql(u8, &digest(schema_bytes), &binding.schema_before)) return error.RelationalIndexCatalogSchemaMismatch;
    if (try optional(txn, inputs_mod.key) != null) return error.InvalidPrivateIndexInputs;
    try txn.put(inputs_mod.key, inputs.bytes);
    try txn.put(key, &encoded);
    try topology.stagePrivateIndexBegin(txn, binding.fence);
    if (binding.authority == .native) _ = try stageNativeCut(txn, binding, .admitted, replay);
    return true;
}

/// Require the exact durable fence AND drained predecessor transactions before
/// burning either allocation. Both catalogs, private definitions and job phase
/// share the caller's single-writer transaction and durable outbox.
pub fn stageReservation(txn: anytype, manager: *transactions.TxnManager, binding: Binding, candidate: *const candidate_mod.Candidate, physical: *const physical_catalog.PrivateAddition) !bool {
    return stageReservationWithReplay(txn, manager, binding, candidate, physical, null);
}

/// Both allocation burns and the forwarded native cut share the caller's
/// transaction/outbox. Duplicate replay never allocates a new generation.
pub fn stageReservationWithReplay(txn: anytype, manager: *transactions.TxnManager, binding: Binding, candidate: *const candidate_mod.Candidate, physical: *const physical_catalog.PrivateAddition, replay: ?positions.Native) !bool {
    return stageReservationImpl(txn, manager, binding, candidate, physical, replay, false);
}

/// Burns both allocation spaces under the exact persisted cancellation intent.
/// These burns are not proof that artifacts/claims have been cleaned up.
pub fn stageCanceledReservationWithReplay(txn: anytype, manager: *transactions.TxnManager, binding: Binding, candidate: *const candidate_mod.Candidate, physical: *const physical_catalog.PrivateAddition, replay: ?positions.Native) !bool {
    return stageReservationImpl(txn, manager, binding, candidate, physical, replay, true);
}

fn stageReservationImpl(txn: anytype, manager: *transactions.TxnManager, binding: Binding, candidate: *const candidate_mod.Candidate, physical: *const physical_catalog.PrivateAddition, replay: ?positions.Native, comptime canceling: bool) !bool {
    try validateReplay(binding, replay);
    if (canceling) try cancellation.requireCanceled(txn, binding) else try cancellation.requireOpen(txn, binding);
    const expected = try prepare(binding.id, binding.plan_digest, binding.owner_digest, binding.ordinal, binding.fence, binding.authority, candidate, physical);
    var original = binding;
    original.phase = .fencing;
    if (!std.mem.eql(u8, &try original.encode(), &try expected.encode())) return error.PrivateIndexOwnerJobChanged;
    const current = (try load(txn)) orelse return error.PrivateIndexJobRequired;
    var predecessor = current;
    predecessor.phase = .fencing;
    if (!std.mem.eql(u8, &try predecessor.encode(), &try expected.encode())) return error.PrivateIndexOwnerJobChanged;
    try topology.requireDrained(txn, manager, binding.fence);
    try requireNamespace(txn, binding);
    try requireNativeCuts(txn, current);
    if (binding.authority == .native) _ = try requireNativeCut(txn, binding, .fenced);
    if (current.phase == .reserved) {
        const raw = (try optional(txn, integrity_catalog.key)) orelse return error.IntegrityCatalogChanged;
        if (!std.mem.eql(u8, &digest(raw), &binding.integrity_reserved)) return error.IntegrityCatalogChanged;
        const head = try physical_catalog.Head.decode(try txn.get(physical_catalog.head_key));
        if (!head.eql(binding.physical_reserved)) return error.PreparedGenerationChanged;
        const blob = try txn.get(&physical_catalog.blobKey(binding.physical_candidate.blob_digest));
        if (!std.mem.eql(u8, blob, physical.candidate.blob)) return error.ImmutableRelationalIndexDefinitionConflict;
        if (binding.authority == .native) try requireReplay(try requireNativeCut(txn, binding, .reserved), replay);
        _ = try @import("relational_index_private_claims.zig").requireRegistered(txn, binding);
        return false;
    }
    try candidate.reservation.stage(txn);
    _ = try physical.stageReservation(txn);
    var next = expected;
    next.phase = .reserved;
    try txn.put(key, &try next.encode());
    try @import("relational_index_private_claims.zig").stageRegistration(txn, next);
    if (binding.authority == .native) _ = try stageNativeCut(txn, binding, .reserved, replay);
    return true;
}

/// Caller commits this intent/clock with its authenticated durable outbox. The
/// returned cut is intent evidence, NOT a canceled publication receipt.
pub fn stageCancellationIntentWithReplay(txn: anytype, intent: cancellation.Intent, replay: ?positions.Native) !cancellation.Stage {
    try intent.validate();
    const binding = try load(txn);
    if (binding) |current| _ = try requireCurrentJob(txn, current);
    return cancellation.stage(txn, intent, binding, replay);
}

pub fn stageCancellationIntentAt(txn: anytype, intent: cancellation.Intent, position: positions.Position) !cancellation.Stage {
    try intent.validate();
    try position.validate();
    const binding = try load(txn);
    if (binding) |current| _ = try requireCurrentJob(txn, current);
    return cancellation.stageAt(txn, intent, binding, position, if (position == .native) .native else .raft);
}

fn loadCancellationCleanup(txn: anytype, binding: ?Binding) !?CancellationCleanupRecord {
    // Decode before any subsequent store read invalidates borrowed bytes.
    const record = try CancellationCleanupRecord.decode((try optional(txn, cancellation_cleanup_key)) orelse return null);
    try cancellation.requireCommitted(txn, record.intent.fence.namespace, record.position);
    if (binding) |current| if (record.intent.eql(cancellation.Intent.fromBinding(current))) {
        if (current.phase != .reserved) return error.InvalidPrivateIndexCancellation;
        try cancellation.requireCanceled(txn, current);
        const intent = (try cancellation.load(txn)).?;
        const reserved: positions.Position = if (current.authority == .native)
            .{ .native = (try requireNativeCut(txn, current, .reserved)).position }
        else
            (try @import("relational_index_raft_control.zig").retainedPosition(txn, current, .reserved)) orelse return error.InvalidPrivateIndexCancellation;
        if (!record.position.strictlyAfter(intent.position) or !record.position.strictlyAfter(reserved))
            return error.InvalidPrivateIndexCancellation;
        const allocation = try integrity_catalog.privateIndexAllocationFromReservation(try txn.get(integrity_catalog.key), current.integrity_reserved, current.fence.namespace.table_id);
        if (record.claim_allocation != allocation.allocation) return error.InvalidPrivateIndexCancellation;
        return record;
    };
    // An old slot is inert only after its exact epoch has been consumed.
    const completed = (try topology.completed(txn)) orelse return error.InvalidPrivateIndexCancellation;
    if (completed.admission_epoch < record.intent.fence.admission_epoch or
        (completed.admission_epoch == record.intent.fence.admission_epoch and !completed.eql(record.intent.fence)))
        return error.InvalidPrivateIndexCancellation;
    if (binding) |current| if (current.fence.admission_epoch <= record.intent.fence.admission_epoch)
        return error.PrivateIndexOwnerJobChanged;
    return null;
}

/// Queue only this canceled, drained and burned generation in the shared GC
/// state machine. The caller must commit its outbox atomically and abort on
/// every error. Neither queue admission nor its cut is a canceled owner ACK.
pub fn stageCancellationCleanupWithReplay(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Native) !CancellationCleanup {
    if (binding.authority != .native) return error.InvalidControlReceiptPosition;
    const result = try stageCancellationCleanupAt(txn, manager, binding, if (replay) |cut| .{ .native = cut } else null);
    return .{ .position = result.position.native, .duplicate = result.duplicate };
}

pub fn stageCancellationCleanupAt(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Position) !OrderedCancellationCleanup {
    if (replay) |cut| {
        try cut.requireNamespace(binding.fence.namespace);
        if ((cut == .native) != (binding.authority == .native)) return error.InvalidControlReceiptPosition;
    } else if (binding.authority == .raft) return error.InvalidControlReceiptPosition;
    try cancellation.requireCanceled(txn, binding);
    try requireReservedState(txn, binding);
    try topology.requireDrained(txn, manager, binding.fence);
    const gc = @import("relational_index_gc.zig");
    const queue_key = gc.key(binding.index);
    if (try loadCancellationCleanup(txn, try load(txn))) |record| {
        if (binding.authority == .native) {
            if (replay) |cut| if (!std.meta.eql(cut, record.position)) return error.InvalidControlReceiptPosition;
        }
        if (try optional(txn, &queue_key)) |bytes| _ = try gc.decode(binding.index, bytes);
        return .{ .position = record.position, .duplicate = true };
    }
    if (try optional(txn, &queue_key) != null) return error.InvalidPrivateIndexCancellation;
    const allocation = try integrity_catalog.privateIndexAllocationFromReservation(try txn.get(integrity_catalog.key), binding.integrity_reserved, binding.fence.namespace.table_id);
    const position: positions.Position = if (binding.authority == .native)
        .{ .native = .{ .namespace = binding.fence.namespace, .sequence = try source_authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), if (replay) |value| value.native.sequence else null) } }
    else blk: {
        const prior = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
        const cut = replay.?;
        if (!cut.strictlyAfter(.{ .raft = .{ .term = prior.term, .index = prior.index } })) return error.InvalidControlReceiptPosition;
        break :blk cut;
    };
    const record: CancellationCleanupRecord = .{ .intent = cancellation.Intent.fromBinding(binding), .position = position, .claim_allocation = allocation.allocation };
    try txn.put(cancellation_cleanup_key, &try record.encode());
    try txn.put(&queue_key, &gc.initial(binding.index));
    try @import("relational_index_private_claims.zig").SourceStream.stageCanceled(txn, binding);
    try txn.delete(&private_claims.InventoryValidation.key(binding));
    try txn.delete(&@import("relational_index_validation.zig").key(binding));
    try txn.delete(@import("relational_index_validation.zig").Seal.key);
    // No page can resume after this exact cancellation has queued its data.
    // Retire the bounded retry slot before GC removes the progress it binds.
    try txn.delete(@import("relational_index_owner_control.zig").PreparedBuild.checkpoint_key);
    return .{ .position = position, .duplicate = false };
}

/// Bounded physical-artifact verification only. Global claims, final binding
/// retirement and epoch release still need independent completion; callers
/// must not turn this check into a signed canceled publication receipt.
pub fn requireCancellationCleanup(txn: anytype, binding: Binding) !positions.Position {
    try cancellation.requireCanceled(txn, binding);
    try requireReservedState(txn, binding);
    const record = (try loadCancellationCleanup(txn, try load(txn))) orelse return error.PrivateIndexCancellationRequired;
    return record.position;
}

pub fn requireCancellationArtifactsRetired(txn: anytype, binding: Binding) !void {
    _ = try requireCancellationCleanup(txn, binding);
    const gc = @import("relational_index_gc.zig");
    if (try optional(txn, &gc.key(binding.index)) != null or
        try optional(txn, @import("relational_index_private_claims.zig").SourceStream.head_key) != null or
        try optional(txn, &@import("relational_index_jobs.zig").progressKey(binding.index)) != null or
        try optional(txn, &@import("relational_index_maintenance_contract.zig").controlKey(binding.index)) != null)
        return error.PrivateIndexCancellationPending;
    var cursor = try txn.openCursor();
    defer cursor.close();
    for ([_][records.forward_prefix_len]u8{ try records.forwardPrefix(binding.index), try records.ownershipPrefix(binding.index) }) |prefix| {
        if (try cursor.seekAtOrAfter(&prefix)) |entry| if (std.mem.startsWith(u8, entry.key, &prefix))
            return error.PrivateIndexCancellationPending;
    }
}

/// Durable cleanup completion, distinct from both queue admission and cohort
/// fence release. All identity/cut data is owned and fixed-size; no schema or
/// JSON parsing is necessary under the serialized owner transaction.
pub const Canceled = struct {
    pub const key = cancellation_cleanup_key ++ ":complete";
    pub const encoded_len = 1216;
    binding: Binding,
    cleanup: CancellationCleanupRecord,
    intent_position: positions.Position,
    position: positions.Position,

    pub fn validate(self: @This()) !void {
        _ = try self.binding.encode();
        _ = try self.cleanup.encode();
        try self.intent_position.requireNamespace(self.binding.fence.namespace);
        try self.position.requireNamespace(self.binding.fence.namespace);
        if ((self.position == .native) != (self.binding.authority == .native) or self.binding.phase != .reserved or
            !self.cleanup.intent.eql(cancellation.Intent.fromBinding(self.binding)) or
            !self.cleanup.position.strictlyAfter(self.intent_position) or
            !self.position.strictlyAfter(self.cleanup.position)) return error.InvalidPrivateIndexCancellation;
    }

    pub fn encode(self: @This()) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..8], "AICC\x02\x00\x00\x00");
        @memcpy(bytes[8..696], &try self.binding.encode());
        @memcpy(bytes[696..1112], &try self.cleanup.encode());
        @memcpy(bytes[1112..1145], &try self.intent_position.encode());
        @memcpy(bytes[1145..1178], &try self.position.encode());
        @memcpy(bytes[1184..1216], &digest(bytes[0..1184]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..8], "AICC\x02\x00\x00\x00") or
            !std.mem.allEqual(u8, bytes[1178..1184], 0) or
            !std.mem.eql(u8, bytes[1184..1216], &digest(bytes[0..1184]))) return error.InvalidPrivateIndexCancellation;
        const intent_position = try positions.Position.decode(bytes[1112..1145]);
        const position = try positions.Position.decode(bytes[1145..1178]);
        const result: @This() = .{ .binding = try Binding.decode(bytes[8..696]), .cleanup = try CancellationCleanupRecord.decode(bytes[696..1112]), .intent_position = intent_position, .position = position };
        try result.validate();
        return result;
    }

    pub fn receipt(self: @This()) !publication.Receipt {
        const bytes = try self.encode();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/private-index-canceled/v1");
        hash.update(&bytes);
        var evidence: Digest = undefined;
        hash.final(&evidence);
        const result: publication.Receipt = .{
            .owner = .{ .plan_id = self.binding.id, .plan_digest = self.binding.plan_digest, .ordinal = self.binding.ordinal, .group_id = self.binding.fence.owner_group_id, .namespace = self.binding.fence.namespace, .fence_digest = self.binding.owner_digest, .authority = self.binding.authority },
            .action = .canceled,
            .position = self.position,
            .candidate_digest = self.binding.integrity_candidate,
            .reservation_digest = self.binding.integrity_reserved,
            .evidence_digest = evidence,
        };
        _ = try result.digest();
        return result;
    }

    pub fn load(txn: anytype) !?@This() {
        const record = try decode((try optional(txn, Canceled.key)) orelse return null);
        try cancellation.requireCommitted(txn, record.binding.fence.namespace, record.position);
        return record;
    }

    pub fn requireCurrent(self: @This(), txn: anytype) !void {
        try self.validate();
        try requireCancellationArtifactsRetired(txn, self.binding);
        try private_claims.requireRetired(txn, self.binding);
        const actual = (try loadCancellationCleanup(txn, self.binding)) orelse return error.PrivateIndexCancellationRequired;
        const intent = (try cancellation.load(txn)) orelse return error.PrivateIndexCancellationRequired;
        if (!std.meta.eql(actual, self.cleanup) or !std.meta.eql(intent.position, self.intent_position)) return error.PrivateIndexOwnerJobChanged;
    }

    /// The caller stages its replay marker and durable transport record in this
    /// same transaction. Even after every local artifact is gone, this never
    /// lifts a fence or retires the active job before metadata's cohort barrier.
    pub const Stage = struct { record: Canceled, duplicate: bool };

    pub fn stage(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Native) !Stage {
        if (binding.authority != .native) return error.InvalidControlReceiptPosition;
        return stageAt(txn, manager, binding, if (replay) |cut| .{ .native = cut } else null);
    }

    pub fn stageAt(txn: anytype, manager: *transactions.TxnManager, binding: Binding, replay: ?positions.Position) !Stage {
        _ = try binding.encode();
        if (replay) |cut| {
            try cut.requireNamespace(binding.fence.namespace);
            if ((cut == .native) != (binding.authority == .native)) return error.InvalidControlReceiptPosition;
        } else if (binding.authority == .raft) return error.InvalidControlReceiptPosition;
        if (try Canceled.load(txn)) |previous| {
            if (std.meta.eql(previous.binding, binding)) {
                try previous.requireCurrent(txn);
                if (replay) |cut| if (!std.meta.eql(cut, previous.position) and
                    (binding.authority == .native or !cut.strictlyAfter(previous.position))) return error.InvalidControlReceiptPosition;
                return .{ .record = previous, .duplicate = true };
            }
            const completed = (try topology.completed(txn)) orelse return error.PrivateIndexCancellationPending;
            if (completed.admission_epoch < previous.binding.fence.admission_epoch or
                (completed.admission_epoch == previous.binding.fence.admission_epoch and !completed.eql(previous.binding.fence)) or
                binding.fence.admission_epoch <= previous.binding.fence.admission_epoch) return error.PrivateIndexOwnerJobChanged;
        }
        try requireCancellationArtifactsRetired(txn, binding);
        try private_claims.requireRetired(txn, binding);
        try topology.requireDrained(txn, manager, binding.fence);
        const intent = (try cancellation.load(txn)) orelse return error.PrivateIndexCancellationRequired;
        const cleanup = (try loadCancellationCleanup(txn, binding)) orelse return error.PrivateIndexCancellationRequired;
        const source = try source_authority.require(txn, if (binding.authority == .native) .native else .raft, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace));
        const position: positions.Position = if (binding.authority == .native)
            .{ .native = .{ .namespace = binding.fence.namespace, .sequence = std.math.add(u64, source.sequence, 1) catch return error.InvalidControlReceiptPosition } }
        else blk: {
            const prior = (try @import("apply_receipts.zig").readOrderedApplyReceiptTxn(txn)) orelse return error.InvalidControlReceiptPosition;
            const cut = replay.?;
            if (!cut.strictlyAfter(.{ .raft = .{ .term = prior.term, .index = prior.index } })) return error.InvalidControlReceiptPosition;
            break :blk cut;
        };
        const record: Canceled = .{ .binding = binding, .cleanup = cleanup, .intent_position = intent.position, .position = position };
        const encoded = try record.encode();
        if (replay) |cut| if (!std.meta.eql(cut, record.position)) return error.InvalidControlReceiptPosition;
        if (position == .native) _ = try source_authority.advance(txn, @import("online_source_contract.zig").namespaceBytes(binding.fence.namespace), position.native.sequence);
        try txn.put(Canceled.key, &encoded);
        return .{ .record = record, .duplicate = false };
    }

    pub fn verifyRecovery(txn: anytype, binding: ?Binding) !void {
        const record = (try Canceled.load(txn)) orelse return;
        if (binding) |current| if (std.meta.eql(current, record.binding)) return record.requireCurrent(txn);
        const completed = (try topology.completed(txn)) orelse return error.InvalidPrivateIndexCancellation;
        if (completed.admission_epoch < record.binding.fence.admission_epoch or
            (completed.admission_epoch == record.binding.fence.admission_epoch and !completed.eql(record.binding.fence))) return error.InvalidPrivateIndexCancellation;
    }
};
