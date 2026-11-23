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

//! Source evidence in the accepting row store. These helpers receive only
//! native-rejoined permission and actual owner identity; they mint neither.
//! The caller holds the DB apply fence and commits this view atomically with
//! intent installation or terminal row application. No provider executes here.
const std = @import("std");
pub const contract = @import("../../api/replication_source_apply.zig");
const Position = @import("receipt_position.zig").Position;
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const A = std.mem.Allocator;
const External = @import("../transaction_decision_owner.zig").External;
pub const family = "\x00\x00__metadata__:source_apply:v1:";
pub const maximum_record_bytes = 32 * 1024;

pub const PhysicalIdentity = struct {
    physical_root_incarnation: u128,
    namespace: Namespace,
    schema_version: u32,
    schema_digest: [32]u8,
};
pub const Preparation = struct {
    permit: contract.Permit,
    physical: PhysicalIdentity,
    pub fn validate(self: @This()) !void {
        try self.permit.validate();
        if (self.physical.physical_root_incarnation == 0 or
            !self.physical.namespace.eql(self.permit.namespace) or
            self.physical.schema_version != self.permit.schema_version or
            !std.mem.eql(u8, &self.physical.schema_digest, &self.permit.schema_digest)) return error.ReplicationCutoverAuthorityLost;
        if (self.permit.expected_image) |image| if (image.physical_root_incarnation != self.physical.physical_root_incarnation) return error.ReplicationCutoverAuthorityLost;
    }
};

/// The DB calls this while its actual pinned runtime schema is protected by
/// the apply fence. Match metadata's canonical runtime encoding, including
/// version zero document tables, rather than an older persisted ASCH frame.
pub fn preparationForSchema(a: A, permit: contract.Permit, current: @import("../schema.zig").TableSchema, physical_root: u128, namespace: Namespace) !Preparation {
    try permit.validate();
    if (current.version != permit.schema_version) return error.PreparedGenerationChanged;
    const encoded = try @import("../schema.zig").serializeSchema(a, current);
    defer a.free(encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(encoded, &digest, .{});
    const result: Preparation = .{ .permit = permit, .physical = .{
        .physical_root_incarnation = physical_root,
        .namespace = namespace,
        .schema_version = current.version,
        .schema_digest = digest,
    } };
    try result.validate();
    return result;
}

/// Borrowed exact journal bytes produced by the transaction manager AFTER
/// normalization and materialization. Never reconstructed API JSON. The caller
/// keeps this list and its buffers alive through the accepting batch commit.
pub const FrozenIntent = struct { key: []const u8, encoded_value: []const u8 };
pub const Prepared = struct {
    receipt: contract.ApplyReceipt,
    intent_revision: u64,
    intent_count: u64,
    intent_digest: [32]u8,
    pub fn validate(self: @This(), a: A) !void {
        try self.receipt.validate();
        if (self.receipt.stage != .prepared or self.intent_revision == 0 or std.mem.allEqual(u8, &self.intent_digest, 0)) return error.InvalidReplicationCutoverIntent;
        if (!std.mem.eql(u8, &self.receipt.marker_digest, &try preparedDigest(a, self))) return error.InvalidReplicationCutoverIntent;
    }
};

/// A stable per-event address. Changing permission or schema cannot hide an
/// existing preparation behind a different permission-derived key.
pub fn preparedKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    try permit.validate();
    return keyAlloc(a, "prepared:", try eventAddress(a, permit));
}
/// Closed hot standby admission: an authenticated PREPARE carries only this event's
/// frozen evidence and this target's image pin, never an entire key family.
pub fn isPreparedPostimageKey(a: A, permit: contract.Permit, key: []const u8) !bool {
    const prepared = try preparedKeyAlloc(a, permit);
    defer a.free(prepared);
    if (std.mem.eql(u8, prepared, key)) return true;
    const image = try imageKeyAlloc(a, permit);
    defer a.free(image);
    if (std.mem.eql(u8, image, key)) return true;
    const transaction = try transactionKeyAlloc(a, permit.reference.txn_id, permit.decision_owner, permit.namespace);
    defer a.free(transaction);
    return std.mem.eql(u8, transaction, key);
}
pub fn isAppliedPostimageKey(a: A, permit: contract.Permit, key: []const u8) !bool {
    const applied = try appliedKeyAlloc(a, permit);
    defer a.free(applied);
    if (std.mem.eql(u8, applied, key)) return true;
    const head = try headKeyAlloc(a, permit);
    defer a.free(head);
    return std.mem.eql(u8, head, key);
}
const Transaction = struct {
    version: u8 = 1,
    permit: contract.Permit,
    prepared_digest: ?[32]u8 = null,
    aborted_digest: ?[32]u8 = null,
};
pub fn transactionKeyAlloc(a: A, txn_id: [16]u8, owner: External, namespace: Namespace) ![]u8 {
    try owner.validate();
    if (std.mem.allEqual(u8, &txn_id, 0) or namespace.table_id == 0 or namespace.shard_id == 0 or namespace.range_id == 0) return error.InvalidReplicationCutoverIntent;
    return keyAlloc(a, "transaction:", try canonical(a, "antfly/source-physical-transaction-address/v1\x00", .{
        .transaction_id = txn_id,
        .metadata_incarnation = owner.metadata_incarnation,
        .operation_id = owner.operation_id,
        .namespace = namespace,
    }));
}

/// Cold terminal recovery has the actual transaction/P owner, not a replay of
/// the original Source request. This immutable point locator rejoins its exact
/// Prepared in the SAME view. The transaction manager's Source-required record
/// bit must treat absence as corruption, rather than downgrade to ordinary.
const JoinedTransaction = struct { locator: Transaction, prepared: ?Prepared };
fn joinTransaction(a: A, txn: anytype, txn_id: [16]u8, owner: External, namespace: Namespace) !?JoinedTransaction {
    const locator = (try read(Transaction, a, txn, try transactionKeyAlloc(a, txn_id, owner, namespace))) orelse return null;
    try locator.permit.validate();
    if (locator.version != 1 or !locator.permit.decision_owner.eql(owner) or !locator.permit.namespace.eql(namespace) or
        !std.mem.eql(u8, &locator.permit.reference.txn_id, &txn_id)) return error.ReplicationCutoverAuthorityLost;
    var prepared: ?Prepared = null;
    if (locator.prepared_digest) |commitment| {
        const value = (try readPrepared(a, txn, locator.permit)) orelse return error.ReplicationCutoverAuthorityLost;
        if (!std.mem.eql(u8, &value.receipt.marker_digest, &commitment)) return error.ReplicationCutoverAuthorityLost;
        prepared = try requirePrepared(a, txn, locator.permit, value.receipt.image.physical_root_incarnation, value.intent_revision);
    }
    if (locator.aborted_digest) |commitment| {
        const aborted = (try readAborted(a, txn, locator.permit)) orelse return error.ReplicationCutoverAuthorityLost;
        if (!std.mem.eql(u8, &aborted.marker_digest, &commitment)) return error.ReplicationCutoverAuthorityLost;
    } else if (prepared == null) return error.ReplicationCutoverAuthorityLost;
    return .{ .locator = locator, .prepared = prepared };
}
pub fn preparedForTransaction(a: A, txn: anytype, txn_id: [16]u8, owner: External, namespace: Namespace) !?Prepared {
    return if (try joinTransaction(a, txn, txn_id, owner, namespace)) |joined| joined.prepared else null;
}
/// Recover the exact original native permission for terminal ACK/cleanup in
/// the caller's existing read cut. A no-PREPARE ABORT has genuine custody but
/// no Prepared; verify its retained fence and image rather than invent one.
/// The transaction manager must reject null when its Source-required bit is set.
pub fn permitForTransaction(a: A, txn: anytype, txn_id: [16]u8, owner: External, namespace: Namespace) !?contract.Permit {
    return if (try joinTransaction(a, txn, txn_id, owner, namespace)) |joined| joined.locator.permit else null;
}

fn appliedKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    return keyAlloc(a, "applied:", try eventAddress(a, permit));
}
pub fn activatedKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    return keyAlloc(a, "activated:", try eventAddress(a, permit));
}

pub fn readActivated(a: A, txn: anytype, permit: contract.Permit) !?contract.ApplyReceipt {
    const receipt = (try read(contract.ApplyReceipt, a, txn, try activatedKeyAlloc(a, permit))) orelse return null;
    try receipt.validate();
    const prepared = (try readPrepared(a, txn, permit)) orelse return error.ReplicationCutoverAuthorityLost;
    if (receipt.stage != .activated or !std.meta.eql(receipt.permit, permit) or
        !std.meta.eql(receipt.image, prepared.receipt.image) or
        !receipt.position.strictlyAfter(prepared.receipt.position) or
        !std.mem.eql(u8, &receipt.marker_digest, &prepared.receipt.marker_digest)) return error.ReplicationCutoverAuthorityLost;
    return receipt;
}

pub fn activationWrite(a: A, txn: anytype, prepared: Prepared, position: Position, physical_root: u128) !?@import("../docstore.zig").KVPair {
    if (prepared.receipt.image.physical_root_incarnation != physical_root) return error.ReplicationCutoverAuthorityLost;
    const permit = prepared.receipt.permit;
    try position.requireNamespace(permit.namespace);
    if (!position.strictlyAfter(prepared.receipt.position)) return error.ReplicationCutoverAuthorityLost;
    if (try readActivated(a, txn, permit)) |existing| {
        if (!std.meta.eql(existing.position, position) and !position.strictlyAfter(existing.position))
            return error.ReplicationCutoverAuthorityLost;
        return null;
    }
    var receipt = prepared.receipt;
    receipt.stage = .activated;
    receipt.position = position;
    return .{ .key = try activatedKeyAlloc(a, permit), .value = try encode(a, receipt) };
}
fn abortedKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    return keyAlloc(a, "aborted:", try eventAddress(a, permit));
}
pub fn isAbortedPostimageKey(a: A, permit: contract.Permit, key: []const u8) !bool {
    const aborted = try abortedKeyAlloc(a, permit);
    defer a.free(aborted);
    if (std.mem.eql(u8, aborted, key)) return true;
    const image = try imageKeyAlloc(a, permit);
    defer a.free(image);
    if (std.mem.eql(u8, image, key)) return true;
    const transaction = try transactionKeyAlloc(a, permit.reference.txn_id, permit.decision_owner, permit.namespace);
    defer a.free(transaction);
    return std.mem.eql(u8, transaction, key);
}
fn headKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    return keyAlloc(a, "head:", try targetAddress(a, permit));
}
fn imageKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    return keyAlloc(a, "image:", try targetAddress(a, permit));
}
fn keyAlloc(a: A, kind: []const u8, digest: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}{x}", .{ family, kind, &digest });
}
fn eventAddress(a: A, permit: contract.Permit) ![32]u8 {
    return canonical(a, "antfly/source-physical-event-address/v1\x00", .{
        .metadata_group = permit.semantic.metadata_group,
        .metadata_incarnation = permit.semantic.metadata_incarnation,
        .owner = permit.reference.owner,
        .birth = permit.reference.birth,
        .transaction_id = permit.reference.txn_id,
        .member = permit.member,
        .namespace = permit.namespace,
    });
}
fn targetAddress(a: A, permit: contract.Permit) ![32]u8 {
    return canonical(a, "antfly/source-physical-target-address/v1\x00", .{
        .metadata_group = permit.semantic.metadata_group,
        .metadata_incarnation = permit.semantic.metadata_incarnation,
        .owner = permit.reference.owner,
        .birth = permit.reference.birth,
        .namespace = permit.namespace,
    });
}

/// Sort only borrowed references. All final serialized bytes, including null,
/// AROW and prepared graph sidecars, are hashed without another row copy.
pub fn frozenDigest(a: A, intents: []const FrozenIntent) ![32]u8 {
    const sorted = try a.dupe(FrozenIntent, intents);
    defer a.free(sorted);
    std.mem.sort(FrozenIntent, sorted, {}, struct {
        fn less(_: void, left: FrozenIntent, right: FrozenIntent) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/source-frozen-intents/v1\x00");
    var length: [8]u8 = undefined;
    std.mem.writeInt(u64, &length, sorted.len, .little);
    hash.update(&length);
    for (sorted, 0..) |intent, index| {
        if (intent.key.len == 0 or intent.encoded_value.len == 0 or
            (index != 0 and std.mem.eql(u8, sorted[index - 1].key, intent.key))) return error.InvalidReplicationCutoverIntent;
        for ([_][]const u8{ intent.key, intent.encoded_value }) |bytes| {
            std.mem.writeInt(u64, &length, bytes.len, .little);
            hash.update(&length);
            hash.update(bytes);
        }
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

/// Position is supplied by the actual accepting owner. An outbound publication
/// and an inbound Source in this batch MUST share the outbox header's position.
/// The source-only caller advances source_authority once in this same batch.
pub fn stagePrepared(a: A, txn: anytype, input: Preparation, position: Position, txn_id: [16]u8, intent_revision: u64, frozen: []const FrozenIntent) !Prepared {
    try input.validate();
    try position.requireNamespace(input.permit.namespace);
    if (!std.mem.eql(u8, &txn_id, &input.permit.reference.txn_id) or intent_revision == 0) return error.ReplicationCutoverAuthorityLost;
    if (try readPrepared(a, txn, input.permit) != null) return error.ExternalTransactionDecisionChanged;
    if (try readAborted(a, txn, input.permit) != null) return error.DecisionConflict;
    if (try preparedForTransaction(a, txn, txn_id, input.permit.decision_owner, input.permit.namespace) != null) return error.ExternalTransactionDecisionChanged;
    const image: contract.Image = .{ .physical_root_incarnation = input.physical.physical_root_incarnation, .target_custody_digest = input.permit.target_custody_digest };
    try requirePrior(a, txn, input.permit, image);
    var value: Prepared = .{
        .receipt = .{ .stage = .prepared, .permit = input.permit, .image = image, .position = position, .marker_digest = undefined },
        .intent_revision = intent_revision,
        .intent_count = frozen.len,
        .intent_digest = try frozenDigest(a, frozen),
    };
    value.receipt.marker_digest = try preparedDigest(a, value);
    try value.validate(a);
    // The first accepted PREPARE pins this physical image even if P later
    // aborts. Missing user rows or a missing applied marker never unpin it.
    try txn.put(try imageKeyAlloc(a, input.permit), try encode(a, image));
    try txn.put(try preparedKeyAlloc(a, input.permit), try encode(a, value));
    try txn.put(try transactionKeyAlloc(a, txn_id, input.permit.decision_owner, input.permit.namespace), try encode(a, Transaction{ .permit = input.permit, .prepared_digest = value.receipt.marker_digest }));
    return value;
}

pub fn readPrepared(a: A, txn: anytype, permit: contract.Permit) !?Prepared {
    const value = (try read(Prepared, a, txn, try preparedKeyAlloc(a, permit))) orelse return null;
    try value.validate(a);
    if (!std.mem.eql(u8, &try value.receipt.permit.digest(a), &try permit.digest(a))) return error.ReplicationCutoverAuthorityLost;
    return value;
}
pub fn requirePrepared(a: A, txn: anytype, permit: contract.Permit, physical_root: u128, intent_revision: u64) !Prepared {
    const value = (try readPrepared(a, txn, permit)) orelse return error.ReplicationCutoverAuthorityLost;
    if (value.intent_revision != intent_revision or value.receipt.image.physical_root_incarnation != physical_root) return error.ReplicationCutoverAuthorityLost;
    const pin = (try read(contract.Image, a, txn, try imageKeyAlloc(a, permit))) orelse return error.ReplicationCutoverAuthorityLost;
    try pin.validate();
    if (!std.meta.eql(pin, value.receipt.image)) return error.ReplicationCutoverAuthorityLost;
    return value;
}
/// Standby validation uses the retained original image, not the standby's new
/// local signing root. Re-attestation on that new root requires native adoption.
pub fn validatePreparedPostimage(a: A, txn: anytype, receipt: contract.ApplyReceipt, physical_root: u128, intent_revision: u64) !Prepared {
    try receipt.validate();
    if (receipt.stage != .prepared) return error.InvalidReplicationCutoverIntent;
    const value = try requirePrepared(a, txn, receipt.permit, physical_root, intent_revision);
    if (!std.mem.eql(u8, &try value.receipt.digest(a), &try receipt.digest(a))) return error.ReplicationCutoverAuthorityLost;
    return value;
}
pub fn validatePreparedIntents(a: A, prepared: Prepared, frozen: []const FrozenIntent) !void {
    try prepared.validate(a);
    if (prepared.intent_count != frozen.len or !std.mem.eql(u8, &prepared.intent_digest, &try frozenDigest(a, frozen))) return error.ReplicationCutoverAuthorityLost;
}

/// Called only in the successful COMMIT's row batch. ABORT retains Prepared
/// and its image pin but never creates Applied. The actual commit position
/// must be later than PREPARE in the same native/Raft ordering domain.
pub fn stageApplied(a: A, txn: anytype, permit: contract.Permit, physical_root: u128, intent_revision: u64, position: Position) !contract.ApplyReceipt {
    const prepared = try requirePrepared(a, txn, permit, physical_root, intent_revision);
    try position.requireNamespace(permit.namespace);
    if (!position.strictlyAfter(prepared.receipt.position)) return error.ReplicationCutoverAuthorityLost;
    if (try readApplied(a, txn, permit) != null) return error.ExternalTransactionDecisionChanged;
    if (try readAborted(a, txn, permit) != null) return error.DecisionConflict;
    try requirePrior(a, txn, permit, prepared.receipt.image);
    const marker: contract.Marker = .{
        .semantic = permit.semantic,
        .reference = permit.reference,
        .decision_owner = permit.decision_owner,
        .member = permit.member,
        .owner_group_id = permit.owner_group_id,
        .topology_epoch = permit.topology_epoch,
        .namespace = permit.namespace,
        .schema_version = permit.schema_version,
        .schema_digest = permit.schema_digest,
        .image = prepared.receipt.image,
        .pregrant_body_digest = permit.pregrant_body_digest,
        .request_digest = permit.request_digest,
        .applied_position = position,
    };
    const receipt: contract.ApplyReceipt = .{ .stage = .applied, .permit = permit, .image = marker.image, .position = position, .marker_digest = try marker.digest(a) };
    try receipt.validate();
    try txn.put(try appliedKeyAlloc(a, permit), try encode(a, .{ .receipt = receipt, .marker = marker }));
    try txn.put(try headKeyAlloc(a, permit), try encode(a, marker));
    return receipt;
}

const Aborted = struct { receipt: contract.ApplyReceipt, prepared_marker: ?[32]u8 };
fn abortedDigest(a: A, receipt: contract.ApplyReceipt, prepared_marker: ?[32]u8) ![32]u8 {
    return canonical(a, "antfly/source-aborted-marker/v1\x00", .{
        .permit = try receipt.permit.digest(a),
        .image = receipt.image,
        .position = receipt.position,
        .prepared_marker = prepared_marker,
    });
}

/// The caller stages this inside the actual durable ABORT decision batch,
/// including the real no-PREPARE branch. No user row or applied-head changes.
/// Native permission is the immutable original ADMIT permit, not a permission
/// recomputed after receiving-image custody has advanced.
pub fn stageAborted(a: A, txn: anytype, permit: contract.Permit, physical_root: u128, namespace: Namespace, position: Position) !contract.ApplyReceipt {
    try permit.validate();
    if (physical_root == 0 or !namespace.eql(permit.namespace)) return error.ReplicationCutoverAuthorityLost;
    if (permit.expected_image) |expected| if (expected.physical_root_incarnation != physical_root) return error.ReplicationCutoverAuthorityLost;
    try position.requireNamespace(permit.namespace);
    if (try readAborted(a, txn, permit) != null) return error.ExternalTransactionDecisionChanged;
    if (try readApplied(a, txn, permit) != null) return error.DecisionConflict;
    const prepared = try readPrepared(a, txn, permit);
    const image: contract.Image = if (prepared) |value| blk: {
        _ = try requirePrepared(a, txn, permit, physical_root, value.intent_revision);
        if (!position.strictlyAfter(value.receipt.position)) return error.ReplicationCutoverAuthorityLost;
        break :blk value.receipt.image;
    } else .{ .physical_root_incarnation = physical_root, .target_custody_digest = permit.target_custody_digest };
    try requirePrior(a, txn, permit, image);
    const previous_locator = try read(Transaction, a, txn, try transactionKeyAlloc(a, permit.reference.txn_id, permit.decision_owner, permit.namespace));
    if (previous_locator) |locator| {
        if (locator.version != 1 or !std.mem.eql(u8, &try locator.permit.digest(a), &try permit.digest(a)) or
            locator.aborted_digest != null or locator.prepared_digest == null or prepared == null or
            !std.mem.eql(u8, &locator.prepared_digest.?, &prepared.?.receipt.marker_digest)) return error.ReplicationCutoverAuthorityLost;
    } else if (prepared != null) return error.ReplicationCutoverAuthorityLost;
    const prepared_marker: ?[32]u8 = if (prepared) |value| value.receipt.marker_digest else null;
    var receipt: contract.ApplyReceipt = .{ .stage = .aborted, .permit = permit, .image = image, .position = position, .marker_digest = undefined };
    receipt.marker_digest = try abortedDigest(a, receipt, prepared_marker);
    try receipt.validate();
    try txn.put(try imageKeyAlloc(a, permit), try encode(a, image));
    try txn.put(try abortedKeyAlloc(a, permit), try encode(a, Aborted{ .receipt = receipt, .prepared_marker = prepared_marker }));
    try txn.put(try transactionKeyAlloc(a, permit.reference.txn_id, permit.decision_owner, permit.namespace), try encode(a, Transaction{ .permit = permit, .prepared_digest = prepared_marker, .aborted_digest = receipt.marker_digest }));
    return receipt;
}
pub fn readAborted(a: A, txn: anytype, permit: contract.Permit) !?contract.ApplyReceipt {
    const value = (try read(Aborted, a, txn, try abortedKeyAlloc(a, permit))) orelse return null;
    try value.receipt.validate();
    if (value.receipt.stage != .aborted or !std.mem.eql(u8, &try value.receipt.permit.digest(a), &try permit.digest(a)) or
        !std.mem.eql(u8, &value.receipt.marker_digest, &try abortedDigest(a, value.receipt, value.prepared_marker))) return error.ReplicationCutoverAuthorityLost;
    const pin = (try read(contract.Image, a, txn, try imageKeyAlloc(a, permit))) orelse return error.ReplicationCutoverAuthorityLost;
    try pin.validate();
    if (!std.meta.eql(pin, value.receipt.image)) return error.ReplicationCutoverAuthorityLost;
    if (value.prepared_marker) |expected| {
        const prepared = (try readPrepared(a, txn, permit)) orelse return error.ReplicationCutoverAuthorityLost;
        if (!std.mem.eql(u8, &prepared.receipt.marker_digest, &expected) or !value.receipt.position.strictlyAfter(prepared.receipt.position) or
            !std.meta.eql(pin, prepared.receipt.image)) return error.ReplicationCutoverAuthorityLost;
    } else if (try readPrepared(a, txn, permit) != null) return error.ReplicationCutoverAuthorityLost;
    return value.receipt;
}
pub fn readApplied(a: A, txn: anytype, permit: contract.Permit) !?contract.ApplyReceipt {
    const Applied = struct { receipt: contract.ApplyReceipt, marker: contract.Marker };
    const value = (try read(Applied, a, txn, try appliedKeyAlloc(a, permit))) orelse return null;
    try value.receipt.validate();
    try value.marker.validate();
    const actual = value.receipt;
    if (actual.stage != .applied or !std.mem.eql(u8, &try actual.permit.digest(a), &try permit.digest(a)) or
        !std.mem.eql(u8, &actual.marker_digest, &try value.marker.digest(a)) or
        !value.marker.reference.eql(permit.reference) or !value.marker.decision_owner.eql(permit.decision_owner) or
        !std.meta.eql(value.marker.semantic, permit.semantic) or value.marker.member != permit.member or
        value.marker.owner_group_id != permit.owner_group_id or value.marker.topology_epoch != permit.topology_epoch or !value.marker.namespace.eql(permit.namespace) or
        value.marker.schema_version != permit.schema_version or !std.mem.eql(u8, &value.marker.schema_digest, &permit.schema_digest) or
        !std.meta.eql(value.marker.image, actual.image) or !std.meta.eql(value.marker.applied_position, actual.position) or
        !std.mem.eql(u8, &value.marker.pregrant_body_digest, &permit.pregrant_body_digest) or
        !std.mem.eql(u8, &value.marker.request_digest, &permit.request_digest)) return error.ReplicationCutoverAuthorityLost;
    return actual;
}

/// Local control evidence for the independently materialized native completion.
/// It is committed with the actual resolved bitmap and standby obligation;
/// neither a bitmap nor a caller completion can manufacture terminal custody.
pub const AckRecord = struct {
    version: u8 = 1,
    completion: contract.AckCompletion,
    image: contract.Image,
    position: Position,
    terminal_marker_digest: [32]u8,
    marker_digest: [32]u8,
    pub fn validate(self: @This(), a: A) !void {
        try self.completion.validate();
        try self.image.validate();
        try self.position.requireNamespace(self.completion.permit.namespace);
        if (self.version != 1 or std.mem.allEqual(u8, &self.terminal_marker_digest, 0) or
            !std.mem.eql(u8, &self.image.target_custody_digest, &self.completion.permit.target_custody_digest) or
            !std.mem.eql(u8, &self.marker_digest, &try acknowledgedDigest(a, self))) return error.InvalidReplicationCutoverIntent;
    }
};
pub fn acknowledgedKeyAlloc(a: A, permit: contract.Permit) ![]u8 {
    try permit.validate();
    return keyAlloc(a, "acknowledged:", try eventAddress(a, permit));
}
pub fn isAcknowledgedPostimageKey(a: A, completion: contract.AckCompletion, key: []const u8) !bool {
    try completion.validate();
    const expected = try acknowledgedKeyAlloc(a, completion.permit);
    defer a.free(expected);
    return std.mem.eql(u8, expected, key);
}
fn terminalForAcknowledgement(a: A, txn: anytype, completion: contract.AckCompletion) !contract.ApplyReceipt {
    try completion.validate();
    const permit = completion.permit;
    const joined = (try joinTransaction(a, txn, permit.reference.txn_id, permit.decision_owner, permit.namespace)) orelse return error.ReplicationCutoverAuthorityLost;
    if (!std.meta.eql(joined.locator.permit, permit) or (completion.decision == .committed and joined.prepared == null)) return error.ReplicationCutoverAuthorityLost;
    const terminal = switch (completion.decision) {
        .committed => try readApplied(a, txn, completion.permit),
        .aborted => try readAborted(a, txn, completion.permit),
    } orelse return error.ReplicationCutoverAuthorityLost;
    const pin = (try read(contract.Image, a, txn, try imageKeyAlloc(a, completion.permit))) orelse return error.ReplicationCutoverAuthorityLost;
    try pin.validate();
    if (!std.meta.eql(pin, terminal.image)) return error.ReplicationCutoverAuthorityLost;
    if (joined.prepared) |prepared| if (!terminal.position.strictlyAfter(prepared.receipt.position)) return error.ReplicationCutoverAuthorityLost;
    return terminal;
}
pub fn readAcknowledged(a: A, txn: anytype, completion: contract.AckCompletion) !?AckRecord {
    try completion.validate();
    const value = (try read(AckRecord, a, txn, try acknowledgedKeyAlloc(a, completion.permit))) orelse return null;
    try value.validate(a);
    if (!std.meta.eql(value.completion, completion)) return error.ReplicationCutoverAuthorityLost;
    const terminal = try terminalForAcknowledgement(a, txn, completion);
    if (!std.meta.eql(value.image, terminal.image) or !value.position.strictlyAfter(terminal.position) or
        !std.mem.eql(u8, &value.terminal_marker_digest, &terminal.marker_digest)) return error.ReplicationCutoverAuthorityLost;
    return value;
}
/// The accepting owner supplies a genuine post-terminal control position.
/// Normal retries read this immutable record BEFORE allocating another cut.
/// The leaf changes only this event's ACK slot; terminal rows/pins stay intact.
pub fn stageAcknowledged(a: A, txn: anytype, completion: contract.AckCompletion, physical_root: u128, position: Position) !AckRecord {
    try completion.validate();
    try position.requireNamespace(completion.permit.namespace);
    if (physical_root == 0) return error.ReplicationCutoverAuthorityLost;
    if (try readAcknowledged(a, txn, completion) != null) return error.ExternalTransactionDecisionChanged;
    const terminal = try terminalForAcknowledgement(a, txn, completion);
    if (terminal.image.physical_root_incarnation != physical_root or !position.strictlyAfter(terminal.position)) return error.ReplicationCutoverAuthorityLost;
    var value: AckRecord = .{ .completion = completion, .image = terminal.image, .position = position, .terminal_marker_digest = terminal.marker_digest, .marker_digest = undefined };
    value.marker_digest = try acknowledgedDigest(a, value);
    try value.validate(a);
    try txn.put(try acknowledgedKeyAlloc(a, completion.permit), try encode(a, value));
    return value;
}
fn acknowledgedDigest(a: A, value: AckRecord) ![32]u8 {
    return canonical(a, "antfly/source-physical-ack/v1\x00", .{ .version = value.version, .completion = value.completion, .image = value.image, .position = value.position, .terminal_marker_digest = value.terminal_marker_digest });
}

fn fixturePermit() contract.Permit {
    const reference: @import("../../api/replication_source_reference.zig").Reference = .{
        .owner = .{ .logical_subscription = .{ .database_id = 2, .subscription_id = 3 } },
        .birth = 1,
        .generation = 1,
        .source_digest = @splat(1),
        .event_digest = @splat(2),
        .intent_digest = @splat(3),
        .manifest_digest = @splat(4),
        .txn_id = @splat(5),
    };
    return .{
        .semantic = .{ .metadata_group = 1, .metadata_incarnation = @splat('1'), .owner = reference.owner, .birth = reference.birth, .source_digest = reference.source_digest, .event_digest = reference.event_digest, .transaction_id = reference.txn_id },
        .reference = reference,
        .decision_owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(6), .admission_digest = @splat(7), .expires_at_unix_ns = 1000 },
        .member = 0,
        .owner_group_id = 9,
        .topology_epoch = 7,
        .namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 12 },
        .schema_version = 0,
        .schema_digest = @splat(8),
        .target_custody_digest = @splat(9),
        .expected_image = null,
        .previous_marker_digest = null,
        .pregrant_body_digest = @splat(10),
        .request_digest = @splat(11),
    };
}
pub fn fixturePreparation() Preparation {
    const permit = fixturePermit();
    return .{ .permit = permit, .physical = .{ .physical_root_incarnation = 123, .namespace = permit.namespace, .schema_version = permit.schema_version, .schema_digest = permit.schema_digest } };
}

test "Source physical schema joins canonical actual runtime layout and accepts document version zero" {
    const a = std.testing.allocator;
    const schemas = @import("../schema.zig");
    for ([_]schemas.TableSchema{
        .{},
        .{ .version = 1, .storage_mode = .relational, .relational_columns = &.{.{ .name = "id", .path = "id", .column_type = .integer }} },
    }) |current| {
        var permit = fixturePermit();
        permit.schema_version = current.version;
        const bytes = try schemas.serializeSchema(a, current);
        defer a.free(bytes);
        std.crypto.hash.Blake3.hash(bytes, &permit.schema_digest, .{});
        const prepared = try preparationForSchema(a, permit, current, 123, permit.namespace);
        try std.testing.expectEqual(current.version, prepared.physical.schema_version);
        var decoded = current;
        decoded.decoded_format_version = 1;
        _ = try preparationForSchema(a, permit, decoded, 123, permit.namespace);
        var altered = current;
        altered.ttl_duration_ns = 1;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, preparationForSchema(a, permit, altered, 123, permit.namespace));
        altered = current;
        altered.version += 1;
        try std.testing.expectError(error.PreparedGenerationChanged, preparationForSchema(a, permit, altered, 123, permit.namespace));
        permit.expected_image = .{ .physical_root_incarnation = 123, .target_custody_digest = permit.target_custody_digest };
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, preparationForSchema(a, permit, current, 124, permit.namespace));
    }
}

test "Source physical frozen journal digest is deterministic and allocation safe" {
    const a = std.testing.allocator;
    const first = [_]FrozenIntent{ .{ .key = "b", .encoded_value = "\x00{}" }, .{ .key = "a", .encoded_value = "\x01" } };
    const reversed = [_]FrozenIntent{ first[1], first[0] };
    try std.testing.expectEqualDeep(try frozenDigest(a, &first), try frozenDigest(a, &reversed));
    const duplicate = [_]FrozenIntent{ first[0], first[0] };
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, frozenDigest(a, &duplicate));
    var changed = first;
    changed[0].encoded_value = "\x00{ }";
    try std.testing.expect(!std.mem.eql(u8, &try frozenDigest(a, &first), &try frozenDigest(a, &changed)));
    const Check = struct {
        fn run(allocator: A) !void {
            _ = try frozenDigest(allocator, &first);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{});
}

test "Source physical preparation and applied marker share actual LSM atomic batches across reopen" {
    const a = std.testing.allocator;
    const Store = @import("../docstore.zig").DocStore;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-physical", .{temp.sub_path}, 0);
    defer a.free(path);
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const input = fixturePreparation();
    const prepare_position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = 1 } };
    const applied_position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = 2 } };
    const frozen = [_]FrozenIntent{.{ .key = "doc:a", .encoded_value = "\x00{\"v\":1}" }};
    var receipt: contract.ApplyReceipt = undefined;
    {
        var store = try Store.open(a, path, .{});
        defer store.close();
        // Cancellation before commit publishes neither rows, pin nor evidence.
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            _ = try stagePrepared(scratch, &txn, input, prepare_position, input.permit.reference.txn_id, 1, &frozen);
            try txn.put("actual-frozen-intent", frozen[0].encoded_value);
        }
        {
            var read_txn = try store.beginReadTxn();
            defer read_txn.abort();
            try std.testing.expect(try readPrepared(scratch, &read_txn, input.permit) == null);
            try std.testing.expectError(error.NotFound, read_txn.get("actual-frozen-intent"));
        }
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            const prepared = try stagePrepared(scratch, &txn, input, prepare_position, input.permit.reference.txn_id, 1, &frozen);
            receipt = prepared.receipt;
            try txn.put("actual-frozen-intent", frozen[0].encoded_value);
            try txn.commit();
        }
    }
    {
        var store = try Store.open(a, path, .{});
        defer store.close();
        {
            var txn = try store.beginReadTxn();
            defer txn.abort();
            const prepared = try validatePreparedPostimage(scratch, &txn, receipt, 123, 1);
            try std.testing.expectEqualDeep(prepared, (try preparedForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace)).?);
            try std.testing.expectEqualDeep(try frozenDigest(a, &frozen), prepared.intent_digest);
            try std.testing.expectEqualStrings(frozen[0].encoded_value, try txn.get("actual-frozen-intent"));
            try std.testing.expect(try readApplied(scratch, &txn, input.permit) == null);
            try std.testing.expectError(error.ReplicationCutoverAuthorityLost, requirePrepared(scratch, &txn, input.permit, 124, 1));
            try std.testing.expectError(error.ReplicationCutoverAuthorityLost, requirePrepared(scratch, &txn, input.permit, 123, 2));
        }
        {
            var txn = try store.beginWriteTxn();
            defer txn.abort();
            try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stageApplied(scratch, &txn, input.permit, 123, 1, prepare_position));
            _ = try stageApplied(scratch, &txn, input.permit, 123, 1, applied_position);
            try txn.put("doc:a", "committed row");
            // Simulated failed COMMIT: both its row and marker disappear.
        }
        {
            var txn = try store.beginWriteTxn();
            errdefer txn.abort();
            try std.testing.expect(try readApplied(scratch, &txn, input.permit) == null);
            try std.testing.expectError(error.NotFound, txn.get("doc:a"));
            _ = try stageApplied(scratch, &txn, input.permit, 123, 1, applied_position);
            try txn.put("doc:a", "committed row");
            try txn.commit();
        }
    }
    {
        var store = try Store.open(a, path, .{});
        defer store.close();
        var txn = try store.beginReadTxn();
        defer txn.abort();
        const applied = (try readApplied(scratch, &txn, input.permit)).?;
        try std.testing.expectEqualDeep(applied_position, applied.position);
        try std.testing.expectEqualStrings("committed row", try txn.get("doc:a"));
        _ = try requirePrepared(scratch, &txn, input.permit, 123, 1);
        var changed = input.permit;
        changed.request_digest[0] ^= 1;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readApplied(scratch, &txn, changed));
    }
}

test "Source physical prepared custody survives abort and refuses a replacement root or a new unbound event" {
    const a = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "source-custody" });
    defer store.deinit();
    var region: std.heap.ArenaAllocator = .init(a);
    defer region.deinit();
    const scratch = region.allocator();
    const input = fixturePreparation();
    const position: Position = .{ .raft = .{ .term = 1, .index = 1 } };
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        _ = try stagePrepared(scratch, &txn, input, position, input.permit.reference.txn_id, 1, &.{});
        try txn.put("actual-decision", "aborted");
        try txn.commit();
    }
    var txn = try store.beginWrite();
    defer txn.abort();
    const retained = try requirePrepared(scratch, &txn, input.permit, 123, 1);
    try std.testing.expectEqual(@as(u64, 0), retained.intent_count);
    try std.testing.expect(try readApplied(scratch, &txn, input.permit) == null);
    var next = input;
    next.permit.semantic.transaction_id = @splat(12);
    next.permit.reference.txn_id = @splat(12);
    next.permit.reference.generation += 1;
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stagePrepared(scratch, &txn, next, .{ .raft = .{ .term = 1, .index = 2 } }, next.permit.reference.txn_id, 1, &.{}));
    var replaced = input;
    replaced.physical.physical_root_incarnation = 124;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, stagePrepared(scratch, &txn, replaced, position, input.permit.reference.txn_id, 1, &.{}));
    // A missing Prepared row cannot waive the surviving native image pin.
    try txn.delete(try preparedKeyAlloc(scratch, input.permit));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stagePrepared(scratch, &txn, replaced, position, input.permit.reference.txn_id, 1, &.{}));
}

test "Source physical marker chain rejects stale events while retaining original applied receipts" {
    const a = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "source-chain" });
    defer store.deinit();
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const first = fixturePreparation();
    var first_receipt: contract.ApplyReceipt = undefined;
    var next = first;
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        _ = try stagePrepared(scratch, &txn, first, .{ .raft = .{ .term = 1, .index = 10 } }, first.permit.reference.txn_id, 1, &.{});
        first_receipt = try stageApplied(scratch, &txn, first.permit, 123, 1, .{ .raft = .{ .term = 1, .index = 11 } });
        try txn.commit();
    }
    next.permit.semantic.transaction_id = @splat(12);
    next.permit.reference.txn_id = @splat(12);
    next.permit.reference.generation += 1;
    next.permit.expected_image = first_receipt.image;
    next.permit.previous_marker_digest = first_receipt.marker_digest;
    var second_receipt: contract.ApplyReceipt = undefined;
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        var stale = next;
        stale.permit.reference.generation = first.permit.reference.generation;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stagePrepared(scratch, &txn, stale, .{ .raft = .{ .term = 1, .index = 12 } }, next.permit.reference.txn_id, 1, &.{}));
        var wrong_chain = next;
        wrong_chain.permit.previous_marker_digest = @splat(13);
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stagePrepared(scratch, &txn, wrong_chain, .{ .raft = .{ .term = 1, .index = 12 } }, next.permit.reference.txn_id, 1, &.{}));
        _ = try stagePrepared(scratch, &txn, next, .{ .raft = .{ .term = 1, .index = 12 } }, next.permit.reference.txn_id, 1, &.{});
        // A position in a different authority domain cannot finish this event.
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stageApplied(scratch, &txn, next.permit, 123, 1, .{ .native = .{ .namespace = next.permit.namespace, .sequence = 100 } }));
        second_receipt = try stageApplied(scratch, &txn, next.permit, 123, 1, .{ .raft = .{ .term = 1, .index = 13 } });
        try txn.commit();
    }
    var txn = try store.beginRead();
    defer txn.abort();
    try std.testing.expectEqualDeep(first_receipt, (try readApplied(scratch, &txn, first.permit)).?);
    try std.testing.expectEqualDeep(second_receipt, (try readApplied(scratch, &txn, next.permit)).?);
}

test "Source physical standby joins exact journal bytes and closed event keys and refuses canonical record tampering" {
    const a = std.testing.allocator;
    var backend = @import("../mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "source-point-joins" });
    defer store.deinit();
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const input = fixturePreparation();
    const position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = 4 } };
    const frozen = [_]FrozenIntent{.{ .key = "doc:a", .encoded_value = "\x00{\"v\":1}" }};
    var txn = try store.beginWrite();
    defer txn.abort();
    var prepared = try stagePrepared(scratch, &txn, input, position, input.permit.reference.txn_id, 1, &frozen);
    try validatePreparedIntents(scratch, prepared, &frozen);
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, validatePreparedIntents(scratch, prepared, &.{}));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, validatePreparedIntents(scratch, prepared, &.{.{ .key = "doc:a", .encoded_value = "\x00{\"v\":2}" }}));
    try std.testing.expect(try isPreparedPostimageKey(scratch, input.permit, try preparedKeyAlloc(scratch, input.permit)));
    try std.testing.expect(try isPreparedPostimageKey(scratch, input.permit, try imageKeyAlloc(scratch, input.permit)));
    try std.testing.expect(try isPreparedPostimageKey(scratch, input.permit, try transactionKeyAlloc(scratch, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace)));
    try std.testing.expect(!try isPreparedPostimageKey(scratch, input.permit, try appliedKeyAlloc(scratch, input.permit)));
    var unrelated = input.permit;
    unrelated.reference.txn_id = @splat(14);
    unrelated.semantic.transaction_id = unrelated.reference.txn_id;
    try std.testing.expect(!try isPreparedPostimageKey(scratch, input.permit, try preparedKeyAlloc(scratch, unrelated)));
    prepared.intent_digest[0] ^= 1;
    try txn.put(try preparedKeyAlloc(scratch, input.permit), try encode(scratch, prepared));
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, readPrepared(scratch, &txn, input.permit));
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, preparedForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace));
}

test "Source physical failed preparation allocation leaves no durable pin or receipt" {
    const Check = struct {
        fn run(failing: A) !void {
            const stable = std.testing.allocator;
            var backend = @import("../mem_backend.zig").Backend.init(stable, .{});
            defer backend.close();
            var store = try backend.runtimeStore(stable, .{ .name = "source-allocation" });
            defer store.deinit();
            const input = fixturePreparation();
            var allocation_failure: ?anyerror = null;
            {
                var txn = try store.beginWrite();
                defer txn.abort();
                var arena: std.heap.ArenaAllocator = .init(failing);
                defer arena.deinit();
                _ = stagePrepared(arena.allocator(), &txn, input, .{ .raft = .{ .term = 1, .index = 1 } }, input.permit.reference.txn_id, 1, &.{}) catch |err| blk: {
                    allocation_failure = err;
                    break :blk undefined;
                };
                // No commit: even a complete candidate remains unpublished.
            }
            var read_txn = try store.beginRead();
            defer read_txn.abort();
            var region: std.heap.ArenaAllocator = .init(stable);
            defer region.deinit();
            try std.testing.expect(try readPrepared(region.allocator(), &read_txn, input.permit) == null);
            try std.testing.expect(try read(contract.Image, region.allocator(), &read_txn, try imageKeyAlloc(region.allocator(), input.permit)) == null);
            try std.testing.expect(try preparedForTransaction(region.allocator(), &read_txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace) == null);
            if (allocation_failure) |err| return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "Source physical ABORT proves a real durable fence with and without a lost PREPARE reply" {
    const a = std.testing.allocator;
    const Store = @import("../docstore.zig").DocStore;
    const source = @import("../source_authority.zig");
    const input = fixturePreparation();
    const source_namespace = @import("online_source_contract.zig").namespaceBytes(input.permit.namespace);
    for ([_]bool{ false, true }) |had_prepare| {
        var temp = std.testing.tmpDir(.{});
        defer temp.cleanup();
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-abort", .{temp.sub_path}, 0);
        defer a.free(path);
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var accepted: contract.ApplyReceipt = undefined;
        {
            var store = try Store.open(a, path, .{});
            defer store.close();
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try source.bind(&txn, .native, source_namespace);
                if (had_prepare) {
                    const position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, source_namespace, null) } };
                    _ = try stagePrepared(scratch, &txn, input, position, input.permit.reference.txn_id, 1, &.{});
                }
                try txn.commit();
            }
            {
                var txn = try store.beginWriteTxn();
                defer txn.abort();
                const position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, source_namespace, null) } };
                _ = try stageAborted(scratch, &txn, input.permit, 123, input.permit.namespace, position);
                try txn.put("actual-decision", "aborted");
                // An uncommitted abort does not publish a fence or pin.
            }
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try std.testing.expect(try readAborted(scratch, &txn, input.permit) == null);
                try std.testing.expectError(error.NotFound, txn.get("actual-decision"));
                const before = (try source.load(&txn)).?;
                try std.testing.expectEqual(@as(u64, @intFromBool(had_prepare)), before.sequence);
                const position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, source_namespace, null) } };
                accepted = try stageAborted(scratch, &txn, input.permit, 123, input.permit.namespace, position);
                try txn.put("actual-decision", "aborted");
                try txn.commit();
            }
        }
        var store = try Store.open(a, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        defer txn.abort();
        const observed = (try readAborted(scratch, &txn, input.permit)).?;
        try std.testing.expectEqualDeep(accepted, observed);
        try std.testing.expectEqual(@as(u64, 1 + @as(u64, @intFromBool(had_prepare))), observed.position.native.sequence);
        try std.testing.expectEqual(observed.position.native.sequence, (try source.load(&txn)).?.sequence);
        try std.testing.expectEqualStrings("aborted", try txn.get("actual-decision"));
        try std.testing.expect(try readApplied(scratch, &txn, input.permit) == null);
        const prepared = try preparedForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace);
        try std.testing.expectEqual(had_prepare, prepared != null);
        const permit = (try permitForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace)).?;
        try std.testing.expectEqualDeep(input.permit, permit);
        var wrong_owner = input.permit.decision_owner;
        wrong_owner.admission_digest[0] ^= 1;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, permitForTransaction(scratch, &txn, input.permit.reference.txn_id, wrong_owner, input.permit.namespace));
        if (!had_prepare) {
            try std.testing.expectError(error.DecisionConflict, stagePrepared(scratch, &txn, input, .{ .native = .{ .namespace = input.permit.namespace, .sequence = 10 } }, input.permit.reference.txn_id, 1, &.{}));
            try txn.delete(try abortedKeyAlloc(scratch, input.permit));
            // Retained no-PREPARE locator with its fence missing is corruption,
            // never a permission to treat this as an ordinary transaction.
            try std.testing.expectError(error.ReplicationCutoverAuthorityLost, preparedForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace));
        } else {
            try txn.delete(try abortedKeyAlloc(scratch, input.permit));
            // A retained PREPARE cannot mask a missing committed ABORT fence.
            try std.testing.expectError(error.ReplicationCutoverAuthorityLost, preparedForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace));
        }
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, permitForTransaction(scratch, &txn, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace));
    }
}

fn requirePrior(a: A, txn: anytype, permit: contract.Permit, image: contract.Image) !void {
    try image.validate();
    const pin = try read(contract.Image, a, txn, try imageKeyAlloc(a, permit));
    const marker = try read(contract.Marker, a, txn, try headKeyAlloc(a, permit));
    if (pin) |actual| {
        try actual.validate();
        if (!std.meta.eql(actual, image)) return error.ReplicationCutoverAuthorityLost;
    } else if (permit.expected_image != null or marker != null) return error.ReplicationCutoverAuthorityLost;
    if (permit.expected_image) |expected| {
        if (!std.meta.eql(expected, image)) return error.ReplicationCutoverAuthorityLost;
    } else if (pin != null) {
        // Only the same accepted event may retry a first-image reservation;
        // callers use readPrepared instead of manufacturing a new PREPARE.
        if (try readPrepared(a, txn, permit) == null) return error.ReplicationCutoverAuthorityLost;
    }
    if (marker) |previous| {
        try previous.validate();
        const expected = permit.previous_marker_digest orelse return error.ReplicationCutoverAuthorityLost;
        if (previous.reference.generation >= permit.reference.generation or
            !std.meta.eql(previous.image, image) or !std.mem.eql(u8, &try previous.digest(a), &expected)) return error.ReplicationCutoverAuthorityLost;
    } else if (permit.previous_marker_digest != null) return error.ReplicationCutoverAuthorityLost;
}

fn preparedDigest(a: A, value: Prepared) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/source-prepared-marker/v1\x00");
    // Permit identity, image and position are independently compared on every
    // point join. Include them in this digest without allocation or JSON.
    hash.update(&try value.receipt.permit.digest(a));
    var number: [16]u8 = undefined;
    std.mem.writeInt(u128, &number, value.receipt.image.physical_root_incarnation, .little);
    hash.update(&number);
    hash.update(&value.receipt.image.target_custody_digest);
    hash.update(&try value.receipt.position.encode());
    std.mem.writeInt(u64, number[0..8], value.intent_revision, .little);
    std.mem.writeInt(u64, number[8..16], value.intent_count, .little);
    hash.update(&number);
    hash.update(&value.intent_digest);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn canonical(a: A, domain: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(domain);
    hash.update(bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn encode(a: A, value: anytype) ![]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    errdefer a.free(bytes);
    if (bytes.len > maximum_record_bytes) return error.InvalidReplicationPayloadAdmission;
    return bytes;
}
fn read(comptime T: type, a: A, txn: anytype, key: []const u8) !?T {
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len == 0 or bytes.len > maximum_record_bytes) return error.InvalidReplicationPayloadAdmission;
    const value = try std.json.parseFromSliceLeaky(T, a, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    const encoded = try encode(a, value);
    defer a.free(encoded);
    if (!std.mem.eql(u8, bytes, encoded)) return error.InvalidReplicationPayloadAdmission;
    return value;
}

test "Source physical ACK control is atomic cold idempotent and requires real prepared or absent-prepare terminal custody" {
    const a = std.testing.allocator;
    const Store = @import("../docstore.zig").DocStore;
    const source = @import("../source_authority.zig");
    const input = fixturePreparation();
    const ns = @import("online_source_contract.zig").namespaceBytes(input.permit.namespace);
    const ids = [_][]const u8{"actual-owner"};
    for (0..3) |shape| {
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-ack", .{temporary.sub_path}, 0);
        defer a.free(path);
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const completion: contract.AckCompletion = .{ .permit = input.permit, .participant_count = 1, .participant_ids_digest = try contract.participantIdsDigest(&ids), .participant_id_digest = contract.participantIdDigest(ids[0]), .decision = if (shape == 0) .committed else .aborted, .decision_version = 77, .terminal_receipts_digest = @splat(19) };
        var accepted: AckRecord = undefined;
        {
            var store = try Store.open(a, path, .{});
            defer store.close();
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try source.bind(&txn, .native, ns);
                try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stageAcknowledged(scratch, &txn, completion, 123, .{ .native = .{ .namespace = input.permit.namespace, .sequence = 1 } }));
                if (shape != 2) {
                    _ = try stagePrepared(scratch, &txn, input, .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, ns, null) } }, input.permit.reference.txn_id, 1, &.{});
                }
                const terminal_position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, ns, null) } };
                if (shape == 0) _ = try stageApplied(scratch, &txn, input.permit, 123, 1, terminal_position) else _ = try stageAborted(scratch, &txn, input.permit, 123, input.permit.namespace, terminal_position);
                try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stageAcknowledged(scratch, &txn, completion, 123, terminal_position));
                try txn.commit();
            }
            {
                var txn = try store.beginWriteTxn();
                defer txn.abort();
                const position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, ns, null) } };
                _ = try stageAcknowledged(scratch, &txn, completion, 123, position);
                try txn.put("actual-ack-bitmap", "resolved");
                // Simulated failure/cancellation aborts all three together.
            }
            {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try std.testing.expect(try readAcknowledged(scratch, &txn, completion) == null);
                try std.testing.expectError(error.NotFound, txn.get("actual-ack-bitmap"));
                const position: Position = .{ .native = .{ .namespace = input.permit.namespace, .sequence = try source.advance(&txn, ns, null) } };
                try std.testing.expectError(error.ReplicationCutoverAuthorityLost, stageAcknowledged(scratch, &txn, completion, 124, position));
                accepted = try stageAcknowledged(scratch, &txn, completion, 123, position);
                try txn.put("actual-ack-bitmap", "resolved");
                try txn.commit();
            }
        }
        var cold = try Store.open(a, path, .{});
        defer cold.close();
        var txn = try cold.beginWriteTxn();
        defer txn.abort();
        const observed = (try readAcknowledged(scratch, &txn, completion)).?;
        try std.testing.expectEqualDeep(accepted, observed);
        try std.testing.expectEqual(observed.position.native.sequence, (try source.load(&txn)).?.sequence);
        try std.testing.expectEqualStrings("resolved", try txn.get("actual-ack-bitmap"));
        try std.testing.expect(try isAcknowledgedPostimageKey(scratch, completion, try acknowledgedKeyAlloc(scratch, input.permit)));
        try std.testing.expect(!try isAcknowledgedPostimageKey(scratch, completion, try imageKeyAlloc(scratch, input.permit)));
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, stageAcknowledged(scratch, &txn, completion, 123, observed.position));
        var wrong = completion;
        wrong.terminal_receipts_digest[0] ^= 1;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readAcknowledged(scratch, &txn, wrong));
        wrong = completion;
        wrong.decision_version += 1;
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readAcknowledged(scratch, &txn, wrong));
        const locator_key = try transactionKeyAlloc(scratch, input.permit.reference.txn_id, input.permit.decision_owner, input.permit.namespace);
        const locator_bytes = try scratch.dupe(u8, try txn.get(locator_key));
        try txn.delete(locator_key);
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readAcknowledged(scratch, &txn, completion));
        try txn.put(locator_key, locator_bytes);
        try txn.delete(if (shape == 0) try appliedKeyAlloc(scratch, input.permit) else try abortedKeyAlloc(scratch, input.permit));
        try std.testing.expectError(error.ReplicationCutoverAuthorityLost, readAcknowledged(scratch, &txn, completion));
    }
}

pub const ObservationRequest = union(enum) {
    stage: struct { permit: contract.Permit, stage: contract.ApplyReceipt.Stage },
    acknowledged: contract.AckCompletion,

    pub fn permit(self: @This()) contract.Permit {
        return switch (self) {
            .stage => |request| request.permit,
            .acknowledged => |completion| completion.permit,
        };
    }
};

pub const Observation = union(enum) {
    stage: contract.ApplyReceipt,
    acknowledged: AckRecord,
};

pub fn observe(a: A, txn: anytype, request: ObservationRequest, physical_root: u128) !?Observation {
    const permit_value = request.permit();
    try permit_value.validate();
    const observed: Observation = switch (request) {
        .stage => |read_request| .{ .stage = switch (read_request.stage) {
            .prepared => if (try readPrepared(a, txn, permit_value)) |value| value.receipt else return null,
            .activated => (try readActivated(a, txn, permit_value)) orelse return null,
            .applied => (try readApplied(a, txn, permit_value)) orelse return null,
            .aborted => (try readAborted(a, txn, permit_value)) orelse return null,
        } },
        .acknowledged => |completion| .{ .acknowledged = (try readAcknowledged(a, txn, completion)) orelse return null },
    };
    const image = switch (observed) {
        inline else => |receipt| receipt.image,
    };
    if (image.physical_root_incarnation != physical_root) return error.ReplicationCutoverAuthorityLost;
    return observed;
}
