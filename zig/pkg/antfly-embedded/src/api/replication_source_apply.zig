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

//! Source semantic input is sealed before admission. Full native reference and
//! receiving-image permission are derived only after canonical Source/P admit.
//! No field in these PODs is a caller authority or an absence certificate.
const std = @import("std");
const Ref = @import("replication_source_reference.zig").Reference;
const Owner = @import("../system_catalog/logical_replication_origin.zig").Owner;
const External = @import("../storage/transaction_decision_owner.zig").External;
const Namespace = @import("../storage/db/doc_identity_namespace.zig").Namespace;
const Position = @import("../storage/db/receipt_position.zig").Position;
const incarnation = @import("../metadata/incarnation.zig");
const A = std.mem.Allocator;

/// Generation, manifest_digest and intent_digest are intentionally absent:
/// they are allocated/derived after these exact member bytes have been sealed.
pub const Semantic = struct {
    version: u8 = 1,
    metadata_group: u64,
    metadata_incarnation: [32]u8,
    owner: Owner,
    birth: u64,
    source_digest: [32]u8,
    event_digest: [32]u8,
    transaction_id: [16]u8,
    pub fn validate(self: @This()) !void {
        try self.owner.validate();
        if (self.version != 1 or self.metadata_group == 0 or self.birth == 0 or !incarnation.isValid(self.metadata_incarnation) or zero(self.source_digest) or zero(self.event_digest) or zero(self.transaction_id)) return error.InvalidReplicationCutoverIntent;
    }
    pub fn requireReference(self: @This(), reference: Ref) !void {
        try self.validate();
        try reference.validate();
        if (!self.owner.eql(reference.owner) or self.birth != reference.birth or !std.mem.eql(u8, &self.source_digest, &reference.source_digest) or !std.mem.eql(u8, &self.event_digest, &reference.event_digest) or !std.mem.eql(u8, &self.transaction_id, &reference.txn_id)) return error.ReplicationCutoverAuthorityLost;
    }
};

pub const Image = struct {
    physical_root_incarnation: u128,
    /// Native-derived immutable target-custody record, not an XOR/set hash or
    /// an inference from a missing physical marker. Must be point-rejoined.
    target_custody_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        if (self.physical_root_incarnation == 0 or zero(self.target_custody_digest)) return error.InvalidReplicationCutoverIntent;
    }
};
pub const Marker = struct {
    version: u8 = 1,
    semantic: Semantic,
    reference: Ref,
    decision_owner: External,
    member: u16,
    owner_group_id: u64,
    topology_epoch: u64,
    namespace: Namespace,
    schema_version: u32,
    schema_digest: [32]u8,
    image: Image,
    pregrant_body_digest: [32]u8,
    request_digest: [32]u8,
    /// Actual applied-row transaction position, never a synthetic zero cut or
    /// PREPARE position. Written in the SAME commit batch as Source rows.
    applied_position: Position,
    pub fn validate(self: @This()) !void {
        try self.semantic.requireReference(self.reference);
        try self.decision_owner.validate();
        try self.image.validate();
        try self.applied_position.requireNamespace(self.namespace);
        if (self.version != 1 or self.member >= 64 or self.owner_group_id == 0 or self.topology_epoch == 0 or self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or !std.mem.eql(u8, &self.decision_owner.metadata_incarnation, &self.semantic.metadata_incarnation) or zero(self.schema_digest) or zero(self.pregrant_body_digest) or zero(self.request_digest)) return error.InvalidReplicationCutoverIntent;
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate();
        return canonical(a, "antfly/source-applied-marker/v1\x00", self);
    }
};

/// Native acceptance derives this from actual Source receipt, retained P/member
/// and authoritative target/image custody. Data independently queries it before
/// PREPARE and compares all fields; unsigned supplied permission is rejected.
pub const PolicyEpoch = struct {
    generation: u64 = 0,
    catalog_epoch: u64 = 0,

    pub fn validate(self: PolicyEpoch) !void {
        if ((self.generation == 0) != (self.catalog_epoch == 0)) return error.InvalidReplicationCutoverIntent;
    }
};

pub const Permit = struct {
    version: u8 = 1,
    semantic: Semantic,
    reference: Ref,
    decision_owner: External,
    member: u16,
    owner_group_id: u64,
    topology_epoch: u64,
    namespace: Namespace,
    schema_version: u32,
    schema_digest: [32]u8,
    target_custody_digest: [32]u8,
    /// Null only after actual native target reservation proves no historical
    /// accepted image. Physical marker absence alone never issues this branch.
    expected_image: ?Image,
    previous_marker_digest: ?[32]u8,
    sql_rights: u16 = 0,
    policy_epoch: PolicyEpoch = .{},
    pregrant_body_digest: [32]u8,
    request_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.policy_epoch.validate();
        try self.semantic.requireReference(self.reference);
        try self.decision_owner.validate();
        if (self.version != 1 or self.member >= 64 or self.owner_group_id == 0 or self.topology_epoch == 0 or self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or !std.mem.eql(u8, &self.decision_owner.metadata_incarnation, &self.semantic.metadata_incarnation) or zero(self.schema_digest) or zero(self.target_custody_digest) or zero(self.pregrant_body_digest) or zero(self.request_digest)) return error.InvalidReplicationCutoverIntent;
        if (self.expected_image) |image| {
            try image.validate();
            if (!std.mem.eql(u8, &image.target_custody_digest, &self.target_custody_digest)) return error.ReplicationCutoverAuthorityLost;
        } else if (self.previous_marker_digest != null) return error.ReplicationCutoverAuthorityLost;
        if (self.previous_marker_digest) |previous| if (zero(previous)) return error.InvalidReplicationCutoverIntent;
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate();
        return canonical(a, "antfly/source-apply-permit/v1\x00", self);
    }
};

pub const ApplyReceipt = struct {
    /// Named because this enum crosses independently compiled owner methods.
    pub const Stage = enum { prepared, applied, aborted, activated };
    version: u8 = 1,
    stage: Stage,
    permit: Permit,
    image: Image,
    /// Real ordered PREPARE or applied-row position according to stage.
    position: Position,
    /// Prepared commits the exact frozen intent+prior marker; applied commits
    /// the actual row marker. Neither stage substitutes for the other.
    marker_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.permit.validate();
        try self.image.validate();
        try self.position.requireNamespace(self.permit.namespace);
        if (self.version != 1 or zero(self.marker_digest) or !std.mem.eql(u8, &self.image.target_custody_digest, &self.permit.target_custody_digest)) return error.InvalidReplicationCutoverIntent;
        if (self.permit.expected_image) |expected| if (!std.meta.eql(expected, self.image)) return error.ReplicationCutoverAuthorityLost;
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate();
        return canonical(a, "antfly/source-apply-receipt/v1\x00", self);
    }
};
fn zero(value: anytype) bool {
    return std.mem.allEqual(u8, &value, 0);
}
fn canonical(a: A, domain: []const u8, value: anytype) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
    defer a.free(bytes);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(domain);
    hash.update(bytes);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

/// Native-retained terminal completion. This is not a signed/caller grant:
/// Data independently rejoins the full original IDs before materialization.
pub const AckCompletion = struct {
    pub const Decision = enum { committed, aborted };
    permit: Permit,
    participant_count: u16,
    participant_ids_digest: [32]u8,
    participant_id_digest: [32]u8,
    decision: Decision,
    decision_version: u64,
    terminal_receipts_digest: [32]u8,
    pub fn validate(self: @This()) !void {
        try self.permit.validate();
        if (self.participant_count == 0 or self.participant_count > 64 or self.permit.member >= self.participant_count or self.decision_version == 0 or zero(self.participant_ids_digest) or zero(self.participant_id_digest) or zero(self.terminal_receipts_digest)) return error.InvalidReplicationCutoverIntent;
    }
    /// Transport may carry the whole canonical set or just this member's
    /// exact original ID. A peer singleton is not a widened physical roster.
    pub fn requireAcknowledgementIds(self: @This(), ids: []const []const u8) !void {
        try self.validate();
        if (ids.len == 1) {
            if (!std.mem.eql(u8, &participantIdDigest(ids[0]), &self.participant_id_digest)) return error.ReplicationCutoverAuthorityLost;
            return;
        }
        try self.requireParticipants(ids);
    }
    pub fn requireParticipants(self: @This(), ids: []const []const u8) !void {
        try self.validate();
        if (ids.len != self.participant_count or !std.mem.eql(u8, &try participantIdsDigest(ids), &self.participant_ids_digest) or !std.mem.eql(u8, &participantIdDigest(ids[self.permit.member]), &self.participant_id_digest)) return error.ReplicationCutoverAuthorityLost;
    }
};
fn ackNumber(hash: *std.crypto.hash.Blake3, value: usize) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, @intCast(value), .little);
    hash.update(&encoded);
}
pub fn participantIdDigest(id: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/source-ack-participant/v1\x00");
    ackNumber(&hash, id.len);
    hash.update(id);
    var out: [32]u8 = undefined;
    hash.final(&out);
    return out;
}
pub fn participantIdsDigest(ids: []const []const u8) ![32]u8 {
    if (ids.len > 64) return error.InvalidReplicationCutoverIntent;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/source-ack-roster/v1\x00");
    ackNumber(&hash, ids.len);
    for (ids, 0..) |id, i| {
        if (id.len == 0 or id.len > std.math.maxInt(u32)) return error.InvalidReplicationCutoverIntent;
        for (ids[0..i]) |earlier| if (std.mem.eql(u8, earlier, id)) return error.InvalidReplicationCutoverIntent;
        ackNumber(&hash, id.len);
        hash.update(id);
    }
    var out: [32]u8 = undefined;
    hash.final(&out);
    return out;
}

test "Source ACK participant digests retain ordered byte framing and exact selected member" {
    const a = try participantIdsDigest(&.{ "a", "bc" });
    const b = try participantIdsDigest(&.{ "ab", "c" });
    const reordered = try participantIdsDigest(&.{ "bc", "a" });
    try std.testing.expect(!std.mem.eql(u8, &a, &b));
    try std.testing.expect(!std.mem.eql(u8, &a, &reordered));
    try std.testing.expect(!std.mem.eql(u8, &participantIdDigest("a\x00b"), &participantIdDigest("a")));
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, participantIdsDigest(&.{ "a", "a" }));
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, participantIdsDigest(&.{""}));
}

test "Source ACK participant digests include every byte of a full long native roster" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ids = try a.alloc([]const u8, 64);
    for (ids, 0..) |*id, index| {
        const bytes = try a.alloc(u8, 1048);
        @memset(bytes, 'a');
        bytes[bytes.len - 1] = @intCast(index + 1);
        id.* = bytes;
    }
    const original = try participantIdsDigest(ids);
    try std.testing.expect(ids.len * ids[0].len > 32 * 1024);
    const changed = try a.dupe(u8, ids[63]);
    changed[changed.len - 2] = 'b';
    ids[63] = changed;
    try std.testing.expect(!std.mem.eql(u8, &original, &try participantIdsDigest(ids)));
}

test "Source ACK completion permits only exact full IDs or its selected singleton" {
    const reference: Ref = .{ .owner = .{ .table_source = .{ .table_id = 1, .source_ordinal = 0 } }, .birth = 1, .generation = 1, .source_digest = @splat(1), .event_digest = @splat(2), .intent_digest = @splat(3), .manifest_digest = @splat(4), .txn_id = @splat(5) };
    const permit: Permit = .{ .semantic = .{ .metadata_group = 1, .metadata_incarnation = @splat('1'), .owner = reference.owner, .birth = 1, .source_digest = reference.source_digest, .event_digest = reference.event_digest, .transaction_id = reference.txn_id }, .reference = reference, .decision_owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(6), .admission_digest = @splat(7), .expires_at_unix_ns = 1000 }, .member = 1, .owner_group_id = 8, .topology_epoch = 9, .namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 12 }, .schema_version = 0, .schema_digest = @splat(13), .target_custody_digest = @splat(14), .expected_image = null, .previous_marker_digest = null, .pregrant_body_digest = @splat(15), .request_digest = @splat(16) };
    const ids = &[_][]const u8{ "a", "b", "c" };
    const completion: AckCompletion = .{ .permit = permit, .participant_count = 3, .participant_ids_digest = try participantIdsDigest(ids), .participant_id_digest = participantIdDigest("b"), .decision = .committed, .decision_version = 2, .terminal_receipts_digest = @splat(17) };
    try completion.requireParticipants(ids);
    try completion.requireAcknowledgementIds(ids);
    try completion.requireAcknowledgementIds(&.{"b"});
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, completion.requireAcknowledgementIds(&.{"a"}));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, completion.requireAcknowledgementIds(&.{ "a", "b" }));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, completion.requireAcknowledgementIds(&.{ "b", "a", "c" }));
    try std.testing.expectError(error.ReplicationCutoverAuthorityLost, completion.requireParticipants(&.{"b"}));
}

/// SQL actions are explicit in production replication intents. Recovery
/// rederives these bits from the immutable body before delivering PREPARE.
pub fn intentRights(intent: anytype) !u16 {
    const Access = @import("../system_catalog/privileges.zig").Access;
    var rights: u16 = 0;
    var row_predicates: usize = 0;
    for (intent.predicates) |predicate| {
        rights |= switch (predicate.row_policy_requirement) {
            .insert, .insert_select => Access.bit(.insert),
            .update, .update_select => Access.bit(.update),
            .delete, .delete_select => Access.bit(.delete),
            .native => continue,
        };
        row_predicates += 1;
        if (predicate.row_policy_requirement.requiresSelect()) rights |= Access.bit(.select);
    }
    if (row_predicates != intent.writes.len + intent.deletes.len) return error.InvalidReplicationCutoverIntent;
    if (row_predicates != intent.predicates.len and intent.relational_integrity_generation_set == null)
        return error.InvalidReplicationCutoverIntent;
    if (rights == 0 and (intent.relational_integrity_generation_set == null or
        (intent.integrity_commands.len == 0 and intent.predicates.len == 0))) return error.InvalidReplicationCutoverIntent;
    return rights;
}

/// A key move materializes as a delete and insert. Folding a source delete
/// followed by an insert at the same key materializes as a replacement.
pub fn requireWalEffectRights(source: u16, physical: u16) !void {
    const Access = @import("../system_catalog/privileges.zig").Access;
    const insert = Access.bit(.insert);
    const update = Access.bit(.update);
    const delete = Access.bit(.delete);
    var allowed = source;
    if (source & update != 0) allowed |= insert | delete;
    if (source & (insert | delete) == (insert | delete)) allowed |= update;
    if (physical == 0 or physical & ~allowed != 0) return error.InvalidReplicationCutoverIntent;
}

test "Source physical WAL effects preserve the original SQL permission requirements" {
    const Access = @import("../system_catalog/privileges.zig").Access;
    const insert = Access.bit(.insert);
    const update = Access.bit(.update);
    const delete = Access.bit(.delete);
    try requireWalEffectRights(update, insert | delete);
    try requireWalEffectRights(insert | delete, update);
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, requireWalEffectRights(insert, delete));
    try std.testing.expectError(error.InvalidReplicationCutoverIntent, requireWalEffectRights(delete, update));
}
