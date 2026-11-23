// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Receiving-root proof for actual Source PREPARE/applied rows. Native Source
//! acceptance independently rejoins current/historical enrollment, real P/member
//! and target/image custody; a presented signature is never its own authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const apply = local.api_replication_source_apply;
const root = @import("../metadata/store_root_attestation.zig");
const signing = local.storage_db_root_signing_identity;
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;
pub const route_suffix = "/replication-apply-receipt";
pub const max_request_bytes = 32 * 1024;
pub const Request = union(enum) {
    stage: struct { permit: apply.Permit, stage: apply.ApplyReceipt.Stage },
    acknowledged: apply.AckCompletion,

    pub fn permit(self: Request) apply.Permit {
        return switch (self) {
            .stage => |stage| stage.permit,
            .acknowledged => |completion| completion.permit,
        };
    }

    pub fn validate(self: Request) !void {
        switch (self) {
            .stage => |stage| try stage.permit.validate(),
            .acknowledged => |completion| try completion.validate(),
        }
    }
};
pub const Observation = union(enum) {
    stage: Receipt,
    acknowledged: Acknowledged,

    pub fn requireRequest(self: Observation, request: Request) !void {
        const matches = switch (request) {
            .stage => |query| self == .stage and self.stage.value.stage == query.stage and std.meta.eql(self.stage.value.permit, query.permit),
            .acknowledged => |query| self == .acknowledged and std.meta.eql(self.acknowledged.value.completion, query),
        };
        if (!matches) return error.ReplicationCutoverAuthorityLost;
    }
};
pub const Driver = struct {
    ptr: *anyopaque,
    observe: *const fn (*anyopaque, A, []const u8, Request, Context) anyerror!?Observation,
};

pub const Proof = root.ProofFor("antfly/source-apply/signature/v1\x00", "antfly/source-apply/accepted/v1\x00");
pub const Receipt = struct {
    value: apply.ApplyReceipt,
    proof: Proof,
    pub fn validate(self: @This(), a: A) !void {
        try self.value.validate();
        // Durable inbound ownership is permanent; SQL TEMP lifetime pins may
        // never masquerade as ordinary native Source target provenance.
        if (self.proof.native_pin != null or !std.mem.eql(u8, &self.proof.identity.metadata_incarnation, &self.value.permit.semantic.metadata_incarnation) or
            ((self.value.position == .native) != (self.proof.authority == .native))) return error.InvalidReplicationCutoverIntent;
        try self.proof.verify(try self.value.digest(a));
    }
    pub fn require(self: @This(), permit: apply.Permit, stage: @FieldType(apply.ApplyReceipt, "stage"), a: A) !void {
        try self.validate(a);
        try permit.validate();
        if (self.value.stage != stage or !std.meta.eql(self.value.permit, permit)) return error.ReplicationCutoverAuthorityLost;
    }
    pub fn sign(self: @This(), key: signing.State, a: A) !@This() {
        try self.value.validate();
        var result = self;
        result.proof = try self.proof.sign(key, try self.value.digest(a));
        return result;
    }
    pub fn ownerScope(self: @This()) root.OwnerScope {
        return .{ .group_id = self.value.permit.owner_group_id, .namespace = self.value.permit.namespace, .authority = if (self.proof.authority == .native) .native else .raft };
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate(a);
        return self.proof.commitment(try self.value.digest(a));
    }
};

/// Derive every ACK completion from the complete ordered roster of verified
/// terminal receipts. The accepting metadata writer supplies enrollment
/// verification against its own catalog transaction.
pub fn terminalCompletions(
    a: A,
    receipts: []const Receipt,
    permits: []const apply.Permit,
    ids: []const []const u8,
    decision: apply.AckCompletion.Decision,
    version: u64,
    verifier: anytype,
) ![]const apply.AckCompletion {
    if (receipts.len == 0 or receipts.len > 64 or receipts.len != permits.len or receipts.len != ids.len or version == 0)
        return error.InvalidReplicationCutoverIntent;
    const roster_digest = try apply.participantIdsDigest(ids);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/source-terminal-roster/v1\x00");
    const stage: apply.ApplyReceipt.Stage = if (decision == .committed) .applied else .aborted;
    for (receipts, permits, 0..) |receipt, permit, member| {
        if (permit.member != member or !permit.reference.eql(permits[0].reference) or
            !permit.decision_owner.eql(permits[0].decision_owner)) return error.ReplicationCutoverAuthorityLost;
        try receipt.require(permit, stage, a);
        try verifier.requireTerminalRoot(receipt.ownerScope(), receipt.proof, receipt.value.position);
        hash.update(&try receipt.proof.commitment(try receipt.value.digest(a)));
    }
    var terminal_digest: [32]u8 = undefined;
    hash.final(&terminal_digest);
    const result = try a.alloc(apply.AckCompletion, permits.len);
    for (permits, ids, result) |permit, id, *completion| {
        completion.* = .{
            .permit = permit,
            .participant_count = @intCast(permits.len),
            .participant_ids_digest = roster_digest,
            .participant_id_digest = apply.participantIdDigest(id),
            .decision = decision,
            .decision_version = version,
            .terminal_receipts_digest = terminal_digest,
        };
    }
    return result;
}

pub const AckProof = root.ProofFor("antfly/source-physical-ack/signature/v1\x00", "antfly/source-physical-ack/accepted/v1\x00");

pub const Acknowledged = struct {
    value: local.storage_db_replication_source_apply_store.AckRecord,
    proof: AckProof,

    pub fn sign(self: @This(), key: signing.State, a: A) !@This() {
        try self.value.validate(a);
        var signed = self;
        signed.proof = try self.proof.sign(key, self.value.marker_digest);
        try signed.validate(a);
        return signed;
    }

    pub fn validate(self: @This(), a: A) !void {
        try self.value.validate(a);
        if (self.proof.native_pin != null or ((self.value.position == .native) != (self.proof.authority == .native)) or
            !std.mem.eql(u8, &self.proof.identity.metadata_incarnation, &self.value.completion.permit.semantic.metadata_incarnation))
            return error.ReplicationCutoverAuthorityLost;
        try self.proof.verify(self.value.marker_digest);
    }

    pub fn ownerScope(self: @This()) root.OwnerScope {
        const permit = self.value.completion.permit;
        return .{ .group_id = permit.owner_group_id, .namespace = permit.namespace, .authority = if (self.proof.authority == .native) .native else .raft };
    }
};
