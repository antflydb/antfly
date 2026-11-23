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

//! Root-local attestation that an exact Source bootstrap was verified before
//! proposal. This is not enrollment, a retention lease, or a decision receipt.
const std = @import("std");
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;
const incarnation = @import("antfly_local_sources").metadata_incarnation;

pub const route = "/internal/v1/replication-snapshot-attestation";
pub const max_request_bytes = 4096;

pub const Expected = struct {
    metadata_group_id: u64,
    metadata_incarnation: incarnation.MetadataClusterIncarnation,
    source_digest: [32]u8,
    expected_generation: u64,
    txn_id: [16]u8,
    request_digest: [32]u8,
    retained_digest: [32]u8,

    pub fn validate(self: @This()) !void {
        if (self.metadata_group_id == 0 or self.expected_generation == 0 or !incarnation.isValid(self.metadata_incarnation) or
            std.mem.allEqual(u8, &self.txn_id, 0)) return error.InvalidReplicationPayloadAdmission;
        for ([_][32]u8{ self.source_digest, self.request_digest, self.retained_digest }) |digest| {
            if (std.mem.allEqual(u8, &digest, 0)) return error.InvalidReplicationPayloadAdmission;
        }
    }
};

pub const Snapshot = struct {
    purpose: @import("antfly_local_sources").api_replication_source_purpose.Purpose,
    ready: @import("antfly_local_sources").api_replication_source_purpose.Ready,
    proof: Proof,
    page_chain: [32]u8,

    pub fn expected(purpose: @FieldType(Snapshot, "purpose"), ready: @FieldType(Snapshot, "ready"), generation: u64, transaction: [16]u8, page_chain: [32]u8) !Expected {
        try purpose.validate();
        try ready.validate();
        if ((ready.total_rows == 0) != std.mem.allEqual(u8, &page_chain, 0) or !purpose.upload.sameIdentity(ready.upload)) return error.InvalidReplicationPayloadAdmission;
        return .{
            .metadata_group_id = purpose.metadata_group,
            .metadata_incarnation = purpose.incarnation,
            .source_digest = purpose.source_digest,
            .expected_generation = generation,
            .txn_id = transaction,
            .request_digest = try @import("replication_digest.zig").digest("antfly/subscription-bootstrap-capture/v1\x00", .{ .purpose = purpose, .ready = ready, .page_chain = page_chain }, 16 * 1024),
            .retained_digest = ready.capture.content_digest,
        };
    }
};

pub fn expectedPage(group: u64, incarnation_value: [32]u8, source: @import("antfly_local_sources").api_replication_source_reference.Reference, plan_digest: [32]u8) !Expected {
    try source.validate();
    return .{
        .metadata_group_id = group,
        .metadata_incarnation = incarnation_value,
        .source_digest = source.source_digest,
        .expected_generation = source.generation,
        .txn_id = source.txn_id,
        .request_digest = try @import("replication_digest.zig").digest("antfly/subscription-routed-page/v1\x00", source, 8192),
        .retained_digest = plan_digest,
    };
}

pub const Proof = struct {
    version: u8 = 1,
    metadata_group_id: u64,
    source_digest: [32]u8,
    expected_generation: u64,
    txn_id: [16]u8,
    request_digest: [32]u8,
    retained_digest: [32]u8,
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    signature: [64]u8,

    fn message(self: @This()) ![32]u8 {
        if (self.version != 1 or self.reporter_incarnation == 0)
            return error.InvalidReplicationPayloadAdmission;
        try (Expected{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.identity.metadata_incarnation,
            .source_digest = self.source_digest,
            .expected_generation = self.expected_generation,
            .txn_id = self.txn_id,
            .request_digest = self.request_digest,
            .retained_digest = self.retained_digest,
        }).validate();
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/cdc-source-initialization-ready/v1\x00");
        hash.update(&[_]u8{self.version});
        hash.update(&(try self.identity.digest()));
        var number: [8]u8 = undefined;
        for ([_]u64{ self.metadata_group_id, self.expected_generation, self.reporter_incarnation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        hash.update(&self.source_digest);
        hash.update(&self.txn_id);
        hash.update(&self.request_digest);
        hash.update(&self.retained_digest);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }

    /// The caller must first verify actual bytes, ordered target headers, and
    /// closure custody. Only the private root owner may supply this key.
    pub fn sign(expected: Expected, identity: enrollment.Identity, reporter_incarnation: u64, root: signing.State) !@This() {
        try expected.validate();
        if (!std.mem.eql(u8, &expected.metadata_incarnation, &identity.metadata_incarnation) or
            root.root_incarnation != identity.root_incarnation or
            !std.mem.eql(u8, &root.public_key, &identity.public_key)) return error.InvalidReplicationPayloadAdmission;
        var result: @This() = .{
            .metadata_group_id = expected.metadata_group_id,
            .source_digest = expected.source_digest,
            .expected_generation = expected.expected_generation,
            .txn_id = expected.txn_id,
            .request_digest = expected.request_digest,
            .retained_digest = expected.retained_digest,
            .identity = identity,
            .reporter_incarnation = reporter_incarnation,
            .signature = undefined,
        };
        result.signature = try root.sign(&(try result.message()));
        return result;
    }

    /// Pure verification. The accepting writer must separately rejoin the
    /// enrolled identity AND current StoreRecord reporter in the same cut.
    pub fn verify(self: @This(), expected: Expected) !void {
        try expected.validate();
        if (self.metadata_group_id != expected.metadata_group_id or
            self.expected_generation != expected.expected_generation or
            !std.mem.eql(u8, &self.identity.metadata_incarnation, &expected.metadata_incarnation) or
            !std.mem.eql(u8, &self.source_digest, &expected.source_digest) or
            !std.mem.eql(u8, &self.txn_id, &expected.txn_id) or
            !std.mem.eql(u8, &self.request_digest, &expected.request_digest) or
            !std.mem.eql(u8, &self.retained_digest, &expected.retained_digest)) return error.InvalidReplicationPayloadAdmission;
        signing.verify(self.identity.public_key, &(try self.message()), self.signature) catch
            return error.InvalidReplicationPayloadAdmission;
    }

    pub fn requireCurrent(self: @This(), current: enrollment.Identity, reporter_incarnation: u64) !void {
        try current.validate();
        if (!std.meta.eql(self.identity, current) or reporter_incarnation == 0 or
            reporter_incarnation != self.reporter_incarnation) return error.InvalidReplicationPayloadAdmission;
    }
};

test "CDC initialization attestation binds draft ready body current root and reporter" {
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const identity: enrollment.Identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 3, .store_id = 5, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    const root: signing.State = .{ .root_incarnation = 7, .seed = seed, .public_key = identity.public_key };
    const expected: Expected = .{ .metadata_group_id = 1, .metadata_incarnation = identity.metadata_incarnation, .source_digest = @splat(2), .expected_generation = 4, .txn_id = @splat(3), .request_digest = @splat(4), .retained_digest = @splat(5) };
    const proof = try Proof.sign(expected, identity, 9, root);
    try proof.verify(expected);
    try proof.requireCurrent(identity, 9);
    inline for (@typeInfo(Expected).@"struct".field_names) |field| {
        var changed = expected;
        if (comptime std.mem.eql(u8, field, "metadata_incarnation")) {
            changed.metadata_incarnation = "ffffffffffffffffffffffffffffffff".*;
        } else switch (@TypeOf(@field(changed, field))) {
            u64 => @field(changed, field) += 1,
            else => @field(changed, field)[0] ^= 1,
        }
        try std.testing.expectError(error.InvalidReplicationPayloadAdmission, proof.verify(changed));
    }
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, proof.requireCurrent(identity, 10));
    var foreign = identity;
    foreign.root_incarnation += 1;
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, proof.requireCurrent(foreign, 9));
    var forged = proof;
    forged.signature[0] ^= 1;
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, forged.verify(expected));
    forged = proof;
    forged.version = 2;
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, forged.verify(expected));
}

test "Source READY attestation cannot reuse a receiving row signature" {
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const identity: enrollment.Identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 3, .store_id = 5, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    const root: signing.State = .{ .root_incarnation = 7, .seed = seed, .public_key = identity.public_key };
    const expected: Expected = .{ .metadata_group_id = 1, .metadata_incarnation = identity.metadata_incarnation, .source_digest = @splat(2), .expected_generation = 4, .txn_id = @splat(3), .request_digest = @splat(4), .retained_digest = @splat(5) };
    var substituted = try Proof.sign(expected, identity, 9, root);
    const receiving = @import("../api/replication_source_apply_attestation.zig").Proof;
    const ready_digest = try substituted.message();
    const other = try (receiving{ .identity = identity, .reporter_incarnation = 9, .authority = .{ .native = @splat(8) }, .root_generation = 1, .signature = undefined }).sign(root, ready_digest);
    try other.verify(ready_digest);
    substituted.signature = other.signature;
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, substituted.verify(expected));
    var unborn = expected;
    unborn.expected_generation = 0;
    try std.testing.expectError(error.InvalidReplicationPayloadAdmission, unborn.validate());
}
