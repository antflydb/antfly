// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Durable hidden-owner state for an initial FK-bearing child CREATE. A fresh
//! owner installs its schema and this record in one transaction, then remains
//! inaccessible until metadata has accepted all parent generations and the
//! exact published-hidden decision authorizes owner release.
const std = @import("std");
const integrity = @import("relational_integrity_contract.zig");
const identity = @import("doc_identity_namespace.zig");

pub const key = "\x00\x00__metadata__:relational_initial_child_publication";
const magic = "AICH";
const version: u8 = 1;
const encoded_len = 4 + 1 + 1 + 2 + 16 + 32 + 24 + 4 + 8 + 32 + 32 + 32 + 8 + 8 + 8 + 8 + 32;

pub const Phase = enum(u8) { hidden = 1, released = 2, canceled = 3 };

/// Authenticated provisioning-only first-open gate. This is deliberately not
/// durable authority: the Raft-provisioned Record below supplies that. It
/// denies direct group I/O during the gap between physical owner creation and
/// the first replicated provision/cancel entry.
pub const Bootstrap = struct {
    plan_id: [16]u8,
    plan_digest: integrity.Digest,
    namespace: identity.Namespace,
    schema_version: u32,
    schema_digest: integrity.Digest,
    public_schema_json_digest: integrity.Digest,
    catalog_digest: integrity.Digest,

    pub fn validate(self: Bootstrap) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.schema_digest, 0) or
            std.mem.allEqual(u8, &self.public_schema_json_digest, 0) or
            std.mem.allEqual(u8, &self.catalog_digest, 0) or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0)
            return error.InvalidInitialChildPublication;
    }

    pub fn matches(self: Bootstrap, record: Record) bool {
        return std.mem.eql(u8, &self.plan_id, &record.plan_id) and
            std.mem.eql(u8, &self.plan_digest, &record.plan_digest) and
            self.namespace.eql(record.namespace) and self.schema_version == record.schema_version and
            std.mem.eql(u8, &self.schema_digest, &record.schema_digest) and
            std.mem.eql(u8, &self.public_schema_json_digest, &record.public_schema_json_digest) and
            std.mem.eql(u8, &self.catalog_digest, &record.catalog_digest);
    }

    pub fn eql(self: Bootstrap, other: Bootstrap) bool {
        return std.mem.eql(u8, &self.plan_id, &other.plan_id) and
            std.mem.eql(u8, &self.plan_digest, &other.plan_digest) and
            self.namespace.eql(other.namespace) and self.schema_version == other.schema_version and
            std.mem.eql(u8, &self.schema_digest, &other.schema_digest) and
            std.mem.eql(u8, &self.public_schema_json_digest, &other.public_schema_json_digest) and
            std.mem.eql(u8, &self.catalog_digest, &other.catalog_digest);
    }
};

pub const Record = struct {
    phase: Phase,
    plan_id: [16]u8,
    plan_digest: integrity.Digest,
    namespace: identity.Namespace,
    schema_version: u32,
    row_count: u64,
    schema_digest: integrity.Digest,
    public_schema_json_digest: integrity.Digest,
    catalog_digest: integrity.Digest,
    provision_term: u64,
    provision_index: u64,
    phase_term: u64 = 0,
    phase_index: u64 = 0,

    pub fn validate(self: Record) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.schema_digest, 0) or
            std.mem.allEqual(u8, &self.public_schema_json_digest, 0) or
            std.mem.allEqual(u8, &self.catalog_digest, 0) or
            self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            self.row_count != 0 or
            ((self.provision_term == 0) != (self.provision_index == 0)) or
            (self.phase != .canceled and self.provision_term == 0) or
            ((self.phase == .hidden) != (self.phase_term == 0 and self.phase_index == 0)) or
            ((self.phase_term == 0) != (self.phase_index == 0))) return error.InvalidInitialChildPublication;
    }

    pub fn encode(self: Record) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], magic);
        bytes[4] = version;
        bytes[5] = @intFromEnum(self.phase);
        @memcpy(bytes[8..24], &self.plan_id);
        @memcpy(bytes[24..56], &self.plan_digest);
        std.mem.writeInt(u64, bytes[56..64], self.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[64..72], self.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[72..80], self.namespace.range_id, .little);
        std.mem.writeInt(u32, bytes[80..84], self.schema_version, .little);
        std.mem.writeInt(u64, bytes[84..92], self.row_count, .little);
        @memcpy(bytes[92..124], &self.schema_digest);
        @memcpy(bytes[124..156], &self.public_schema_json_digest);
        @memcpy(bytes[156..188], &self.catalog_digest);
        std.mem.writeInt(u64, bytes[188..196], self.provision_term, .little);
        std.mem.writeInt(u64, bytes[196..204], self.provision_index, .little);
        std.mem.writeInt(u64, bytes[204..212], self.phase_term, .little);
        std.mem.writeInt(u64, bytes[212..220], self.phase_index, .little);
        std.crypto.hash.Blake3.hash(bytes[0 .. encoded_len - 32], bytes[encoded_len - 32 ..][0..32], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Record {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], magic) or
            bytes[4] != version or !std.mem.allEqual(u8, bytes[6..8], 0)) return error.InvalidInitialChildPublication;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. encoded_len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[encoded_len - 32 ..])) return error.InvalidInitialChildPublication;
        const record: Record = .{
            .phase = std.enums.fromInt(Phase, bytes[5]) orelse return error.InvalidInitialChildPublication,
            .plan_id = bytes[8..24].*,
            .plan_digest = bytes[24..56].*,
            .namespace = .{
                .table_id = std.mem.readInt(u64, bytes[56..64], .little),
                .shard_id = std.mem.readInt(u64, bytes[64..72], .little),
                .range_id = std.mem.readInt(u64, bytes[72..80], .little),
            },
            .schema_version = std.mem.readInt(u32, bytes[80..84], .little),
            .row_count = std.mem.readInt(u64, bytes[84..92], .little),
            .schema_digest = bytes[92..124].*,
            .public_schema_json_digest = bytes[124..156].*,
            .catalog_digest = bytes[156..188].*,
            .provision_term = std.mem.readInt(u64, bytes[188..196], .little),
            .provision_index = std.mem.readInt(u64, bytes[196..204], .little),
            .phase_term = std.mem.readInt(u64, bytes[204..212], .little),
            .phase_index = std.mem.readInt(u64, bytes[212..220], .little),
        };
        try record.validate();
        return record;
    }

    pub fn samePlan(self: Record, other: Record) bool {
        return std.mem.eql(u8, &self.plan_id, &other.plan_id) and
            std.mem.eql(u8, &self.plan_digest, &other.plan_digest) and
            self.namespace.eql(other.namespace) and self.schema_version == other.schema_version and
            std.mem.eql(u8, &self.schema_digest, &other.schema_digest) and
            std.mem.eql(u8, &self.public_schema_json_digest, &other.public_schema_json_digest) and
            std.mem.eql(u8, &self.catalog_digest, &other.catalog_digest);
    }
};

pub fn load(txn: anytype) !?Record {
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try Record.decode(bytes);
}

pub fn stagePhase(txn: anytype, expected: Record, phase: Phase, term: u64, index: u64) !Record {
    if (phase == .hidden or term == 0 or index == 0) return error.InvalidInitialChildPublication;
    const prior = (try load(txn)) orelse return error.InitialChildPublicationMissing;
    if (!prior.samePlan(expected)) return error.InitialChildPublicationChanged;
    if (prior.phase == phase) return prior;
    if (prior.phase != .hidden) return error.InitialChildPublicationChanged;
    var next = prior;
    next.phase = phase;
    next.phase_term = term;
    next.phase_index = index;
    const bytes = try next.encode();
    try txn.put(key, &bytes);
    return next;
}

/// A cancellation decided before a hidden owner was provisioned still leaves
/// a durable tombstone. Otherwise a delayed provision Raft entry could revive
/// the candidate after metadata has canceled its publication.
pub fn stageUnprovisionedCancel(txn: anytype, record: Record) !Record {
    if (record.phase != .canceled or record.provision_term != 0 or record.provision_index != 0) return error.InvalidInitialChildPublication;
    const encoded = try record.encode();
    if (try load(txn)) |prior| {
        if (!prior.samePlan(record) or prior.phase != .canceled) return error.InitialChildPublicationChanged;
        return prior;
    }
    try txn.put(key, &encoded);
    return record;
}

test "initial child durable marker rejects corruption and keeps canceled owners hidden" {
    const record: Record = .{
        .phase = .hidden,
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 11 },
        .schema_version = 1,
        .row_count = 0,
        .schema_digest = @splat(3),
        .public_schema_json_digest = @splat(4),
        .catalog_digest = @splat(5),
        .provision_term = 2,
        .provision_index = 3,
    };
    const bytes = try record.encode();
    try std.testing.expect((try Record.decode(&bytes)).samePlan(record));
    var corrupt = bytes;
    corrupt[56] ^= 1;
    try std.testing.expectError(error.InvalidInitialChildPublication, Record.decode(&corrupt));
    var canceled = record;
    canceled.phase = .canceled;
    canceled.phase_term = 2;
    canceled.phase_index = 4;
    _ = try canceled.encode();
}
