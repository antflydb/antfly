// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable installation intent for already provisioned tables. Pending is not
//! permission to prepare; active is published only after all applying members
//! return challenge-bound installed-backing evidence. An installed generation
//! has no timeout/abort transition: its structural fence survives leader loss.
const std = @import("std");
const table_storage = @import("../common/table_storage.zig");
const incarnation = @import("incarnation.zig");
const topology = @import("topology_protocol.zig");
const Sha = std.crypto.hash.sha2.Sha256;
pub const max_groups = 64;
pub const max_encoded_bytes = 128 * 1024;
pub const Phase = enum { pending, active };
pub const Group = struct {
    group_id: u64,
    range_id: u64,
    split_attempt_epoch: u64,
    range_digest: [32]u8,
};
pub const Record = struct {
    version: u32 = 1,
    phase: Phase = .pending,
    cluster_incarnation: incarnation.MetadataClusterIncarnation,
    table_id: u64,
    expected_definition: [32]u8,
    schema_catalog_digest: [32]u8,
    expected_transition_generation: u64,
    generation: u64,
    policy: table_storage.TransactionRecovery,
    groups: []const Group,
    /// Digest of validated fresh evidence for the complete membership. The
    /// replicated apply path checks state/CAS; it does not perform network I/O.
    evidence_digest: [32]u8 = @splat(0),

    pub fn validate(self: @This()) !void {
        if (self.version != 1 or !incarnation.isValid(self.cluster_incarnation) or self.table_id == 0 or
            self.expected_transition_generation == std.math.maxInt(u64) or self.generation != self.expected_transition_generation + 1 or
            self.groups.len == 0 or self.groups.len > max_groups or std.mem.allEqual(u8, &self.expected_definition, 0) or std.mem.allEqual(u8, &self.schema_catalog_digest, 0)) return error.InvalidCompletionActivation;
        try self.policy.validate();
        if (self.policy.completion_protocol_version != 1 or self.policy.profile_version != 1) return error.InvalidCompletionActivation;
        var last: u64 = 0;
        for (self.groups) |group| {
            if (group.group_id == 0 or group.group_id <= last or std.mem.allEqual(u8, &group.range_digest, 0)) return error.InvalidCompletionActivation;
            last = group.group_id;
        }
        const no_evidence = std.mem.allEqual(u8, &self.evidence_digest, 0);
        if ((self.phase == .pending) != no_evidence) return error.InvalidCompletionActivation;
    }

    pub fn membership(self: @This()) !topology.RangeMembership {
        var result: topology.RangeMembershipAccumulator = .{};
        for (self.groups) |group| try result.add(group.group_id);
        return result.finish(self.table_id);
    }

    pub fn groupIncarnation(self: @This(), group: Group) [16]u8 {
        var hash = Sha.init(.{});
        hash.update("antfly-completion-group-incarnation-v1");
        hash.update(&self.cluster_incarnation);
        hash.update(&self.expected_definition);
        hash.update(&self.schema_catalog_digest);
        for ([_]u64{ self.table_id, group.group_id, group.range_id, group.split_attempt_epoch, self.generation }) |value| integer(&hash, value);
        hash.update(&group.range_digest);
        const digest = hash.finalResult();
        return digest[0..16].*;
    }

    pub fn sameIntent(a: @This(), b: @This()) bool {
        if (a.version != b.version or a.table_id != b.table_id or a.generation != b.generation or
            a.expected_transition_generation != b.expected_transition_generation or
            !std.meta.eql(a.cluster_incarnation, b.cluster_incarnation) or !std.meta.eql(a.expected_definition, b.expected_definition) or !std.meta.eql(a.schema_catalog_digest, b.schema_catalog_digest) or
            !std.meta.eql(a.policy, b.policy) or a.groups.len != b.groups.len) return false;
        for (a.groups, b.groups) |x, y| if (!std.meta.eql(x, y)) return false;
        return true;
    }
};
fn integer(hash: *Sha, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
pub fn policyDigest(policy: table_storage.TransactionRecovery) [32]u8 {
    var hash = Sha.init(.{});
    hash.update("antfly-completion-policy-v1");
    for ([_]u64{ policy.protocol_version, policy.max_count, policy.max_bytes, policy.max_transaction_bytes, policy.completion_protocol_version, policy.profile_version }) |value| integer(&hash, value);
    return hash.finalResult();
}
pub fn encode(alloc: std.mem.Allocator, record: Record) ![]u8 {
    try record.validate();
    const bytes = try std.json.Stringify.valueAlloc(alloc, record, .{});
    errdefer alloc.free(bytes);
    if (bytes.len > max_encoded_bytes) return error.InvalidCompletionActivation;
    return bytes;
}
pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Record) {
    if (bytes.len > max_encoded_bytes) return error.InvalidCompletionActivation;
    var parsed = try std.json.parseFromSlice(Record, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

test "workload admission completion activation identities bind exact policy and durable generation" {
    const group: Group = .{ .group_id = 11, .range_id = 3, .split_attempt_epoch = 0, .range_digest = @splat(7) };
    var record: Record = .{ .cluster_incarnation = "0123456789abcdef0123456789abcdef".*, .table_id = 9, .expected_definition = @splat(5), .schema_catalog_digest = @splat(6), .expected_transition_generation = 4, .generation = 5, .policy = .{ .protocol_version = 1, .max_count = 4, .max_bytes = 65536, .max_transaction_bytes = 16384, .completion_protocol_version = 1, .profile_version = 1 }, .groups = &.{group} };
    const encoded = try encode(std.testing.allocator, record);
    defer std.testing.allocator.free(encoded);
    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit();
    try std.testing.expect(record.sameIntent(decoded.value));
    const identity = record.groupIncarnation(group);
    var changed_range = group;
    changed_range.range_digest[0] ^= 1;
    try std.testing.expect(!std.meta.eql(identity, record.groupIncarnation(changed_range)));
    record.generation += 1;
    record.expected_transition_generation += 1;
    try std.testing.expect(!std.meta.eql(identity, record.groupIncarnation(group)));
    const digest = policyDigest(record.policy);
    record.policy.max_count += 1;
    try std.testing.expect(!std.meta.eql(digest, policyDigest(record.policy)));
    record.phase = .active;
    try std.testing.expectError(error.InvalidCompletionActivation, record.validate());
    record.evidence_digest = @splat(8);
    try record.validate();
}
