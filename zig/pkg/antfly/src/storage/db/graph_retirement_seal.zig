// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Exact old-owner graph cutover evidence for empty-generation retirement.
//! The fence and plan are durable before graph admission closes; the seal is
//! recorded only after every pre-fence graph worker snapshot has drained.
const std = @import("std");
const topology = @import("relational_integrity_topology_contract.zig");
const graph_config = @import("graph_retirement_config.zig");

pub const intent_key = "\x00\x00__metadata__:graph_retirement_intent_v1";
pub const receipt_key = "\x00\x00__metadata__:graph_retirement_seal_v1";
const scope_magic = "GRS1";
const receipt_magic = "GRR1";
const scope_payload_len = 4 + 136 + 16 + 32 + 8 + 32;
const scope_len = scope_payload_len + 32;
const receipt_payload_len = 4 + 32 + 32 + 8 + 8;
const receipt_len = receipt_payload_len + 32;

pub const Scope = struct {
    fence: topology.Fence,
    plan_id: [16]u8,
    plan_digest: [32]u8,
    target_table_id: u64,
    graph_config_digest: [32]u8,

    pub fn validate(self: Scope) !void {
        _ = try self.fence.encode();
        if (self.fence.role != .rewrite_source or self.target_table_id == 0 or
            self.target_table_id == self.fence.namespace.table_id or
            std.mem.allEqual(u8, &self.plan_id, 0) or
            std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.graph_config_digest, 0))
            return error.InvalidGraphRetirementScope;
    }

    pub fn retirementDigest(self: Scope) graph_config.Digest {
        return graph_config.retirementDigest(self.fence.namespace.table_id, self.target_table_id, self.graph_config_digest);
    }

    pub fn encode(self: Scope) ![scope_len]u8 {
        try self.validate();
        var bytes: [scope_len]u8 = undefined;
        @memcpy(bytes[0..4], scope_magic);
        const fence = try self.fence.encode();
        @memcpy(bytes[4..140], &fence);
        @memcpy(bytes[140..156], &self.plan_id);
        @memcpy(bytes[156..188], &self.plan_digest);
        std.mem.writeInt(u64, bytes[188..196], self.target_table_id, .little);
        @memcpy(bytes[196..228], &self.graph_config_digest);
        std.crypto.hash.Blake3.hash(bytes[0..scope_payload_len], bytes[scope_payload_len..], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Scope {
        if (bytes.len != scope_len or !std.mem.eql(u8, bytes[0..4], scope_magic)) return error.InvalidGraphRetirementScope;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..scope_payload_len], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[scope_payload_len..])) return error.InvalidGraphRetirementScope;
        const result: Scope = .{
            .fence = topology.Fence.decode(bytes[4..140]) catch return error.InvalidGraphRetirementScope,
            .plan_id = bytes[140..156].*,
            .plan_digest = bytes[156..188].*,
            .target_table_id = std.mem.readInt(u64, bytes[188..196], .little),
            .graph_config_digest = bytes[196..228].*,
        };
        try result.validate();
        return result;
    }

    pub fn eql(a: Scope, b: Scope) bool {
        return a.fence.eql(b.fence) and std.mem.eql(u8, &a.plan_id, &b.plan_id) and
            std.mem.eql(u8, &a.plan_digest, &b.plan_digest) and a.target_table_id == b.target_table_id and
            std.mem.eql(u8, &a.graph_config_digest, &b.graph_config_digest);
    }

    /// Metadata independently computes this digest from its immutable plan.
    /// A read-index owner status must additionally attest nonzero Raft term and
    /// index before the coordinator can submit it as an old-fenced receipt.
    pub fn sealDigest(self: Scope) ![32]u8 {
        const encoded = try self.encode();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-graph-retirement-owner-seal-v1\x00");
        hash.update(&encoded);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
};

pub const Receipt = struct {
    scope_digest: [32]u8,
    digest: [32]u8,
    applied_term: u64,
    applied_index: u64,

    pub fn encode(self: Receipt) ![receipt_len]u8 {
        if (self.applied_term == 0 or self.applied_index == 0 or
            std.mem.allEqual(u8, &self.scope_digest, 0) or std.mem.allEqual(u8, &self.digest, 0))
            return error.InvalidGraphRetirementSeal;
        var bytes: [receipt_len]u8 = undefined;
        @memcpy(bytes[0..4], receipt_magic);
        @memcpy(bytes[4..36], &self.scope_digest);
        @memcpy(bytes[36..68], &self.digest);
        std.mem.writeInt(u64, bytes[68..76], self.applied_term, .little);
        std.mem.writeInt(u64, bytes[76..84], self.applied_index, .little);
        std.crypto.hash.Blake3.hash(bytes[0..receipt_payload_len], bytes[receipt_payload_len..], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Receipt {
        if (bytes.len != receipt_len or !std.mem.eql(u8, bytes[0..4], receipt_magic)) return error.InvalidGraphRetirementSeal;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..receipt_payload_len], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[receipt_payload_len..])) return error.InvalidGraphRetirementSeal;
        const result: Receipt = .{
            .scope_digest = bytes[4..36].*,
            .digest = bytes[36..68].*,
            .applied_term = std.mem.readInt(u64, bytes[68..76], .little),
            .applied_index = std.mem.readInt(u64, bytes[76..84], .little),
        };
        _ = try result.encode();
        return result;
    }
};

pub const Status = struct { intent: ?Scope, receipt: ?Receipt };

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn status(txn: anytype) !Status {
    const intent = if (try optional(txn, intent_key)) |bytes| try Scope.decode(bytes) else null;
    const receipt = if (try optional(txn, receipt_key)) |bytes| try Receipt.decode(bytes) else null;
    if (receipt != null and intent == null) return error.InvalidGraphRetirementSeal;
    if (intent) |scope| {
        const current = try @import("relational_integrity_topology.zig").current(txn);
        const completed = try @import("relational_integrity_topology.zig").completed(txn);
        if (!(if (current) |fence| fence.eql(scope.fence) else false) and
            !(if (completed) |fence| fence.eql(scope.fence) else false)) return error.InvalidGraphRetirementScope;
        if (receipt) |sealed| {
            var digest: [32]u8 = undefined;
            const encoded = try scope.encode();
            std.crypto.hash.Blake3.hash(&encoded, &digest, .{});
            const expected_seal = try scope.sealDigest();
            if (!std.mem.eql(u8, &sealed.scope_digest, &digest) or
                !std.mem.eql(u8, &sealed.digest, &expected_seal)) return error.InvalidGraphRetirementSeal;
        }
    }
    return .{ .intent = intent, .receipt = receipt };
}

pub fn stageBegin(txn: anytype, scope: Scope) !void {
    try scope.validate();
    // The caller staged the exact topology begin in this same write txn.
    // DocStore transactions do not read their own pending writes, so do not
    // probe current() here; the atomic commit binds both records.
    if (try optional(txn, intent_key)) |raw| {
        const previous = try Scope.decode(raw);
        if (previous.eql(scope)) return;
        const completed = (try @import("relational_integrity_topology.zig").completed(txn)) orelse return error.InvalidGraphRetirementScope;
        if (!completed.eql(previous.fence) or scope.fence.admission_epoch <= previous.fence.admission_epoch or
            (try @import("relational_integrity_topology.zig").current(txn)) != null) return error.InvalidGraphRetirementScope;
        // The prior plan completed and a strictly newer topology begin is
        // staged by the caller in this transaction. Replace both records
        // atomically; a stale receipt must never seal the new generation.
        try txn.delete(receipt_key);
    }
    const encoded = try scope.encode();
    try txn.put(intent_key, &encoded);
}

pub fn stageSeal(txn: anytype, scope: Scope, term: u64, index: u64) !Receipt {
    const current = try status(txn);
    const intent = current.intent orelse return error.InvalidGraphRetirementScope;
    if (!intent.eql(scope)) return error.InvalidGraphRetirementScope;
    if (current.receipt) |previous| return previous;
    const active = (try @import("relational_integrity_topology.zig").current(txn)) orelse return error.InvalidGraphRetirementScope;
    if (!active.eql(scope.fence)) return error.InvalidGraphRetirementScope;
    const encoded_scope = try scope.encode();
    var scope_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(&encoded_scope, &scope_digest, .{});
    const receipt: Receipt = .{ .scope_digest = scope_digest, .digest = try scope.sealDigest(), .applied_term = term, .applied_index = index };
    const encoded = try receipt.encode();
    try txn.put(receipt_key, &encoded);
    return receipt;
}

pub fn stageCancel(txn: anytype, fence: topology.Fence) !bool {
    const raw = (try optional(txn, intent_key)) orelse return false;
    const intent = try Scope.decode(raw);
    if (!intent.fence.eql(fence)) return error.InvalidGraphRetirementScope;
    // A sealed owner may already have an irrevocable metadata publication.
    // Generic topology cancellation has no authority to revoke that proof.
    // Likewise, a late cancel/abort after a completed cutover must not
    // reopen graph admission on the retired physical owner.
    if ((try optional(txn, receipt_key)) != null) return error.IntegrityTopologyCutoverRequired;
    const active = (try @import("relational_integrity_topology.zig").current(txn)) orelse return error.IntegrityTopologyCutoverRequired;
    if (!active.eql(fence)) return error.IntegrityTopologyChanged;
    try txn.delete(intent_key);
    try txn.delete(receipt_key);
    return true;
}

test "graph retirement scope and receipt bind owner fence, plan, graph config and Raft apply" {
    const scope: Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .peer_group_id = 401, .owner_group_id = 301, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) },
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .target_table_id = 10,
        .graph_config_digest = @splat(3),
    };
    const encoded = try scope.encode();
    try std.testing.expect(Scope.eql(scope, try Scope.decode(&encoded)));
    var changed = scope;
    changed.graph_config_digest = @splat(5);
    const original_digest = try scope.sealDigest();
    const changed_config_digest = try changed.sealDigest();
    try std.testing.expect(!std.mem.eql(u8, &original_digest, &changed_config_digest));
    changed = scope;
    changed.target_table_id = 11;
    const changed_target_digest = try changed.sealDigest();
    try std.testing.expect(!std.mem.eql(u8, &original_digest, &changed_target_digest));
    const receipt: Receipt = .{ .scope_digest = @splat(6), .digest = try scope.sealDigest(), .applied_term = 2, .applied_index = 3 };
    const encoded_receipt = try receipt.encode();
    try std.testing.expectEqualDeep(receipt, try Receipt.decode(&encoded_receipt));
    var forged = encoded_receipt;
    forged[36] ^= 1;
    try std.testing.expectError(error.InvalidGraphRetirementSeal, Receipt.decode(&forged));
}

test "graph retirement late cancel cannot revoke a seal or completed cutover" {
    const scope: Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .admission_epoch = 1, .peer_group_id = 401, .owner_group_id = 301, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) },
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .target_table_id = 10,
        .graph_config_digest = @splat(3),
    };
    const encoded_scope = try scope.encode();
    const encoded_fence = try scope.fence.encode();
    const receipt: Receipt = .{ .scope_digest = @splat(6), .digest = try scope.sealDigest(), .applied_term = 2, .applied_index = 3 };
    const encoded_receipt = try receipt.encode();
    const Fake = struct {
        scope_bytes: []const u8,
        fence_bytes: ?[]const u8,
        graph_receipt_bytes: ?[]const u8 = null,
        completed_bytes: ?[]const u8 = null,
        deletes: usize = 0,

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (std.mem.eql(u8, key, intent_key)) return self.scope_bytes;
            if (std.mem.eql(u8, key, receipt_key)) return self.graph_receipt_bytes orelse return error.NotFound;
            if (std.mem.eql(u8, key, @import("relational_integrity_topology.zig").fence_key)) return self.fence_bytes orelse return error.NotFound;
            if (std.mem.eql(u8, key, @import("relational_integrity_topology.zig").receipt_key)) return self.completed_bytes orelse return error.NotFound;
            return error.NotFound;
        }

        pub fn delete(self: *@This(), _: []const u8) !void {
            self.deletes += 1;
        }
    };
    var fake: Fake = .{ .scope_bytes = &encoded_scope, .fence_bytes = &encoded_fence, .graph_receipt_bytes = &encoded_receipt };
    try std.testing.expectError(error.IntegrityTopologyCutoverRequired, stageCancel(&fake, scope.fence));
    try std.testing.expectEqual(@as(usize, 0), fake.deletes);
    fake.graph_receipt_bytes = null;
    fake.completed_bytes = &encoded_fence;
    try std.testing.expectError(error.IntegrityTopologyCutoverRequired, stageCancel(&fake, scope.fence));
    try std.testing.expectEqual(@as(usize, 0), fake.deletes);
    fake.fence_bytes = null;
    try std.testing.expectError(error.IntegrityTopologyCutoverRequired, stageCancel(&fake, scope.fence));
    try std.testing.expectEqual(@as(usize, 0), fake.deletes);
    fake.completed_bytes = null;
    fake.fence_bytes = &encoded_fence;
    try std.testing.expect(try stageCancel(&fake, scope.fence));
    try std.testing.expectEqual(@as(usize, 2), fake.deletes);
}
