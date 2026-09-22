// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Internal replicated retention controls. A scope digest names a consumer; it
//! is NOT an immutable snapshot certificate or receiver durability receipt.
const std = @import("std");

pub fn finalCutDigest(scope: Scope, sequence: u64, applied_index: u64) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-online-source-final-cut-v1");
    hash.update(&scope.pin());
    var cut: [16]u8 = undefined;
    std.mem.writeInt(u64, cut[0..8], sequence, .little);
    std.mem.writeInt(u64, cut[8..16], applied_index, .little);
    hash.update(&cut);
    return hash.finalResult();
}
const topology = @import("relational_integrity_topology_contract.zig");
const identity = @import("doc_identity_namespace.zig");
pub const Authority = @import("../source_authority.zig").Kind;
pub const scope_encoded_size = 192;

/// These are deterministic results of a replicated command, including the
/// durable journal quota (not process-local memory pressure). Advance rejected
/// Raft entries so following ACK/reclaim commands can release that quota.
/// Corrupt storage, missing history, OOM and executor pressure are not here.
pub const Rejection = error{
    RetainedEffectsFull,
    RetainedEffectsConsumerLimit,
    RetainedEffectsNamespaceMismatch,
    RetainedEffectsFenceMismatch,
    RetainedEffectsCursorMismatch,
    InvalidRetainedEffectsAdmission,
    InvalidOnlineSourceCommand,
    OnlineSourceScopeChanged,
    InvalidSourceSnapshot,
    SourceSnapshotCutMismatch,
};

pub const Scope = struct {
    version: u8 = 2,
    authority: Authority = .raft,
    fence: topology.Fence,
    receiver_namespace: identity.Namespace,
    consumer_epoch: u64,
    copy_attempt: @import("relational_integrity_handoff_contract.zig").MergeCopyAttempt,

    pub fn validate(self: Scope) !void {
        if (self.version != 2 or self.consumer_epoch == 0 or self.copy_attempt.sequence == 0 or (self.fence.role != .merge_source and self.fence.role != .rewrite_source) or
            self.receiver_namespace.table_id == 0 or self.receiver_namespace.shard_id == 0 or
            self.receiver_namespace.range_id == 0 or self.fence.namespace.shard_id == 0 or self.fence.namespace.range_id == 0 or self.fence.namespace.eql(self.receiver_namespace))
            return error.InvalidOnlineSourceCommand;
        switch (self.authority) {
            .raft => if (self.copy_attempt.donor_term == 0) return error.InvalidOnlineSourceCommand,
            .native => if (self.fence.role != .rewrite_source or self.copy_attempt.donor_term != 0 or self.copy_attempt.sequence != self.consumer_epoch) return error.InvalidOnlineSourceCommand,
        }
        _ = try self.fence.encode();
        // A rewritten live owner may retain a pre-split document namespace.
        // Routing group and physical identity are independently authenticated
        // by the private owner port and native namespace fence, respectively.
        // The hidden target is freshly allocated; merge admission keeps its
        // existing stricter identity requirement.
        if ((self.fence.role == .merge_source and self.fence.owner_group_id != self.fence.namespace.shard_id) or
            self.fence.peer_group_id != self.receiver_namespace.shard_id or
            self.fence.owner_group_id == self.fence.peer_group_id) return error.InvalidOnlineSourceCommand;
        // A rewrite creates a hidden generation of the table. Unlike a merge,
        // its fresh target identity MUST differ from the live source table.
        if ((self.fence.namespace.table_id == self.receiver_namespace.table_id) != (self.fence.role == .merge_source)) return error.InvalidOnlineSourceCommand;
    }
    pub fn namespace(self: Scope) [24]u8 {
        return namespaceBytes(self.fence.namespace);
    }
    pub fn encode(self: Scope) ![scope_encoded_size]u8 {
        try self.validate();
        var bytes: [scope_encoded_size]u8 = @splat(0);
        @memcpy(bytes[0..136], &try self.fence.encode());
        @memcpy(bytes[136..160], &namespaceBytes(self.receiver_namespace));
        std.mem.writeInt(u64, bytes[160..168], self.consumer_epoch, .little);
        std.mem.writeInt(u64, bytes[168..176], self.copy_attempt.donor_term, .little);
        std.mem.writeInt(u64, bytes[176..184], self.copy_attempt.sequence, .little);
        bytes[184] = self.version;
        bytes[185] = @intFromEnum(self.authority);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Scope {
        if (bytes.len != scope_encoded_size or !std.mem.allEqual(u8, bytes[186..192], 0)) return error.InvalidOnlineSourceCommand;
        const value: Scope = .{ .version = bytes[184], .authority = std.enums.fromInt(Authority, bytes[185]) orelse return error.InvalidOnlineSourceCommand, .fence = try topology.Fence.decode(bytes[0..136]), .receiver_namespace = .{ .table_id = std.mem.readInt(u64, bytes[136..144], .big), .shard_id = std.mem.readInt(u64, bytes[144..152], .big), .range_id = std.mem.readInt(u64, bytes[152..160], .big) }, .consumer_epoch = std.mem.readInt(u64, bytes[160..168], .little), .copy_attempt = .{ .donor_term = std.mem.readInt(u64, bytes[168..176], .little), .sequence = std.mem.readInt(u64, bytes[176..184], .little) } };
        try value.validate();
        return value;
    }
    pub fn pin(self: Scope) [32]u8 {
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly-online-source-consumer-v2");
        h.update(&.{ self.version, @intFromEnum(self.authority) });
        const fence = self.fence.encode() catch unreachable;
        h.update(&fence);
        h.update(&namespaceBytes(self.receiver_namespace));
        var epoch: [8]u8 = undefined;
        std.mem.writeInt(u64, &epoch, self.consumer_epoch, .little);
        h.update(&epoch);
        std.mem.writeInt(u64, &epoch, self.copy_attempt.donor_term, .little);
        h.update(&epoch);
        std.mem.writeInt(u64, &epoch, self.copy_attempt.sequence, .little);
        h.update(&epoch);
        return h.finalResult();
    }
};

pub fn namespaceBytes(value: identity.Namespace) [24]u8 {
    var result: [24]u8 = undefined;
    // Match doc_identity.encodeNamespace's durable representation; do not
    // confuse it with the little-endian field encoding inside topology fences.
    std.mem.writeInt(u64, result[0..8], value.table_id, .big);
    std.mem.writeInt(u64, result[8..16], value.shard_id, .big);
    std.mem.writeInt(u64, result[16..24], value.range_id, .big);
    return result;
}

pub const Command = union(enum) {
    admit: struct { scope: Scope, limit: u64 = 256 * 1024 * 1024 },
    acknowledge: struct { scope: Scope, previous: u64, next: u64 },
    release: Scope,
    final_fence: struct { scope: Scope, expected_sequence: u64 },
    publish_certificate: struct { scope: Scope, certificate: @import("../source_snapshot.zig").Certificate },
    /// Explicit bounded maintenance; never reclaim an unacknowledged frame.
    reclaim: struct { scope: Scope, frame_limit: u8 = 16, byte_limit: u32 = 1024 * 1024 },

    pub fn scope(self: Command) Scope {
        return switch (self) {
            .release => |value| value,
            inline else => |value| value.scope,
        };
    }
    pub fn validate(self: Command) !void {
        try self.scope().validate();
        switch (self) {
            .admit => |value| if (value.limit < 16 * 1024 * 1024) return error.InvalidOnlineSourceCommand,
            .acknowledge => |value| if (value.next < value.previous) return error.InvalidOnlineSourceCommand,
            .reclaim => |value| if (value.frame_limit == 0 or value.frame_limit > 128 or value.byte_limit == 0 or value.byte_limit > 16 * 1024 * 1024) return error.InvalidOnlineSourceCommand,
            .publish_certificate => |value| {
                _ = try value.certificate.encode();
                if (value.certificate.integrity) |binding| if (!std.mem.eql(u8, &binding.catalog_digest, &value.scope.fence.catalog_digest)) return error.SourceSnapshotCutMismatch;
            },
            else => {},
        }
    }
    pub fn jsonStringify(self: Command, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};

pub fn validateRequest(req: anytype) !void {
    const command = req.online_source orelse return;
    try command.validate();
    if (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
        req.schema_version != null or req.relational_schema_version != null or req.relational_integrity_generation_set != null or req.timestamp_ns != 0 or
        req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.integrity.len != 0 or
        req.integrity_commands.len != 0 or req.predicates.len != 0 or req.transaction != null or
        req.restore_staging != null or req.restore_staging_scope != null or req.restore_staging_plan_id != null or req.relational_topology != null or
        req.relational_activation != null or req.relational_retirement != null or req.relational_index_maintenance != null or
        req.relational_repair or req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or
        req.merge_checkpoint != null or req.merge_replication != null or req.merge_source_transition != null or req.merge_page != null or
        req.merge_artifacts.len != 0) return error.InvalidOnlineSourceCommand;
}
