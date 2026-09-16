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

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const integrity = @import("relational_integrity_contract.zig");
const identity = @import("doc_identity_namespace.zig");

pub const Identity = struct {
    namespace: identity.Namespace,
    catalog_digest: [32]u8,
    next_epoch: u64,
    backup_seal_supported: bool = false,
};

pub const Status = struct { fence: ?Fence, drained: bool };

pub const fence_key = "\x00\x00__metadata__:relational_integrity_topology_fence";

pub const receipt_key = "\x00\x00__metadata__:relational_integrity_topology_receipt";

pub const abort_prefix = "\x00\x00__metadata__:relational_integrity_topology_aborted:";

pub const Role = enum(u8) { split_source = 1, split_destination = 2, merge_source = 3, merge_destination = 4, backup_snapshot = 5, rewrite_source = 6 };

pub const Fence = struct {
    admission_epoch: u64 = 1,
    transition_id: u64,
    attempt: u64,
    peer_group_id: u64,
    owner_group_id: u64,
    role: Role,
    namespace: identity.Namespace,
    catalog_digest: integrity.Digest,

    pub fn eql(a: Fence, b: Fence) bool {
        return a.admission_epoch == b.admission_epoch and a.transition_id == b.transition_id and a.attempt == b.attempt and
            a.peer_group_id == b.peer_group_id and a.owner_group_id == b.owner_group_id and a.role == b.role and
            a.namespace.eql(b.namespace) and std.mem.eql(u8, &a.catalog_digest, &b.catalog_digest);
    }

    pub fn encode(self: Fence) ![136]u8 {
        if (self.admission_epoch == 0 or self.transition_id == 0 or self.attempt == 0 or self.peer_group_id == 0 or self.owner_group_id == 0 or self.namespace.table_id == 0)
            return error.InvalidIntegrityTopologyFence;
        var bytes: [136]u8 = @splat(0);
        @memcpy(bytes[0..4], "AIT1");
        bytes[4] = @intFromEnum(self.role);
        std.mem.writeInt(u64, bytes[8..16], self.transition_id, .little);
        std.mem.writeInt(u64, bytes[16..24], self.attempt, .little);
        std.mem.writeInt(u64, bytes[24..32], self.peer_group_id, .little);
        std.mem.writeInt(u64, bytes[32..40], self.namespace.table_id, .little);
        std.mem.writeInt(u64, bytes[40..48], self.namespace.shard_id, .little);
        std.mem.writeInt(u64, bytes[48..56], self.namespace.range_id, .little);
        @memcpy(bytes[56..88], &self.catalog_digest);
        std.mem.writeInt(u64, bytes[88..96], self.owner_group_id, .little);
        std.mem.writeInt(u64, bytes[96..104], self.admission_epoch, .little);
        std.crypto.hash.Blake3.hash(bytes[0..104], bytes[104..136], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Fence {
        if (bytes.len != 136 or !std.mem.eql(u8, bytes[0..4], "AIT1") or
            !std.mem.allEqual(u8, bytes[5..8], 0)) return error.InvalidIntegrityTopologyFence;
        var checksum: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..104], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[104..136])) return error.InvalidIntegrityTopologyFence;
        const result: Fence = .{
            .admission_epoch = std.mem.readInt(u64, bytes[96..104], .little),
            .transition_id = std.mem.readInt(u64, bytes[8..16], .little),
            .attempt = std.mem.readInt(u64, bytes[16..24], .little),
            .peer_group_id = std.mem.readInt(u64, bytes[24..32], .little),
            .owner_group_id = std.mem.readInt(u64, bytes[88..96], .little),
            .role = std.enums.fromInt(Role, bytes[4]) orelse return error.InvalidIntegrityTopologyFence,
            .namespace = .{
                .table_id = std.mem.readInt(u64, bytes[32..40], .little),
                .shard_id = std.mem.readInt(u64, bytes[40..48], .little),
                .range_id = std.mem.readInt(u64, bytes[48..56], .little),
            },
            .catalog_digest = bytes[56..88].*,
        };
        _ = try result.encode();
        return result;
    }
};

pub const Command = struct {
    fence: Fence,
    action: enum { begin, release, cancel, abort_transition, transfer, prune },
    transfer: ?@import("relational_integrity_handoff_contract.zig").Command = null,
    pub fn jsonStringify(self: Command, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
