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

pub const Role = enum(u8) { split_source = 1, split_destination = 2, merge_source = 3, merge_destination = 4, backup_snapshot = 5, rewrite_source = 6, truncate_parent = 7, child_generation_parent = 8, child_generation_source = 9 };

pub const ParentRetirementEntry = struct {
    child_table_id: u64,
    child_table_name: []const u8,
    constraint_name: []const u8,
    generation: integrity.Generation,
    next_generation: integrity.Generation,
};

pub const ParentRetirementStage = struct {
    plan_digest: integrity.Digest,
    entries: []const ParentRetirementEntry,
};

pub const ParentActivation = struct {
    plan_id: [16]u8,
    plan_digest: integrity.Digest,
    publication_digest: integrity.Digest,
};

pub const ChildSchemaInstall = struct {
    schema_json: []const u8,
    before_schema_json_digest: integrity.Digest,
    schema_json_digest: integrity.Digest,
    before_catalog_digest: integrity.Digest,
    after_catalog_digest: integrity.Digest,

    pub fn nativeJsonProjection(self: ChildSchemaInstall) struct {
        schema_json: std.json.Value,
        before_schema_json_digest: integrity.Digest,
        schema_json_digest: integrity.Digest,
        before_catalog_digest: integrity.Digest,
        after_catalog_digest: integrity.Digest,
    } {
        return .{
            .schema_json = .{ .string = self.schema_json },
            .before_schema_json_digest = self.before_schema_json_digest,
            .schema_json_digest = self.schema_json_digest,
            .before_catalog_digest = self.before_catalog_digest,
            .after_catalog_digest = self.after_catalog_digest,
        };
    }
};

pub const InitialChildProvision = struct {
    schema_json: []const u8,
    child_table_name: []const u8,
    plan_id: [16]u8,
    plan_digest: integrity.Digest,
    schema_digest: integrity.Digest,
    public_schema_json_digest: integrity.Digest,
    catalog_digest: integrity.Digest,

    pub fn nativeJsonProjection(self: InitialChildProvision) struct {
        schema_json: std.json.Value,
        child_table_name: []const u8,
        plan_id: [16]u8,
        plan_digest: integrity.Digest,
        schema_digest: integrity.Digest,
        public_schema_json_digest: integrity.Digest,
        catalog_digest: integrity.Digest,
    } {
        return .{
            .schema_json = .{ .string = self.schema_json },
            .child_table_name = self.child_table_name,
            .plan_id = self.plan_id,
            .plan_digest = self.plan_digest,
            .schema_digest = self.schema_digest,
            .public_schema_json_digest = self.public_schema_json_digest,
            .catalog_digest = self.catalog_digest,
        };
    }
};

pub const InitialChildControl = struct {
    plan_id: [16]u8,
    plan_digest: integrity.Digest,
    schema_version: u32,
    schema_digest: integrity.Digest,
    public_schema_json_digest: integrity.Digest,
    catalog_digest: integrity.Digest,
};

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
    action: enum { begin, release, cancel, abort_transition, transfer, prune, stage_parent_retirement, activate_parent_retirement, acknowledge_parent_retirement, stage_child_generation, activate_child_generation, acknowledge_child_generation, cancel_child_generation_source, install_child_schema, provision_initial_child, release_initial_child, cancel_initial_child },
    transfer: ?@import("relational_integrity_handoff_contract.zig").Command = null,
    parent_retirement: ?ParentRetirementStage = null,
    parent_activation: ?ParentActivation = null,
    child_generations: ?[]const @import("relational_integrity_generation_admission.zig").Transition = null,
    child_schema_install: ?ChildSchemaInstall = null,
    initial_child_provision: ?InitialChildProvision = null,
    initial_child_control: ?InitialChildControl = null,
    pub fn jsonStringify(self: Command, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
