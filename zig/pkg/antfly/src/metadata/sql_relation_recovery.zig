// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Authority for replacing lost UNLOGGED images through the existing staged
//! empty-generation coordinator. These records contain identities/digests only.
//! A missing owner or transaction is never itself a proof of generation loss.
const std = @import("std");
const local = @import("antfly_local_sources");
const persistence = local.common_topology_records.RelationPersistence;
const boots = @import("sql_relation_boots.zig");
const attestation = @import("sql_relation_attestation.zig");

/// Captured in the same metadata read cut as the complete FK/sequence/graph
/// closure. Admission revalidates both this guard and that catalog cut before
/// acquiring the ordinary generation-publication leases.
pub const Guard = struct {
    before: attestation.SignedBinding,
    source_group_id: u64,
    lost_boot_generation: u64,
    lost_native_instance: [16]u8,
    catalog_revision: u64,

    pub fn validate(self: Guard) !void {
        try self.before.verify();
        if (self.before.binding.descriptor.kind != .unlogged or self.source_group_id == 0 or
            self.catalog_revision == 0 or self.lost_boot_generation <= self.before.boot_generation or
            std.mem.allEqual(u8, &self.lost_native_instance, 0) or
            std.mem.eql(u8, &self.lost_native_instance, &self.before.binding.native_instance))
            return error.InvalidSqlRelationPersistence;
    }

    /// The metadata receiver obtains current itself from its owning transaction.
    /// A signed claim from a new process does not manufacture a clean restart.
    pub fn requireLostBoot(self: Guard, current: boots.Record) !void {
        try self.validate();
        try current.validate();
        if (current.phase != .recovering or !current.unclean_restart or
            current.generation != self.lost_boot_generation or
            !std.mem.eql(u8, &current.instance, &self.lost_native_instance) or
            current.node_id != self.before.binding.descriptor.owner_node_id or
            !std.mem.eql(u8, &current.identity.metadata_incarnation, &self.before.identity.metadata_incarnation))
            return error.SqlRelationOwnerChanged;
    }

    pub fn requireSuccessor(self: Guard, next: persistence.Descriptor, ready: boots.Record) !void {
        try self.validate();
        try next.validate();
        try ready.validate();
        if (next.kind != .unlogged or next.data_generation !=
            (std.math.add(u64, self.before.binding.descriptor.data_generation, 1) catch return error.CatalogGenerationExhausted) or
            (ready.phase != .ready and !(ready.phase == .recovering and ready.node_id == self.before.binding.descriptor.owner_node_id and
                ready.generation == self.lost_boot_generation and std.mem.eql(u8, &ready.instance, &self.lost_native_instance))) or
            next.owner_node_id != ready.identity.node_id or next.owner_store_id != ready.identity.store_id or
            !std.mem.eql(u8, &next.generation_instance, &ready.instance) or
            !std.mem.eql(u8, &ready.identity.metadata_incarnation, &self.before.identity.metadata_incarnation))
            return error.SqlRelationOwnerChanged;
    }

    pub fn digest(self: Guard) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-unlogged-loss-source/v1\x00");
        hash.update(&try self.before.digest());
        hash.update(&self.lost_native_instance);
        var bytes: [8]u8 = undefined;
        for ([_]u64{ self.source_group_id, self.lost_boot_generation, self.catalog_revision }) |value| {
            std.mem.writeInt(u64, &bytes, value, .little);
            hash.update(&bytes);
        }
        var value: [32]u8 = undefined;
        hash.final(&value);
        return value;
    }
};

/// An admitted reset records an irreversible source-loss fence before waiting
/// for cross-owner transaction drainage. It never asserts successor readiness.
/// Once this checkpoint exists the reset job cannot release the lost image or
/// take the ordinary cancel-and-resume-source path.
pub const LossFence = struct {
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    plan_id: [16]u8,
    plan_digest: [32]u8,
    source: Guard,
    revision: u64,
    checkpoint_digest: [32]u8,

    pub fn validate(self: LossFence) !void {
        try self.source.validate();
        if (self.metadata_group_id == 0 or self.metadata_group_id != self.source.before.metadata_group_id or self.revision == 0 or
            !std.mem.eql(u8, &self.metadata_incarnation, &self.source.before.identity.metadata_incarnation) or
            std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or std.mem.allEqual(u8, &self.checkpoint_digest, 0))
            return error.InvalidSqlRelationPersistence;
        if (!std.mem.eql(u8, &self.checkpoint_digest, &try self.checkpointDigest())) return error.InvalidSqlRelationPersistence;
    }
    pub fn checkpointDigest(self: LossFence) ![32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-unlogged-loss-fence/v1\x00");
        hash.update(&self.metadata_incarnation);
        hash.update(&self.plan_id);
        hash.update(&self.plan_digest);
        hash.update(&try self.source.digest());
        var bytes: [8]u8 = undefined;
        for ([_]u64{ self.metadata_group_id, self.revision }) |value| {
            std.mem.writeInt(u64, &bytes, value, .little);
            hash.update(&bytes);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn requireCaptured(self: LossFence, group: u64, incarnation: [32]u8, source_group: u64, pin: persistence.PhysicalPin) !void {
        try self.validate();
        if (self.metadata_group_id != group or !std.mem.eql(u8, &self.metadata_incarnation, &incarnation) or
            self.source.source_group_id != source_group or !(try persistence.PhysicalPin.fromBinding(self.source.before.binding)).eql(pin))
            return error.ExternalTransactionDecisionChanged;
    }
};

/// Derived from an immutable terminal empty-generation job at read index. The
/// secondary directory stores only a pointer to that job, never another log.
/// Its authority is irreversible retirement of old, not a route to successor.
pub const PublishedLoss = struct {
    version: u8 = 1,
    metadata_group_id: u64,
    metadata_incarnation: [32]u8,
    plan_id: [16]u8,
    plan_digest: [32]u8,
    source: Guard,
    successor: attestation.SignedBinding,
    target_group_id: u64,
    publication_revision: u64,
    publication_digest: [32]u8,

    pub fn validate(self: PublishedLoss) !void {
        try self.source.validate();
        try self.successor.verify();
        const before = self.source.before.binding;
        const after = self.successor.binding;
        if (self.version != 1 or self.metadata_group_id == 0 or self.target_group_id == 0 or self.publication_revision == 0 or
            self.metadata_group_id != self.source.before.metadata_group_id or self.metadata_group_id != self.successor.metadata_group_id or
            !std.mem.eql(u8, &self.metadata_incarnation, &self.source.before.identity.metadata_incarnation) or
            !std.mem.eql(u8, &self.metadata_incarnation, &self.successor.identity.metadata_incarnation) or
            std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or std.mem.allEqual(u8, &self.publication_digest, 0) or
            self.successor.kind != .initialized or self.successor.creation_scope == null or
            !std.mem.eql(u8, &self.successor.creation_scope.?.plan_id, &self.plan_id) or
            !std.mem.eql(u8, &self.successor.creation_scope.?.plan_digest, &self.plan_digest) or
            after.descriptor.kind != .unlogged or after.has_user_data or after.has_transaction_state or
            after.table_id == before.table_id or after.physical_root_incarnation == before.physical_root_incarnation or
            after.descriptor.data_generation != (std.math.add(u64, before.descriptor.data_generation, 1) catch return error.CatalogGenerationExhausted) or
            !std.mem.eql(u8, &after.descriptor.generation_instance, &after.native_instance))
            return error.InvalidSqlRelationPersistence;
    }

    /// Consumption intentionally makes no assertion about the successor's
    /// *current* process or generation. Further resets cannot resurrect old.
    pub fn requireCaptured(self: PublishedLoss, group: u64, incarnation: [32]u8, source_group: u64, old: persistence.PhysicalPin) !void {
        try self.validate();
        if (self.metadata_group_id != group or !std.mem.eql(u8, &self.metadata_incarnation, &incarnation) or
            self.source.source_group_id != source_group or !(try persistence.PhysicalPin.fromBinding(self.source.before.binding)).eql(old))
            return error.ExternalTransactionDecisionChanged;
    }
};

pub const Query = struct {
    table_id: u64,
    physical_pin: persistence.PhysicalPin,
    pub fn validate(self: Query) !void {
        try self.physical_pin.validate();
        if (self.table_id == 0 or self.table_id != self.physical_pin.table_id) return error.InvalidSqlRelationPersistence;
    }
};

/// Metadata derives this only from a scoped child's irreversible generation
/// retirement checkpoint. A merely reserved/cancelable fence is insufficient.
/// This is not a physical-file deletion receipt or permission to use old rows.
pub const TemporaryRetirement = struct {
    before: attestation.SignedBinding,
    source_group_id: u64,
    namespace_id: u64,
    scope: @import("scope_retirement_ledger.zig").Lookup,
    child_ordinal: u16,
    child_id: [16]u8,
    /// An irreversible metadata loss checkpoint is not a physical child
    /// head. Each tag carries only the immutable identity it can prove.
    checkpoint: union(enum) {
        lost_source_fenced: struct { sequence: u16, digest: [32]u8 },
        physical_ready: struct { child_revision: u64, digest: [32]u8 },
        published: struct { child_revision: u64, digest: [32]u8 },
    },

    pub fn validate(self: TemporaryRetirement) !void {
        try self.before.verify();
        try self.scope.validate();
        const binding = self.before.binding;
        if (binding.descriptor.kind != .temporary or binding.descriptor.owner == null or
            self.source_group_id == 0 or self.namespace_id == 0 or
            self.scope.group != self.before.metadata_group_id or self.child_ordinal >= self.scope.reference.tables or
            (self.scope.reference.kind == .namespace and self.scope.reference.root != self.namespace_id) or
            !std.mem.eql(u8, &self.child_id, &try self.scope.reference.childId(binding.table_id))) return error.InvalidSqlRelationPersistence;
        switch (self.checkpoint) {
            .lost_source_fenced => |point| if (point.sequence == 0 or point.sequence > self.scope.reference.tables or std.mem.allEqual(u8, &point.digest, 0)) return error.InvalidSqlRelationPersistence,
            inline .physical_ready, .published => |point| if (point.child_revision == 0 or std.mem.allEqual(u8, &point.digest, 0)) return error.InvalidSqlRelationPersistence,
        }
    }
    pub fn requireCaptured(self: TemporaryRetirement, group: u64, incarnation: [32]u8, source_group: u64, pin: persistence.PhysicalPin) !void {
        try self.validate();
        if (self.scope.group != group or !std.mem.eql(u8, &self.before.identity.metadata_incarnation, &incarnation) or
            self.source_group_id != source_group or !(try persistence.PhysicalPin.fromBinding(self.before.binding)).eql(pin))
            return error.ExternalTransactionDecisionChanged;
    }
};

pub const RetiredGeneration = union(enum) {
    unlogged_reset: PublishedLoss,
    unlogged_loss_fenced: LossFence,
    temporary_scope: TemporaryRetirement,

    /// Stable across an irreversible fence becoming a published reset and
    /// across subsequent successor boots. This names the retirement event,
    /// while the accepting transaction independently rejoins its checkpoint.
    pub fn identityDigest(self: RetiredGeneration, a: std.mem.Allocator) ![32]u8 {
        const encoded = switch (self) {
            .unlogged_reset => |value| try std.json.Stringify.valueAlloc(a, .{ .kind = "unlogged", .plan = value.plan_id, .digest = value.plan_digest, .source = try value.source.digest() }, .{}),
            .unlogged_loss_fenced => |value| try std.json.Stringify.valueAlloc(a, .{ .kind = "unlogged", .plan = value.plan_id, .digest = value.plan_digest, .source = try value.source.digest() }, .{}),
            .temporary_scope => |value| try std.json.Stringify.valueAlloc(a, .{ .kind = "temporary", .scope = value.scope, .child = value.child_id, .before = try value.before.digest() }, .{}),
        };
        defer a.free(encoded);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-participant-retirement/v1\x00");
        hash.update(encoded);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }

    pub fn beforeBinding(self: RetiredGeneration) attestation.SignedBinding {
        return switch (self) {
            .unlogged_reset => |value| value.source.before,
            .unlogged_loss_fenced => |value| value.source.before,
            .temporary_scope => |value| value.before,
        };
    }
    pub fn requireCaptured(self: RetiredGeneration, group: u64, incarnation: [32]u8, source_group: u64, pin: persistence.PhysicalPin) !void {
        switch (self) {
            .unlogged_reset => |value| try value.requireCaptured(group, incarnation, source_group, pin),
            .unlogged_loss_fenced => |value| try value.requireCaptured(group, incarnation, source_group, pin),
            .temporary_scope => |value| try value.requireCaptured(group, incarnation, source_group, pin),
        }
    }
};

/// Indexed immutable job addresses. The accepting reader must rejoin the
/// referenced canonical job/checkpoint; an index entry alone grants nothing.
pub const Pointer = union(enum) {
    reset: struct { plan_id: [16]u8, plan_digest: [32]u8, source_table_id: u64 },
    scope: struct { lookup: @import("scope_retirement_ledger.zig").Lookup, child_ordinal: u16 },
};

pub fn indexKey(buffer: []u8, group: u64, pin: persistence.PhysicalPin) ![]const u8 {
    if (group == 0) return error.InvalidSqlRelationPersistence;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:sql-native-retired:{s}", .{ group, std.fmt.bytesToHex(try pin.encode(), .lower) });
}
pub fn resetFenceKey(buffer: []u8, group: u64, plan: [16]u8, table: u64) ![]const u8 {
    if (group == 0 or table == 0 or std.mem.allEqual(u8, &plan, 0)) return error.InvalidSqlRelationPersistence;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:restore_staging:{d}:native_loss:{s}:{x:0>16}", .{ group, std.fmt.bytesToHex(plan, .lower), table });
}

const records = @import("schema_validation_publication.zig");
pub const PointerRecord = records.Record(Pointer, "NLRP", 4096);
pub const LossFenceRecord = records.Record(LossFence, "NLSF", 8192);
