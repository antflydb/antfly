// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A read-cut projection of an existing hidden empty-target reservation. This
//! grants neither initialization nor replacement: those have distinct signed
//! Boot transitions. It only permits inspection of the already initialized
//! current image while its actual host is recovering.
const std = @import("std");
const local = @import("antfly_local_sources");
const p = local.common_topology_records.RelationPersistence;
const restore = local.storage_db_restore_staging_contract;
const Origin = local.storage_db_native_empty_origin.Origin;
const Namespace = local.storage_db_doc_identity_namespace.Namespace;
const CreationScope = @import("sql_relation_attestation.zig").CreationScope;
pub const Attempt = struct {
    number: u64,
    digest: [32]u8,
    pub fn validate(self: Attempt) !void {
        if (self.number == 0 or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidSqlRelationPersistence;
    }
};
pub const Admission = struct {
    metadata_group_id: u64,
    creation_scope: CreationScope,
    scope_digest: [32]u8,
    namespace: Namespace,
    descriptor_fingerprint: [32]u8,
    origin: Origin,
    attempt: Attempt,

    pub fn validate(self: Admission) !void {
        try self.creation_scope.validate();
        try self.origin.validate();
        try self.attempt.validate();
        if (self.metadata_group_id == 0 or self.namespace.table_id == 0 or self.namespace.shard_id == 0 or self.namespace.range_id == 0 or
            std.mem.allEqual(u8, &self.scope_digest, 0) or std.mem.allEqual(u8, &self.descriptor_fingerprint, 0)) return error.InvalidSqlRelationPersistence;
    }
    /// The producer must additionally rejoin the actual current registered
    /// Boot and initialized proof. No process epoch is inferred from a table's
    /// immutable generation birth, and schema snapshots never mint loss proof.
    pub fn require(self: Admission, scope: restore.Scope, descriptor: p.Descriptor, namespace: Namespace, attempt: anytype) !void {
        try self.validate();
        try scope.validateReservation();
        try descriptor.validate();
        const expected_origin = scope.native_empty_origin orelse return error.InvalidSqlRelationPersistence;
        const supplied_attempt = attempt orelse return error.SqlRelationRecoveryIncomplete;
        if (descriptor.kind != .unlogged or !scope.empty_generation or scope.population != null or scope.rewrite != null or
            !std.meta.eql(self.creation_scope, CreationScope{ .plan_id = scope.plan_id, .plan_digest = scope.plan_digest }) or
            !std.meta.eql(self.namespace, namespace) or !std.meta.eql(self.namespace, scope.target_namespace) or
            !std.mem.eql(u8, &self.scope_digest, &scope.digest()) or
            !std.mem.eql(u8, &self.descriptor_fingerprint, &try descriptor.fingerprint()) or
            !std.mem.eql(u8, &try self.origin.digest(), &try expected_origin.digest()) or
            self.attempt.number != supplied_attempt.number or !std.mem.eql(u8, &self.attempt.digest, &supplied_attempt.digest)) return error.SqlRelationOwnerChanged;
    }
};

test "SQL native empty target admission binds current image attempt and distinguishes snapshot authority from loss" {
    const descriptor: p.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 3 };
    const namespace: Namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 6 };
    const origin: Origin = .{ .schema_only_snapshot = .{ .manifest_digest = @splat(7), .schema_digest = @splat(8), .source_table_id = 2 } };
    const scope: restore.Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(0), .source_namespace = .{ .table_id = 0, .shard_id = 0, .range_id = 0 }, .target_namespace = namespace, .target_schema_digest = @splat(8), .empty_generation = true, .native_empty_origin = origin };
    const attempt: ?Attempt = .{ .number = 2, .digest = @splat(9) };
    const admission: Admission = .{ .metadata_group_id = 1, .creation_scope = .{ .plan_id = scope.plan_id, .plan_digest = scope.plan_digest }, .scope_digest = scope.digest(), .namespace = namespace, .descriptor_fingerprint = try descriptor.fingerprint(), .origin = origin, .attempt = attempt.? };
    try admission.require(scope, descriptor, namespace, attempt);
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, admission.require(scope, descriptor, namespace, @as(?Attempt, null)));
    var changed = attempt.?;
    changed.number += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, admission.require(scope, descriptor, namespace, @as(?Attempt, changed)));
    var other_scope = scope;
    other_scope.native_empty_origin = .{ .lost_source = .{ .checkpoint_digest = @splat(7), .source = .{ .physical_root_incarnation = 1, .table_id = 2, .shard_id = 3, .range_id = 4, .data_generation = 5 } } };
    other_scope.source_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 4 };
    try std.testing.expectError(error.SqlRelationOwnerChanged, admission.require(other_scope, descriptor, namespace, attempt));
    var changed_descriptor = descriptor;
    changed_descriptor.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, admission.require(scope, changed_descriptor, namespace, attempt));
}
