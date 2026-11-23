// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Partition birth is a companion of the existing hidden initial-table journal.
//! The parent schema stays published at its predecessor until that journal and
//! the ordinary native schema-owner journal publish in the same metadata writer.
//! These values describe intended effects; only the retained journals and their
//! native owner receipts authorize installation or publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const records = local.common_topology_records;
const domain = local.system_catalog_domain;
const schema = local.schema_table_partitions;
const A = std.mem.Allocator;
pub const Id = [16]u8;
pub const Digest = [32]u8;

/// Private SQL intent. Native preparation resolves this actual parent owner
/// inside its allocation cut; the caller supplies neither child identity nor
/// successor membership bytes.
pub const Request = struct {
    parent_guard: domain.RelationMutationGuard,
    bounds: local.system_catalog_table_partitions.Range,
    pub fn validate(self: Request) !void {
        try self.parent_guard.validate();
        try self.bounds.validate();
        if (self.parent_guard.hasDefinitionIntent() or self.parent_guard.owner.kind != .table or
            !std.mem.eql(u8, self.parent_guard.target.name, self.parent_guard.logical_table)) return error.InvalidGenerationPublication;
    }
};

/// Immutable initial admission body. The child is initialized with its final
/// inverse membership, so no nonexistent old child owner or schema is claimed.
pub const Intent = struct {
    transition: schema.Transition,
    parent_guard: domain.RelationMutationGuard,
    parent_before: records.TableRecord,
    parent_after: records.TableRecord,
    parent_ranges: []const records.RangeRecord,

    pub fn validate(self: Intent, a: A, child: records.TableRecord) !void {
        const t = self.transition;
        if (t.before.policy.enabled or t.after.policy.enabled) return error.RowPolicyUnsupported;
        try t.validate(a);
        try self.parent_guard.validate();
        if (t.operation != .birth or self.parent_guard.hasDefinitionIntent() or
            self.parent_guard.owner.kind != .table or
            self.parent_guard.owner.table_id != t.before.parent.table_id or
            self.parent_guard.owner.schema_version != t.before.parent.schema_version or
            self.parent_before.table_id != t.before.parent.table_id or
            self.parent_after.table_id != self.parent_before.table_id or
            child.table_id != t.child_after.table_id or self.parent_ranges.len == 0 or
            self.parent_ranges.len > @import("fk_generation_publication.zig").max_owners or
            self.parent_before.restore_backup_id.len != 0 or self.parent_after.restore_backup_id.len != 0 or
            self.parent_before.read_schema_json.len != 0 or self.parent_after.read_schema_json.len != 0 or
            self.parent_before.storage_migration != null or self.parent_after.storage_migration != null)
            return error.InvalidGenerationPublication;
        try records.RelationPersistence.requireForeignKeyClasses(child.sql_persistence, self.parent_before.sql_persistence);
        try @import("table_manager.zig").validateCompleteKeyspaceRanges(self.parent_ranges);
        for (self.parent_ranges) |range| if (range.table_id != self.parent_before.table_id) return error.InvalidGenerationPublication;
        const owner_hash = local.storage_db_relational_schema_owner_contract.hash(self.parent_before.schema_json);
        if (!std.mem.eql(u8, &owner_hash, &self.parent_guard.owner.schema_digest)) return error.InvalidGenerationPublication;
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        const scratch = region.allocator();
        const parent_expected = try t.schemaAlloc(scratch, self.parent_before.table_id, self.parent_before.schema_json);
        if (!std.mem.eql(u8, parent_expected, self.parent_after.schema_json)) return error.InvalidGenerationPublication;
        var prepared = try local.schema_mod.PreparedSchemaDefinition.init(scratch, parent_expected);
        defer prepared.deinit(scratch);
        if (!std.meta.eql(self.parent_after.schema_reader_requirements, @as(?local.common_schema_readers.ReaderRequirements, prepared.readerRequirements()))) return error.InvalidGenerationPublication;
        var envelope = self.parent_after;
        envelope.schema_json = self.parent_before.schema_json;
        envelope.schema_reader_requirements = self.parent_before.schema_reader_requirements;
        if (!@import("table_manager.zig").tableDefinitionsEqual(envelope, self.parent_before)) return error.InvalidGenerationPublication;
        // The inverse and its complete declaration identity come from the
        // actual reserved child; arbitrary user JSON cannot confer membership.
        const root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, child.schema_json, .{});
        if (root != .object) return error.InvalidGenerationPublication;
        const raw = root.object.get(schema.field) orelse return error.InvalidGenerationPublication;
        var inverse = try schema.parseValue(scratch, raw);
        defer inverse.deinit();
        if (inverse.value != .member) return error.InvalidGenerationPublication;
        try inverse.value.member.require(t.after);
        if (!std.meta.eql(inverse.value.member.child, t.child_after) or
            !std.mem.eql(u8, &try schema.definitionDigest(scratch, child.schema_json), &t.child_after.definition))
            return error.InvalidGenerationPublication;
    }
};

/// Back-reference carried by the parent-only schema-owner journal. It names
/// the immutable initial admission digest, not a mutable execution revision.
/// Metadata rejoins the actual provisioned initial image receipts before
/// admitting this owner journal and again before the joint publication.
pub const Reference = struct {
    initial_id: Id,
    child_table_id: u64,
    admission_digest: Digest,
    pub fn validate(self: Reference) !void {
        if (std.mem.allEqual(u8, &self.initial_id, 0) or self.child_table_id == 0 or
            std.mem.allEqual(u8, &self.admission_digest, 0)) return error.InvalidGenerationPublication;
    }
};

test "schema validation publication hidden partition birth waits for the admitted parent journal before child release" {
    const allocator = std.testing.allocator;
    var region: std.heap.ArenaAllocator = .init(allocator);
    defer region.deinit();
    const a = region.allocator();
    const core = local.system_catalog_table_partitions;
    const publication = @import("fk_generation_publication.zig");
    const source =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"created_at":{"type":"datetime"}},"additionalProperties":false}}}}
    ;
    const child_name = "table:3";
    const hash = std.hash.Wyhash.hash(0x54424c45, child_name);
    const child_id = if (hash == 0) @as(u64, 1) else hash;
    const child: core.Relation = .{ .table_id = child_id, .schema_version = 1, .definition = try schema.definitionDigest(a, source) };
    const before: core.Descriptor = .{ .parent = .{ .table_id = 7, .schema_version = 1, .definition = child.definition }, .revision = 1, .key_attribute = 1 };
    var after = try before.attach(a, .{ .relation = child, .bounds = .{ .lower = .minvalue, .upper = .maxvalue } });
    after.parent.schema_version = 2;
    const transition: schema.Transition = .{ .operation = .birth, .before = before, .after = after, .child_before = child, .child_after = child, .bounds = .{ .lower = .minvalue, .upper = .maxvalue } };
    var root = try std.json.parseFromSliceLeaky(std.json.Value, a, source, .{});
    try schema.canonicalize(a, &root, .{ .parent = before });
    const parent_schema = try std.json.Stringify.valueAlloc(a, root, .{});
    const parent_after = try transition.schemaAlloc(a, 7, parent_schema);
    const child_schema = try transition.schemaAlloc(a, child_id, source);
    const guard: domain.RelationMutationGuard = .{ .target = .{ .name = "parent" }, .logical_table = "parent", .incarnation = @splat('1'), .owner = .{ .kind = .table, .table_id = 7, .schema_version = 1, .schema_digest = local.storage_db_relational_schema_owner_contract.hash(parent_schema) } };
    const intent: Intent = .{ .transition = transition, .parent_guard = guard, .parent_before = .{ .table_id = 7, .name = "parent", .schema_json = parent_schema }, .parent_after = .{ .table_id = 7, .name = "parent", .schema_json = parent_after, .schema_reader_requirements = try local.schema_mod.readerRequirementsForDefinitions(a, parent_after, "") }, .parent_ranges = &.{.{ .table_id = 7, .group_id = 201, .range_id = 201, .doc_identity_shard_id = 201, .doc_identity_range_id = 201, .start_key = "" }} };
    const plan: publication.InitialCreatePlan = .{ .id = @splat(9), .catalog_id = 3, .expected_catalog_revision = 0, .namespace_id = 2, .logical_name = "child", .retirement_scope = .hosted_store, .child = .{ .table_id = child_id, .name = child_name, .schema_json = child_schema, .schema_reader_requirements = try local.schema_mod.readerRequirementsForDefinitions(a, child_schema, "") }, .child_ranges = &.{.{ .table_id = child_id, .group_id = 200, .range_id = 200, .doc_identity_shard_id = 200, .doc_identity_range_id = 200, .start_key = "" }}, .parents = &.{}, .partition_birth = intent };
    try plan.validate(a);
    const digest = try plan.digest(a);
    var initial: publication.InitialPublication = .{
        .plan = plan,
        .plan_digest = digest,
        .candidate = try plan.candidateIdentity(a),
        .revision = 2,
        .phase = .provisioning_child,
    };
    const barriers = @import("schema_validation/publication.zig");
    var head: barriers.Head = .{
        .metadata_group_id = 1,
        .metadata_incarnation = @splat('1'),
        .id = @splat(11),
        .plan_digest = @splat(12),
        .table_id = 7,
        .owner_count = 1,
        .partition_parent = 7,
        .participants_digest = @splat(13),
        .initial_partition = try initial.partitionReference(),
    };
    try std.testing.expectError(error.GenerationPublicationChanged, initial.linkPartitionJournal(a, head));
    initial.child_provisioned = &.{.{ .group_id = 200, .digest = @splat(14) }};
    var wrong_admission = head;
    wrong_admission.initial_partition.?.admission_digest = @splat(15);
    try std.testing.expectError(error.GenerationPublicationChanged, initial.linkPartitionJournal(a, wrong_admission));
    const linked = try initial.linkPartitionJournal(a, head);
    try std.testing.expectError(error.GenerationPublicationChanged, linked.markPartitionReady(a, head));
    try std.testing.expectError(error.GenerationPublicationChanged, linked.markPartitionCanceled(a, head));
    var canceled_head = head;
    canceled_head.phase = .canceled;
    canceled_head.revision = 3;
    canceled_head.cancellation_revision = 2;
    canceled_head.failure = .requested;
    canceled_head.canceled = 1;
    canceled_head.terminal_chain = @splat(18);
    const canceled = try linked.markPartitionCanceled(a, canceled_head);
    try std.testing.expectEqual(publication.InitialPhase.canceling, canceled.phase);

    head.phase = .installed;
    head.revision = 4;
    head.install_revision = 3;
    head.ready = 1;
    head.installed = 1;
    head.ready_chain = @splat(16);
    head.installed_chain = @splat(17);
    const ready = try linked.markPartitionReady(a, head);
    try std.testing.expectEqual(publication.InitialPhase.published_hidden, ready.phase);
    try std.testing.expectEqual(@as(u64, 4), ready.revision);
}
